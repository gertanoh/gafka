package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/gertanoh/gafka/internal/discovery"
	"github.com/gertanoh/gafka/internal/helpers"
	"github.com/gertanoh/gafka/internal/partition"
	"github.com/gertanoh/gafka/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/go-dynaport"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	LeaderElectionWait = 5 * time.Second
	MaxRetries         = 3
	RetryDelay         = time.Second
)

type Broker struct {
	proto.UnimplementedGafkaServiceServer
	topicsMu    sync.RWMutex
	topics      map[string][]*partition.Partition // topics to list of partitions handled by current broker
	grpcServer  *grpc.Server
	membership  *discovery.Membership
	nodeName    string
	nodeIP      string
	nodePort    uint16
	topicDataMu sync.RWMutex
	topicData   map[string]discovery.TopicData // topic name to metadata
}

type partitionCreation struct {
	partitions map[string][]*partition.Partition // topic to list of partitions
	success    bool
	topicData  discovery.TopicData
	broker     *Broker
}

func NewBroker(nodeName string, nodeIP string, nodePort uint16, memberConf discovery.Config) (*Broker, error) {
	b := &Broker{
		topics:    make(map[string][]*partition.Partition),
		nodeName:  nodeName,
		nodeIP:    nodeIP,
		nodePort:  nodePort,
		topicData: make(map[string]discovery.TopicData),
	}

	membership, err := discovery.New(b, memberConf)
	if err != nil {
		zap.S().Error("failed to create membership for broker", zap.Error(err))
		return nil, err
	}

	b.membership = membership
	return b, nil
}

func (b *Broker) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	b.grpcServer = grpc.NewServer()
	proto.RegisterGafkaServiceServer(b.grpcServer, b)

	zap.S().Infof("Starting gRPC server on %s", address)
	go func() {
		if err := b.grpcServer.Serve(lis); err != nil {
			zap.S().Errorf("Failed to serve grpc:  %v", err)
		}
	}()
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	zap.S().Info("Starting broker shutdown")
	defer zap.S().Info("Broker shutdown complete")

	shutdowns := []struct {
		name string
		fn   func(context.Context) error
	}{
		{
			name: "membership",
			fn: func(ctx context.Context) error {
				done := make(chan error, 1)
				go func() {
					done <- b.membership.Leave()
				}()

				select {
				case err := <-done:
					return err
				case <-ctx.Done():
					return fmt.Errorf("membership leave: %w", ctx.Err())
				}
			},
		},
		{
			name: "grpc",
			fn: func(ctx context.Context) error {
				if b.grpcServer == nil {
					return nil
				}

				done := make(chan struct{})
				go func() {
					b.grpcServer.GracefulStop()
					close(done)
				}()

				select {
				case <-done:
					return nil
				case <-ctx.Done():
					zap.S().Warn("Context cancelled, forcing gRPC server stop")
					b.grpcServer.Stop()
					return fmt.Errorf("grpc graceful shutdown: %w", ctx.Err())
				}
			},
		},
		{
			name: "partitions",
			fn: func(ctx context.Context) error {
				b.topicsMu.RLock()
				defer b.topicsMu.RUnlock()

				for topicName, partitions := range b.topics {
					select {
					case <-ctx.Done():
						return fmt.Errorf("partition shutdown interrupted: %w", ctx.Err())
					default:
					}

					for i, partition := range partitions {
						if partition == nil {
							continue
						}

						zap.S().Infof("Closing partition %d of topic %s", i, topicName)
						if err := partition.Close(); err != nil {
							return fmt.Errorf("closing partition %d of topic %s: %w", i, topicName, err)
						}
					}
				}
				return nil
			},
		},
	}

	for _, s := range shutdowns {
		zap.S().Infof("Stopping %s", s.name)

		if err := s.fn(ctx); err != nil {
			zap.S().Errorw("Failed to stop component",
				"component", s.name,
				"error", err)
			return fmt.Errorf("stopping %s: %w", s.name, err)
		}

		zap.S().Infof("Successfully stopped %s", s.name)
	}

	return nil
}

func (b *Broker) handlePartitionLeadershipChange(p *partition.Partition, isLeader bool) {
	b.topicDataMu.Lock()
	defer b.topicDataMu.Unlock()

	topicName := p.TopicName
	partitionID := p.Id

	// Get current topic metadata
	topicData, exists := b.topicData[topicName]
	if !exists {
		topicData = discovery.TopicData{
			Partitions: make(map[int]string),
		}
	}

	if isLeader {
		// Update partition leader address
		topicData.Partitions[partitionID] = b.nodeIP + ":" + "port" // You'll need to add GetPort() to Partition
		b.topicData[topicName] = topicData

		// Broadcast leadership change
		eventPayload, err := json.Marshal(struct {
			Name string
			Data discovery.TopicData
		}{
			Name: topicName,
			Data: topicData,
		})
		if err != nil {
			zap.S().Errorw("Failed to marshal topic update",
				"topic", topicName,
				"partition", partitionID,
				"error", err)
			return
		}

		if err := b.membership.Serf.UserEvent("topic_updated", eventPayload, true); err != nil {
			zap.S().Errorw("Failed to broadcast topic update",
				"topic", topicName,
				"partition", partitionID,
				"error", err)
			return
		}
	}
}

func (b *Broker) CreatePartition(ctx context.Context, req *proto.PartitionRequest) (*proto.PartitionResponse, error) {

	addr, err := b.createLocalPartition(req.TopicName, int(req.PartitionId), int32(req.NumPartitions), req.IsLeader, req.FollowerAddrs)
	return &proto.PartitionResponse{BindAddr: addr}, err
}

func (b *Broker) createLocalPartition(topicName string, partitionID int, nbPartitions int32,
	isLeader bool, followerAddrs map[string]string) (string, error) {
	port := dynaport.Get(1)
	bindAddr := fmt.Sprintf("%s:%d", b.nodeIP, port[0])

	config := partition.Config{
		Config: raft.Config{
			LocalID: raft.ServerID(b.nodeName),
		},
		BindAddr:  bindAddr,
		Bootstrap: isLeader,
	}

	p, err := partition.NewPartition(partitionID, topicName, config)
	if err != nil {
		return "", fmt.Errorf("failed to create local follower: %w", err)
	}

	if isLeader {
		// Wait for leader election
		_, err = p.WaitForLeader(LeaderElectionWait)
		if err != nil {
			// Cleanup on error
			p.Close()
			return "", fmt.Errorf("leader election failed: %w", err)
		}

		// Join followers
		for nodeName, addr := range followerAddrs {
			if err := helpers.Retry(MaxRetries, RetryDelay, func() error {
				return p.Join(nodeName, addr)
			}); err != nil {
				p.Close()
				return "", fmt.Errorf("failed to join follower %s: %w", nodeName, err)
			}
		}
	}
	// Initialize partition slice if it doesn't exist
	if b.topics[topicName] == nil {
		b.topics[topicName] = make([]*partition.Partition, nbPartitions)
	}
	b.topics[topicName][partitionID] = p
	return bindAddr, nil
}

func (b *Broker) createPartitionLeader(ctx context.Context, req *proto.CreateTopicRequest, partitionID int,
	leader serf.Member, followerAddrs map[string]string) (string, error) {

	leaderName := leader.Tags["node_name"]
	leaderRPC := leader.Tags["rpc_addr"]

	if leaderName == b.nodeName {
		return b.createLocalPartition(req.TopicName, partitionID, req.NumPartitions, true, followerAddrs)
	}
	return b.createRemotePartition(ctx, req.TopicName, partitionID, req.NumPartitions, leaderRPC, true, followerAddrs)
}

func (b *Broker) createRemotePartition(ctx context.Context, topicName string, partitionID int, nbPartitions int32,
	rpcAddr string, isLeader bool, followerAddrs map[string]string) (string, error) {

	var bindAddr string
	err := helpers.Retry(MaxRetries, RetryDelay, func() error {
		conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		client := proto.NewGafkaServiceClient(conn)
		resp, err := client.CreatePartition(ctx, &proto.PartitionRequest{
			TopicName:     topicName,
			PartitionId:   int32(partitionID),
			IsLeader:      isLeader,
			FollowerAddrs: followerAddrs,
			NumPartitions: int32(nbPartitions),
		})
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf(resp.Error)
		}
		bindAddr = resp.BindAddr
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to create remote partition after retries: %w", err)
	}
	return bindAddr, nil
}

// Topic creation logic
// Partition leaders and followers are created
// from startIdx in list to select the leader and followers are the next indices
// rand is added for the start variable
// current broker acts as the controller for the request
// Call is sync
// topic creation is broadcast to all brokers using serf membership
// TODO better handling of error when creating partition. Need to send clean up signal to all involved
// TODO how do we handle remote partition clean up
func (b *Broker) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {

	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()

	if _, exists := b.topics[req.TopicName]; exists { // TODO shall send a query
		return &proto.CreateTopicResponse{}, fmt.Errorf("topic already exists")
	}

	members := b.membership.Members()
	nbMem := len(members)

	if nbMem < int(req.NumPartitions) {
		return &proto.CreateTopicResponse{},
			fmt.Errorf("not enough brokers available. Need %d, have %d", req.NumPartitions, nbMem)
	}

	// Handle graceful delete of all partitions created by current broker if an error happens
	pc := &partitionCreation{
		partitions: make(map[string][]*partition.Partition),
		success:    false,
		topicData: discovery.TopicData{
			Partitions: make(map[int]string),
		},
		broker: b,
	}
	pc.partitions[req.TopicName] = make([]*partition.Partition, req.NumPartitions)
	defer pc.cleanup()

	// For each partition, setup raft and assign replicas
	for i := 0; i < int(req.NumPartitions); i++ {

		// start index is randomized
		startBroker := rand.IntN(nbMem)
		startIndex := (startBroker + i) % nbMem // startbroker is the partition leader

		// create followers first, then create the leader and add followers to the raft cluster
		followerAddrs := make(map[string]string) // nodeName => bindAddr

		// create followers first and then collect the addresses
		for r := 1; r < int(req.ReplicaFactor); r++ {
			memberIndex := (startIndex + r) % nbMem
			member := members[memberIndex]
			rpcAddr := member.Tags["rpc_addr"]
			// rpcAddr, _, _ = net.SplitHostPort(rpcAddr)
			nodeName := member.Tags["node_name"]

			var bindAddr string
			var err error

			if nodeName == b.nodeName {
				bindAddr, err = b.createLocalPartition(req.TopicName, i, req.NumPartitions, false, map[string]string{})
				if err != nil {
					return &proto.CreateTopicResponse{}, fmt.Errorf("failed to create follower for partition %d: %w", i, err)
				}
				pc.partitions[req.TopicName][i] = b.topics[req.TopicName][i]
			} else {
				bindAddr, err = b.createRemotePartition(ctx, req.TopicName, i, req.NumPartitions, rpcAddr, false, map[string]string{})
				if err != nil {
					return &proto.CreateTopicResponse{}, fmt.Errorf("failed to create follower for partition %d: %w", i, err)
				}
			}

			followerAddrs[nodeName] = bindAddr
		}
		// create leader
		leaderMember := members[startIndex]
		leaderAddr, err := b.createPartitionLeader(ctx, req, i, leaderMember, followerAddrs)
		if err != nil {
			return &proto.CreateTopicResponse{}, fmt.Errorf("failed to create leader for partition %d: %w", i, err)
		}

		if leaderMember.Tags["node_name"] == b.nodeName {
			pc.partitions[req.TopicName][i] = b.topics[req.TopicName][i]
		}

		pc.topicData.Partitions[i] = leaderAddr
	}

	// broadcast topic creation to others brokers with serf
	topicEvent := struct {
		Name string
		Data discovery.TopicData
	}{
		Name: req.TopicName,
		Data: pc.topicData,
	}

	eventPayload, err := json.Marshal(topicEvent)
	if err != nil {
		return &proto.CreateTopicResponse{},
			fmt.Errorf("failed to marshal topic event: %v", err)
	}

	if err := b.membership.Serf.UserEvent("topic_created", eventPayload, true); err != nil {
		return &proto.CreateTopicResponse{},
			fmt.Errorf("failed to broadcast topic creation: %v", err)
	}
	pc.success = true
	return &proto.CreateTopicResponse{}, nil
}

// Routing is done at the networking layer
// Broker is the leader of the partitions

func (b *Broker) Write(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {

	b.topicsMu.RLock()
	defer b.topicsMu.RUnlock()

	partitions, exists := b.topics[req.Topic]
	if !exists {
		return &proto.WriteResponse{Success: false}, fmt.Errorf("topic does not exist")
	}

	// partitions load balancing shall be done at the network layer

	partitionIndex := helpers.ComputeHash(req.Key) % uint32(len(partitions))

	err := partitions[partitionIndex].Write([]byte(req.Payload))
	if err != nil {
		zap.S().Error("broker:write Failed to write to log due to %s", err)
		return &proto.WriteResponse{Success: true, Error: err.Error()}, nil
	}

	return &proto.WriteResponse{Success: true, Error: ""}, nil
}

func (b *Broker) Read(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {

	b.topicsMu.RLock()
	defer b.topicsMu.RUnlock()
	partitions, exists := b.topics[req.Topic]
	if !exists {
		return &proto.ReadResponse{Success: false}, fmt.Errorf("topic does not exist")
	}

	if req.Partition < int32(len(partitions)) {
		return &proto.ReadResponse{Success: false}, fmt.Errorf("partition index is over count")
	}

	partitionIndex := int(req.Partition) % len(partitions)
	// TODO: Implement actual read logic from the partition
	data, err := partitions[partitionIndex].Read(req.Offset, partition.ReadConsistencyDefault)
	if err != nil {
		return &proto.ReadResponse{Success: false, Error: err.Error()}, err

	}
	return &proto.ReadResponse{Success: true, Data: data}, nil
}

func (b *Broker) WriteStream(stream proto.GafkaService_WriteStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := b.Write(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (b *Broker) ReadStream(req *proto.ReadRequest, stream proto.GafkaService_ReadStreamServer) error {
	// TODO: Implement streaming read logic
	return nil
}

func (b *Broker) ListTopics(ctx context.Context, req *proto.ListTopicsRequest) (*proto.ListTopicsResponse, error) {

	b.topicsMu.RLock()
	defer b.topicsMu.RUnlock()
	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}

	return &proto.ListTopicsResponse{Topics: topics}, nil
}

func (b *Broker) Join(name, addr string) error {
	return nil
}

func (b *Broker) Leave(name string) error {
	return nil
}

// return local metadata for topic
func (b *Broker) GetTopicData(topicName string) (discovery.TopicData, bool) {
	b.topicDataMu.RLock()
	defer b.topicDataMu.RUnlock()

	data, exits := b.topicData[topicName]
	return data, exits
}

// called when receiving metadata update for a topic
func (b *Broker) UpdateTopicData(topicName string, data discovery.TopicData) {
	b.topicDataMu.Lock()
	defer b.topicDataMu.Unlock()

	topicData, exists := b.topicData[topicName]
	if exists {
		if topicData.Partitions == nil {
			topicData.Partitions = make(map[int]string)
		}
		for partID, leaderAddr := range data.Partitions {
			topicData.Partitions[partID] = leaderAddr
		}
		b.topicData[topicName] = topicData
	} else {
		if data.Partitions == nil {
			data.Partitions = make(map[int]string)
		}
		b.topicData[topicName] = data
	}
	zap.S().Infow("Updated topic metadata",
		"topic", topicName,
		"partitions", len(data.Partitions),
		"metadata", data)
}

func (pc *partitionCreation) cleanup() {
	if pc.success {
		return
	}

	for topicName, partitions := range pc.partitions {
		for _, partition := range partitions {
			if partition != nil {
				partition.Remove()
			}
		}
		delete(pc.broker.topics, topicName)
	}
}
