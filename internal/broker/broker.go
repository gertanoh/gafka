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
	"github.com/travisjeffery/go-dynaport"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	LeaderElectionWait = 5 * time.Second
)

type Broker struct {
	proto.UnimplementedGafkaServiceServer
	topicsMu    sync.RWMutex
	topics      map[string][]*partition.Partition // topics to list of partitions handled by current broker
	grpcServer  *grpc.Server
	membership  *discovery.Membership
	nodeName    string
	nodeIP      string
	topicDataMu sync.RWMutex
	topicData   map[string]discovery.TopicData // topic name to metadata
}

type partitionCreation struct {
	partitions map[string][]*partition.Partition // topic to list of partitions
	success    bool
	topicData  discovery.TopicData
}

func NewBroker(nodeName string, nodeIP string, memberConf discovery.Config) (*Broker, error) {
	b := &Broker{
		topics:    make(map[string][]*partition.Partition),
		nodeName:  nodeName,
		nodeIP:    nodeIP,
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
	return b.grpcServer.Serve(lis)
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

func (b *Broker) CreatePartition(ctx context.Context, req *proto.PartitionRequest) (*proto.PartitionResponse, error) {

	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()

	port := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", b.nodeIP, port[0])

	config := partition.Config{
		Config: raft.Config{
			LocalID: raft.ServerID(b.nodeName),
		},
		BindAddr:  addr,
		Bootstrap: req.IsLeader,
	}

	p, err := partition.NewPartition(int(req.PartitionId), req.TopicName, config)
	if err != nil {
		return &proto.PartitionResponse{
			Error: fmt.Sprintf("failed to create partition: %v", err),
		}, nil
	}

	if req.IsLeader {
		// join followers to raft cluster
		for node, nodeAddr := range req.FollowerAddrs {
			if err := p.Join(node, nodeAddr); err != nil {
				return &proto.PartitionResponse{
					Error: fmt.Sprintf("failed to join followers to raft cluster due to : %v. Data is %+v", err, req.FollowerAddrs),
				}, nil
			}
		}
	}
	// Initialize partition slice if it doesn't exist
	if b.topics[req.TopicName] == nil {
		b.topics[req.TopicName] = make([]*partition.Partition, req.PartitionId+1)
	}
	// Ensure slice has enough capacity
	if int(req.PartitionId) >= len(b.topics[req.TopicName]) {
		newPartitions := make([]*partition.Partition, req.PartitionId+1)
		copy(newPartitions, b.topics[req.TopicName])
		b.topics[req.TopicName] = newPartitions
	}
	b.topics[req.TopicName][req.PartitionId] = p

	return &proto.PartitionResponse{BindAddr: addr}, nil
}

// Topic creation logic
// Partition leaders and followers are created
// from startIdx in list to select the leader and followers are the next indices
// rand is added for the start variable
// current broker acts as the controller for the request
// Call is sync
// topic creation is broadcast to all brokers using serf membership
// TODO better handling of error when creating partition. Need to send clean up signal to all involved
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
		partitions: make(map[string][]*partition.Partition, req.NumPartitions),
		success:    false,
		topicData: discovery.TopicData{
			Partitions: make(map[int]string),
		},
	}
	defer pc.cleanup()

	b.topics[req.TopicName] = make([]*partition.Partition, req.NumPartitions)

	// For each partition, setup raft and assign replicas
	for i := 0; i < int(req.NumPartitions); i++ {

		// start index is randomized
		startBroker := rand.IntN(nbMem)
		startIndex := (startBroker + i) % nbMem

		followerAddrs := make(map[string]string) // nodeName => bindAddr

		// create followers first and then collect the addresses
		for r := 1; r < int(req.ReplicaFactor); r++ {
			memberIndex := (startIndex + r) % nbMem
			member := members[memberIndex]
			rpcAddr := member.Tags["rpc_addr"]
			nodeName := member.Tags["node_name"]

			var bindAddr string
			if nodeName == b.nodeName {
				partHost, _, err := net.SplitHostPort(rpcAddr)
				port := dynaport.Get(1)
				bindAddr = fmt.Sprintf("%s:%d", partHost, port[0])

				// local call
				config := partition.Config{
					Config: raft.Config{
						LocalID: raft.ServerID(b.nodeName),
					},
					BindAddr:  bindAddr,
					Bootstrap: false,
				}

				p, err := partition.NewPartition(i, req.TopicName, config)
				if err != nil {
					return &proto.CreateTopicResponse{},
						fmt.Errorf("failed to create partition %d: %v", i, err)
				}
				pc.partitions[req.TopicName][i] = p
			} else {
				conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return &proto.CreateTopicResponse{}, fmt.Errorf("failed to connect to broker: %v", err)
				}

				client := proto.NewGafkaServiceClient(conn)
				partReq := &proto.PartitionRequest{
					TopicName:   req.TopicName,
					PartitionId: int32(i),
					IsLeader:    false,
				}
				defer conn.Close()

				resp, err := client.CreatePartition(ctx, partReq)
				if err != nil || resp.Error != "" {
					return &proto.CreateTopicResponse{}, fmt.Errorf("failed to create partition: %v, %s", err, resp.Error)
				}
				bindAddr = resp.BindAddr
			}
			followerAddrs[nodeName] = bindAddr

		}

		// create partition leader
		leaderMember := members[startIndex]
		leaderName := leaderMember.Tags["node_name"]
		leaderRPC := leaderMember.Tags["rpc_addr"]

		if leaderName == b.nodeName {

			port := dynaport.Get(1)
			leaderAddr := fmt.Sprintf("%s:%d", b.nodeIP, port[0])

			config := partition.Config{
				Config: raft.Config{
					LocalID: raft.ServerID(b.nodeName),
				},
				BindAddr:  leaderAddr,
				Bootstrap: true,
			}

			p, err := partition.NewPartition(i, req.TopicName, config)
			if err != nil {
				return &proto.CreateTopicResponse{},
					fmt.Errorf("failed to create leader partition %d: %v", i, err)
			}

			// Wait for leader election
			_, err = p.WaitForLeader(LeaderElectionWait)
			if err != nil {
				// Cleanup on error
				p.Close()
				return &proto.CreateTopicResponse{},
					fmt.Errorf("failed to elect leader for partition %d: %v", i, err)
			}
			// Join followers
			for nodeName, addr := range followerAddrs {
				if err := p.Join(nodeName, addr); err != nil {
					return &proto.CreateTopicResponse{},
						fmt.Errorf("failed to add replica for partition %d: %v", i, err)
				}
			}

			pc.partitions[req.TopicName][i] = p
			pc.topicData.Partitions[i] = leaderAddr

		} else {
			// remote leader for partitions
			conn, err := grpc.NewClient(leaderRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return &proto.CreateTopicResponse{},
					fmt.Errorf("failed to connect to leader broker: %v", err)
			}
			defer conn.Close()

			client := proto.NewGafkaServiceClient(conn)
			partReq := &proto.PartitionRequest{
				TopicName:     req.TopicName,
				PartitionId:   int32(i),
				IsLeader:      true,
				FollowerAddrs: followerAddrs,
			}

			resp, err := client.CreatePartition(ctx, partReq)
			if err != nil || resp.Error != "" {
				return &proto.CreateTopicResponse{},
					fmt.Errorf("failed to create leader partition: %v, %s", err, resp.Error)
			}
			pc.topicData.Partitions[i] = resp.BindAddr
		}
	}

	b.topics[req.TopicName] = pc.partitions[req.TopicName]
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
	b.topicDataMu.RLock()
	defer b.topicDataMu.RUnlock()

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

	for _, partitions := range pc.partitions {
		for _, partition := range partitions {
			if partition != nil {
				partition.Remove()
			}
		}
	}
}
