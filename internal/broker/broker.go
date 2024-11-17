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

type TopicMetadata struct {
	Partitions map[int]string `json:"partitions"` // (partition ID -> leader RPC address)
}

type Broker struct {
	proto.UnimplementedGafkaServiceServer
	topicsMu        sync.RWMutex
	localPartitions map[string][]*partition.Partition // Local partitions (leader/follower) indexed by topic and partition ID
	grpcServer      *grpc.Server
	membership      *discovery.Membership
	nodeName        string
	nodeIP          string
	nodePort        uint16
	topicMetadataMu sync.RWMutex
	topicMetadata   map[string]TopicMetadata
}

type partitionCreation struct {
	partitions map[string][]*partition.Partition // topic to list of partitions
	success    bool
	topicData  []discovery.MetadataUpdate
	broker     *Broker
}

func NewBroker(nodeName string, nodeIP string, nodePort uint16, memberConf discovery.Config) (*Broker, error) {
	b := &Broker{
		localPartitions: make(map[string][]*partition.Partition),
		nodeName:        nodeName,
		nodeIP:          nodeIP,
		nodePort:        nodePort,
		topicMetadata:   make(map[string]TopicMetadata),
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
	var startupError error
	ready := make(chan struct{})
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	b.grpcServer = grpc.NewServer()
	proto.RegisterGafkaServiceServer(b.grpcServer, b)

	zap.S().Infof("Starting gRPC server on %s", address)
	go func() {
		close(ready)
		if err := b.grpcServer.Serve(lis); err != nil {
			if err != grpc.ErrServerStopped {
				startupError = err
				zap.S().Errorf("Failed to serve grpc:  %v", err)
			}
		}
	}()
	<-ready
	return startupError
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

				for topicName, partitions := range b.localPartitions {
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

func (b *Broker) OnLeadershipChange(change partition.LeadershipChange) {
	b.topicMetadataMu.Lock()
	defer b.topicMetadataMu.Unlock()

	zap.S().Infof("Leadership change for topic %s partition %d: isLeader=%v, leaderAddr=%s",
		change.TopicName, change.PartitionID, change.IsLeader, change.LeaderAddr)

	data, exists := b.topicMetadata[change.TopicName]
	if !exists {
		data = TopicMetadata{
			Partitions: make(map[int]string),
		}
	}

	data.Partitions[change.PartitionID] = change.LeaderAddr
	b.topicMetadata[change.TopicName] = data

	// Broadcast the change via Serf
	topicEvent := discovery.MetadataUpdate{
		TopicName:   change.TopicName,
		PartitionId: change.PartitionID,
		LeaderAddr:  change.LeaderAddr,
	}

	eventPayload, err := json.Marshal(topicEvent)
	if err != nil {
		zap.S().Errorf("Failed to marshal topic update: %v", err)
		return
	}

	if err := b.membership.Serf.UserEvent("topic_updated", eventPayload, true); err != nil {
		zap.S().Errorf("Failed to broadcast topic update: %v", err)
		return
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

	p, err := partition.NewPartition(partitionID, topicName, config, b)
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
	if b.localPartitions[topicName] == nil {
		b.localPartitions[topicName] = make([]*partition.Partition, nbPartitions)
	}
	b.localPartitions[topicName][partitionID] = p
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
			return fmt.Errorf("failed to create remote partition, retrying %s", resp.Error)
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

	if b.topicExists(req.TopicName) { // TODO shall send a query
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
		topicData:  make([]discovery.MetadataUpdate, req.NumPartitions),
		broker:     b,
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
				pc.partitions[req.TopicName][i] = b.localPartitions[req.TopicName][i]
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
			pc.partitions[req.TopicName][i] = b.localPartitions[req.TopicName][i]
		}

		pc.topicData[i] = discovery.MetadataUpdate{TopicName: req.TopicName, PartitionId: i, LeaderAddr: leaderAddr}
	}

	// broadcast topic creation to others brokers with serf
	eventPayload, err := json.Marshal(pc.topicData)
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

// Routing is done at the producer level
func (b *Broker) Write(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {

	b.topicsMu.RLock()
	defer b.topicsMu.RUnlock()

	partitions, exists := b.localPartitions[req.Topic]
	if !exists {
		return &proto.WriteResponse{Success: false}, fmt.Errorf("topic does not exist")
	}

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
	partitions, exists := b.localPartitions[req.Topic]
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

	topicsSet := make(map[string]struct{})

	// Get local topics
	b.topicsMu.RLock()
	for topic := range b.localPartitions {
		topicsSet[topic] = struct{}{}
	}
	b.topicsMu.RUnlock()

	//query, err := b.membership.Serf.Query("list_topics", nil, &serf.QueryParam{
	//	Timeout: 5 * time.Second,
	//})
	query, err := b.membership.Serf.Query("list_topics", nil, nil)
	if err != nil {
		zap.S().Warnf("Failed to query other brokers for topics: %v", err)
	} else {
		for response := range query.ResponseCh() {
			var topics []string
			if err := json.Unmarshal(response.Payload, &topics); err != nil {
				zap.S().Warnf("Failed to unmarshal topics from broker: %v", err)
				continue
			}
			for _, topic := range topics {
				topicsSet[topic] = struct{}{}
			}
		}
	}

	topics := make([]string, 0, len(topicsSet))
	for topic := range topicsSet {
		topics = append(topics, topic)
	}

	// check metadata metadata
	b.topicMetadataMu.RLock()
	for topic := range b.topicMetadata {
		if _, exists := topicsSet[topic]; !exists {
			topics = append(topics, topic)
		}
	}
	b.topicMetadataMu.RUnlock()

	return &proto.ListTopicsResponse{Topics: topics}, nil
}

func (b *Broker) Join(name, addr string) error {
	return nil
}

func (b *Broker) Leave(name string) error {
	return nil
}

// return local metadata for topic
func (b *Broker) GetTopicData(topicName string, partitionId int) (discovery.MetadataUpdate, bool) {
	b.topicMetadataMu.RLock()
	defer b.topicMetadataMu.RUnlock()

	data, exits := b.topicMetadata[topicName]
	if !exits {
		return discovery.MetadataUpdate{}, false
	}

	return discovery.MetadataUpdate{TopicName: topicName, PartitionId: partitionId, LeaderAddr: data.Partitions[partitionId]}, true
}

// called when receiving metadata update for a topic
func (b *Broker) UpdateTopicData(data []discovery.MetadataUpdate) {
	b.topicMetadataMu.Lock()
	defer b.topicMetadataMu.Unlock()

	for _, update := range data {
		_, exists := b.topicMetadata[update.TopicName]
		if !exists {
			b.topicMetadata[update.TopicName] = TopicMetadata{Partitions: make(map[int]string)}
		}
		b.topicMetadata[update.TopicName].Partitions[update.PartitionId] = update.LeaderAddr
		zap.S().Infow("Updated topic metadata",
			"topic", update.TopicName,
			"partitionId", update.PartitionId,
			"metadata", update.LeaderAddr)
	}
}

func (b *Broker) GetAllTopics() []string {
	b.topicMetadataMu.RLock()
	defer b.topicMetadataMu.RUnlock()
	topics := make([]string, 0, len(b.localPartitions))
	for topic := range b.localPartitions {
		topics = append(topics, topic)
	}
	return topics
}

func (pc *partitionCreation) cleanup() {
	if pc.success {
		return
	}

	for topicName, partitions := range pc.partitions {
		for _, partition := range partitions {
			if partition != nil {
				_ = partition.Remove()
			}
		}
		delete(pc.broker.localPartitions, topicName)
	}
}

func (b *Broker) topicExists(topicName string) bool {
	// Check local
	b.topicsMu.RLock()
	_, exists := b.localPartitions[topicName]
	b.topicsMu.RUnlock()
	if exists {
		return true
	}

	// Check metadata
	b.topicMetadataMu.RLock()
	_, exists = b.topicMetadata[topicName]
	b.topicMetadataMu.RUnlock()
	if exists {
		return true
	}

	// Query other brokers
	query, err := b.membership.Serf.Query("list_topics", nil, &serf.QueryParam{
		Timeout: 2 * time.Second,
	})
	if err != nil {
		zap.S().Warnf("Failed to query brokers for topic existence: %v", err)
		return false
	}

	for response := range query.ResponseCh() {
		var topics []string
		if err := json.Unmarshal(response.Payload, &topics); err != nil {
			continue
		}
		for _, topic := range topics {
			if topic == topicName {
				return true
			}
		}
	}

	return false
}
