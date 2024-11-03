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
	topicsMu   sync.RWMutex
	topics     map[string]*partition.Partition
	grpcServer *grpc.Server
	membership *discovery.Membership
	nodeName   string
	nodeIP string
	topicDataMu sync.RWMutex
	topicData   map[string]discovery.TopicData // topic name to metadata
}

type parititionCreation struct {
	partitions []*partition.Partition
	currentIdx int
	success    bool
	topicData  discovery.TopicData
}

func NewBroker(nodeName string, nodeIP string,  memberConf discovery.Config) (*Broker, error) {
	b := &Broker{
		topics:    make(map[string]*partition.Partition),
		nodeName:  nodeName,
		nodeIP: nodeIP,
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
			Success: false,
			Error:   fmt.Sprintf("failed to create partition: %v", err),
		}, nil
	}

	if !req.IsLeader {
		 
	}
	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()
	b.topics[req.TopicName] = p

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

	if _, exists := b.topics[req.TopicName]; exists {
		return &proto.CreateTopicResponse{Success: true}, fmt.Errorf("topic already exists")
	}

	members := b.membership.Members()
	nbMem := len(members)

	if nbMem < int(req.NumPartitions) {
		return &proto.CreateTopicResponse{Success: false},
			fmt.Errorf("not enough brokers available. Need %d, have %d", req.NumPartitions, nbMem)
	}

	pc := &parititionCreation{
		partitions: make([]*partition.Partition, req.NumPartitions),
	}

	defer pc.cleanup()

	// For each partition, setup raft and assign replicas
	for i := 0; i < int(req.NumPartitions); i++ {

		// start index is randomized
		startBroker := rand.IntN(nbMem)
		startIndex := (startBroker + i) % nbMem
		pc.currentIdx = i

		var partitionLeaderAddr string

		// create all replicas
		for r := range int(req.ReplicaFactor) {
			memberIndex := (startIndex + r) % nbMem
			member := members[memberIndex]
			rpcAddr := member.Tags["rpc_addr"]
			nodeName := member.Tags["node_name"]
			isLeader := r == 0

			if nodeName == b.nodeName {
				partHost, _, err := net.SplitHostPort(rpcAddr)
				port := dynaport.Get(1)
				addr := fmt.Sprintf("%s:%d", partHost, port[0])
				// local call
				config := partition.Config{
                    Config: raft.Config{
                        LocalID: raft.ServerID(b.nodeName),
                    },
                    BindAddr:  addr,
                    Bootstrap: isLeader,
                }

                p, err := partition.NewPartition(i, req.TopicName, config)
                if err != nil {
                    return &proto.CreateTopicResponse{Success: false}, 
                        fmt.Errorf("failed to create partition %d: %v", i, err)
                }
				if isLeader {
					pc.partitions[i] = p
					pc.topicData.Partitions[i] = addr
					partitionLeaderAddr = addr
				}
			} else {
				conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return fmt.Errorf("failed to connect to broker: %v", err)
				}
	
				client := proto.NewGafkaServiceClient(conn)
				partReq := &proto.PartitionRequest{
					TopicName:   req.TopicName,
					PartitionId: int32(i),
					IsLeader:    isLeader,
					BindAddr:    rpcAddr,
				}
				defer conn.Close()

				resp, err := client.CreatePartition(ctx, partReq)
				if err != nil || !resp.Success {
					return fmt.Errorf("failed to create partition: %v, %s", err, resp.Error)
				}
				if isLeader {
					partitionLeaderAddr = resp.BindAddr
				}
			}

			// join raft cluster
			if !isLeader && pc.partitions[i] != nil {
				if err := pc.partitions[i].Join()
			}

			if !isLeader && pc.partitions

		}

		port := dynaport.Get(1)
		addr := fmt.Sprintf("%s:%d", partHost, port[0])

		config := partition.Config{
			Config: raft.Config{
				LocalID: raft.ServerID(b.nodeName),
			},
			BindAddr:  addr,
			Bootstrap: true,
		}

		p, err := partition.NewPartition(i, req.TopicName, config)
		if err != nil {
			return &proto.CreateTopicResponse{Success: false}, fmt.Errorf("failed to create partition %d: %v", i, err)

		}

		// add replicas
		for r := range int(req.ReplicaFactor) {
			memberIndex := (startIndex + r) % nbMem
			member := members[memberIndex]
			if err := p.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
				p.Close()
				return &proto.CreateTopicResponse{Success: false},
					fmt.Errorf("failed to add replica for partition %d: %v", i, err)
			}
		}

		// Wait for leader election
		leaderAddr, err := p.WaitForLeader(LeaderElectionWait)
		if err != nil {
			// Cleanup on error
			p.Close()
			return &proto.CreateTopicResponse{Success: false},
				fmt.Errorf("failed to elect leader for partition %d: %v", i, err)
		}

		pc.partitions[i] = p
		pc.topicData.Partitions[i] = leaderAddr
	}

	b.topics[req.TopicName] = pc.partitions

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
		return &proto.CreateTopicResponse{Success: false},
			fmt.Errorf("failed to marshal topic event: %v", err)
	}

	if err := b.membership.Serf.UserEvent("topic_created", eventPayload, true); err != nil {
		return &proto.CreateTopicResponse{Success: false},
			fmt.Errorf("failed to broadcast topic creation: %v", err)
	}
	pc.success = true
	return &proto.CreateTopicResponse{Success: true}, nil
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

func (pc *parititionCreation) cleanup() {
	if pc.success {
		return
	}

	for i := range pc.currentIdx {
		if pc.partitions[i] != nil {
			pc.partitions[i].Close()
		}
	}
}
