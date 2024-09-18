package broker

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"hash/fnv"

	"github.com/gertanoh/gafka/internal/partition"
	"github.com/gertanoh/gafka/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Broker struct {
	proto.UnimplementedGafkaServiceServer
	mu         sync.RWMutex
	topics     map[string][]*partition.Partition
	grpcServer *grpc.Server
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string][]*partition.Partition),
	}
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

func (b *Broker) Stop() {
	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}
}

func (b *Broker) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[req.TopicName]; exists {
		return &proto.CreateTopicResponse{Success: true}, fmt.Errorf("topic already exists")
	}

	partitions := make([]*partition.Partition, req.NumPartitions)
	for i := 0; i < int(req.NumPartitions); i++ {
		// TODO: Implement partition creation logic
		partitions[i] = &partition.Partition{}
	}

	b.topics[req.TopicName] = partitions
	return &proto.CreateTopicResponse{Success: true}, nil
}

func (b *Broker) Write(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	partitions, exists := b.topics[req.Topic]
	if !exists {
		return &proto.WriteResponse{Success: false}, fmt.Errorf("topic does not exist")
	}

	// partitions load balancing shall be done at the network layer

	partitionIndex := hash(req.Key) % uint32(len(partitions))

	err := partitions[partitionIndex].Write([]byte(req.Payload))
	if err != nil {
		zap.S().Error("broker:write Failed to write to log due to %s", err)
		return &proto.WriteResponse{Success: true, Error: err.Error()}, nil
	}

	return &proto.WriteResponse{Success: true, Error: ""}, nil
}

func (b *Broker) Read(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}

	return &proto.ListTopicsResponse{Topics: topics}, nil
}

// To be moved to helpers
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
