package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gertanoh/gafka/internal/discovery"
	"github.com/gertanoh/gafka/internal/partition"
	"github.com/gertanoh/gafka/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func setupTestBroker(t *testing.T, nbBrokers int) ([]*Broker, error) {
	brokers := make([]*Broker, nbBrokers)
	var startJoinAddr []string
	for i := range nbBrokers {
		port := dynaport.Get(2)
		addr := fmt.Sprintf("%s:%d", "127.0.0.1", port[0])
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", port[1])
		startJoinAddr = append(startJoinAddr, bindAddr)
		memberConf := discovery.Config{
			NodeName: "test-node-" + strconv.Itoa(i),
			BindAddr: bindAddr,
			Tags: map[string]string{
				"rpc_addr":  addr,
				"node_name": "test-node-" + strconv.Itoa(i),
			},
			StartJoinAddrs: startJoinAddr,
		}
		broker, err := NewBroker("test-node-"+strconv.Itoa(i), "127.0.0.1", uint16(port[0]), memberConf)
		require.NoError(t, err)

		err = broker.Start(addr)
		require.NoError(t, err)

		brokers[i] = broker
		require.NoError(t, err)

	}

	return brokers, nil
}

func cleanupBrokers(t *testing.T, brokers []*Broker) {
	for _, broker := range brokers {
		if broker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, broker.Stop(ctx))
		}
	}
	matches, _ := filepath.Glob("test-topic-*")

	for _, match := range matches {
		if err := os.RemoveAll(match); err != nil {
			_ = fmt.Errorf("failed to remove %s: %w", match, err)
		}
	}
}

func TestNewBroker(t *testing.T) {
	tests := []struct {
		name       string
		nodeName   string
		memberConf discovery.Config
		wantErr    bool
	}{
		{
			name:     "valid configuration",
			nodeName: "test-node",
			memberConf: discovery.Config{
				NodeName: "test-node",
				BindAddr: "localhost:0",
				Tags: map[string]string{
					"rpc_addr": "localhost:9092",
				},
			},
			wantErr: false,
		},
		{
			name:     "invalid bind address",
			nodeName: "test-node",
			memberConf: discovery.Config{
				NodeName: "test-node",
				BindAddr: "invalid:addr:",
			},
			wantErr: true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker, err := NewBroker(tt.nodeName, "localhost-"+strconv.Itoa(i), 45665, tt.memberConf)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, broker)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, broker)
				assert.Equal(t, tt.nodeName, broker.nodeName)
				assert.NotNil(t, broker.localPartitions)
				assert.NotNil(t, broker.topicMetadata)
			}
		})
	}
}

func TestCreateTopic(t *testing.T) {
	tests := []struct {
		name        string
		request     *proto.CreateTopicRequest
		setupBroker func(*Broker)
		wantErr     bool
		errContains string
	}{
		{
			name: "successful creation",
			request: &proto.CreateTopicRequest{
				TopicName:     "test-topic",
				NumPartitions: 1,
				ReplicaFactor: 1,
			},
			setupBroker: func(b *Broker) {},
			wantErr:     false,
		},
		{
			name: "topic already exists",
			request: &proto.CreateTopicRequest{
				TopicName:     "test-topic-existing",
				NumPartitions: 1,
				ReplicaFactor: 1,
			},
			setupBroker: func(b *Broker) {
				b.topicsMu.Lock()
				b.localPartitions["test-topic-existing"] = make([]*partition.Partition, 1)
				b.topicsMu.Unlock()
			},
			wantErr:     true,
			errContains: "topic already exists",
		},
		{
			name: "not enough brokers",
			request: &proto.CreateTopicRequest{
				TopicName:     "test-topic",
				NumPartitions: 5,
				ReplicaFactor: 2,
			},
			setupBroker: func(b *Broker) {},
			wantErr:     true,
			errContains: "not enough brokers available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			brokers, err := setupTestBroker(t, 1)
			defer cleanupBrokers(t, brokers)
			require.NoError(t, err)

			broker := brokers[0]
			tt.setupBroker(broker)
			resp, err := broker.CreateTopic(context.Background(), tt.request)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
				assert.Empty(t, resp.Error)

				// Verify topic was created
				broker.topicsMu.RLock()
				partitions, exists := broker.localPartitions[tt.request.TopicName]
				broker.topicsMu.RUnlock()

				assert.True(t, exists)
				assert.Len(t, partitions, int(tt.request.NumPartitions))

				// Verify topic metadata
				require.Eventually(t, func() bool {
					broker.topicMetadataMu.RLock()
					metadata, exists := broker.topicMetadata[tt.request.TopicName]
					broker.topicMetadataMu.RUnlock()
					if !exists {
						return false
					}
					if len(metadata.Partitions) != int(tt.request.NumPartitions) {
						return false
					}
					return true
				},
					1*time.Second, 100*time.Millisecond, "Failed to get topic metadata through membership")

			}
		})
	}
}

func TestTopicDataOperations(t *testing.T) {
	broker, err := setupTestBroker(t, 1)
	require.NoError(t, err)

	testTopic := "test-topic"
	testData := make([]discovery.MetadataUpdate, 2)
	testData[0] = discovery.MetadataUpdate{
		TopicName:   testTopic,
		PartitionId: 0,
		LeaderAddr:  "localhost:1111",
	}
	testData[1] = discovery.MetadataUpdate{
		TopicName:   testTopic,
		PartitionId: 1,
		LeaderAddr:  "localhost:2222",
	}

	// Test UpdateTopicData
	broker[0].UpdateTopicData(testData)

	// Test GetTopicData
	data, exists := broker[0].GetTopicData(testTopic, 0)
	assert.True(t, exists)
	assert.Equal(t, testData[0], data)

	// Test update existing topic
	newData := make([]discovery.MetadataUpdate, 1)
	newData[0] = discovery.MetadataUpdate{
		TopicName:   testTopic,
		PartitionId: 2,
		LeaderAddr:  "localhost:3333",
	}
	broker[0].UpdateTopicData(newData)

	data, exists = broker[0].GetTopicData(testTopic, 2)
	assert.True(t, exists)
	assert.Equal(t, "localhost:3333", data.LeaderAddr)
}

func TestBrokerStop(t *testing.T) {
	broker, err := setupTestBroker(t, 1)
	require.NoError(t, err)

	// Start the broker
	err = broker[0].Start("localhost:0")
	require.NoError(t, err)

	// Test normal shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = broker[0].Stop(ctx)
	assert.NoError(t, err)

	// Test shutdown with cancelled context
	broker, err = setupTestBroker(t, 1)
	require.NoError(t, err)

	ctx, cancel = context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = broker[0].Stop(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}
