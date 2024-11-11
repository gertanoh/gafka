package broker

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gertanoh/gafka/internal/discovery"
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
				"rpc_addr": addr,
			},
			StartJoinAddrs: startJoinAddr,
		}
		broker, err := NewBroker("test-node-"+strconv.Itoa(i), "127.0.0.1", uint16(port[0]), memberConf)
		broker.Start(addr)
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
				assert.NotNil(t, broker.topics)
				assert.NotNil(t, broker.topicData)
			}
		})
	}
}

// func dumpGoroutines() {
// 	buf := make([]byte, 1<<16)
// 	runtime.Stack(buf, true)
// 	zap.S().Info(string(buf))
// }

// func TestCreateTopic(t *testing.T) {
// 	tests := []struct {
// 		name        string
// 		request     *proto.CreateTopicRequest
// 		setupBroker func(*Broker)
// 		wantErr     bool
// 		errContains string
// 	}{
// 		{
// 			name: "successful creation",
// 			request: &proto.CreateTopicRequest{
// 				TopicName:     "test-topic",
// 				NumPartitions: 2,
// 				ReplicaFactor: 2,
// 			},
// 			setupBroker: func(b *Broker) {},
// 			wantErr:     false,
// 		},
// 		// {
// 		// 	name: "topic already exists",
// 		// 	request: &proto.CreateTopicRequest{
// 		// 		TopicName:     "existing-topic",
// 		// 		NumPartitions: 2,
// 		// 		ReplicaFactor: 2,
// 		// 	},
// 		// 	setupBroker: func(b *Broker) {
// 		// 		b.topicsMu.Lock()
// 		// 		b.topics["existing-topic"] = make([]*partition.Partition, 1)
// 		// 		b.topicsMu.Unlock()
// 		// 	},
// 		// 	wantErr:     true,
// 		// 	errContains: "topic already exists",
// 		// },
// 		// {
// 		// 	name: "not enough brokers",
// 		// 	request: &proto.CreateTopicRequest{
// 		// 		TopicName:     "test-topic",
// 		// 		NumPartitions: 5,
// 		// 		ReplicaFactor: 2,
// 		// 	},
// 		// 	setupBroker: func(b *Broker) {},
// 		// 	wantErr:     true,
// 		// 	errContains: "not enough brokers available",
// 		// },
// 	}

// 	// defer func() {
// 	// 	if t.Failed() {
// 	// 		zap.S().Info("Test failed, dumping goroutines:")
// 	// 		dumpGoroutines()
// 	// 	}
// 	// }()

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			brokers, err := setupTestBroker(t, 3)
// 			defer cleanupBrokers(t, brokers)
// 			require.NoError(t, err)

// 			broker := brokers[0]
// 			resp, err := broker.CreateTopic(context.Background(), tt.request)
// 			if tt.wantErr {
// 				assert.Error(t, err)
// 				assert.Contains(t, err.Error(), tt.errContains)
// 			} else {
// 				assert.NoError(t, err)
// 				assert.Empty(t, resp.Error)

// 				// Verify topic was created
// 				broker.topicsMu.RLock()
// 				partitions, exists := broker.topics[tt.request.TopicName]
// 				broker.topicsMu.RUnlock()

// 				assert.True(t, exists)
// 				assert.Len(t, partitions, int(tt.request.NumPartitions))

// 				// Verify topic metadata
// 				broker.topicDataMu.RLock()
// 				metadata, exists := broker.topicData[tt.request.TopicName]
// 				broker.topicDataMu.RUnlock()

// 				assert.True(t, exists)
// 				assert.Len(t, metadata.Partitions, int(tt.request.NumPartitions))
// 			}
// 		})
// 	}
// }

// func TestTopicDataOperations(t *testing.T) {
// 	broker, err := setupTestBroker(t, 1)
// 	require.NoError(t, err)

// 	testTopic := "test-topic"
// 	testData := discovery.TopicData{
// 		Partitions: map[int]string{
// 			0: "localhost:1111",
// 			1: "localhost:2222",
// 		},
// 	}

// 	// Test UpdateTopicData
// 	broker[0].UpdateTopicData(testTopic, testData)

// 	// Test GetTopicData
// 	data, exists := broker[0].GetTopicData(testTopic)
// 	assert.True(t, exists)
// 	assert.Equal(t, testData.Partitions, data.Partitions)

// 	// Test update existing topic
// 	newData := discovery.TopicData{
// 		Partitions: map[int]string{
// 			2: "localhost:3333",
// 		},
// 	}
// 	broker[0].UpdateTopicData(testTopic, newData)

// 	data, exists = broker[0].GetTopicData(testTopic)
// 	assert.True(t, exists)
// 	assert.Len(t, data.Partitions, 3)
// 	assert.Equal(t, "localhost:3333", data.Partitions[2])
// }

// // func TestBrokerStop(t *testing.T) {
// // 	broker, err := setupTestBroker(t, 1)
// // 	require.NoError(t, err)

// // 	// Start the broker
// // 	go func() {
// // 		err := broker[0].Start("localhost:0")
// // 		require.NoError(t, err)
// // 	}()

// // 	// Allow time for startup
// // 	time.Sleep(100 * time.Millisecond)

// // 	// Test normal shutdown
// // 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// // 	defer cancel()

// // 	err = broker[0].Stop(ctx)
// // 	assert.NoError(t, err)

// // 	// Test shutdown with cancelled context
// // 	broker, err = setupTestBroker(t, 1)
// // 	require.NoError(t, err)

// // 	ctx, cancel = context.WithCancel(context.Background())
// // 	cancel() // Cancel immediately

// // 	err = broker[0].Stop(ctx)
// // 	assert.Error(t, err)
// // 	assert.Contains(t, err.Error(), "context canceled")
// // }
