package partition

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"go.uber.org/zap"
)

func initTestLogger() {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	config.OutputPaths = []string{"stdout"}
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
}

const DEFAULT_TOPIC_NAME = "test-topic"

func TestSinglePartition(t *testing.T) {

	initTestLogger()

	dir, err := os.MkdirTemp("", "partition-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.Nil(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, 0, p.Id)
	assert.Equal(t, DEFAULT_TOPIC_NAME, p.TopicName)
	leaderAddr, leaderId := p.raftNode.LeaderWithID()
	assert.NotEqual(t, string(leaderAddr), "")
	assert.NotEqual(t, string(leaderId), "")
}

// Test shall fail as leader is not yet elected
func TestPartitionWriteFailAsNoLeader(t *testing.T) {
	p, err := setupTestPartition(1, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)

	require.NoError(t, err)
	assert.NotNil(t, p)

	err = p.Write([]byte("test message"))
	assert.NotNil(t, err)
}

func TestPartitionWrite(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)
	assert.NotNil(t, p)

	err = p.Write([]byte("test message"))
	assert.NoError(t, err)
}

func TestSinglePartitionReadStrong(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	message := []byte("test message")
	err = p.Write(message)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		read, err := p.Read(0, ReadConsistencyStrong)
		if err != nil {
			return false
		}
		if !reflect.DeepEqual(message, read) {
			return false
		}
		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestSinglePartitionReadDefault(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	message := []byte("test message")
	err = p.Write(message)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		read, err := p.Read(0, ReadConsistencyDefault)
		if err != nil {
			return false
		}
		if !reflect.DeepEqual(message, read) {
			return false
		}
		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestSinglePartitionReadWeak(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	message := []byte("test message")
	err = p.Write(message)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		read, err := p.Read(0, ReadConsistencyWeak)
		if err != nil {
			return false
		}
		if !reflect.DeepEqual(message, read) {
			return false
		}
		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestSinglePartitionReadDefaultInvalidOffset(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	message := []byte("test message")
	err = p.Write(message)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := p.Read(10, ReadConsistencyDefault)

		return err != nil

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestSinglePartitionReadDefaultInvalidOffset_no_write(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	_, err = p.Read(0, ReadConsistencyDefault)
	require.NotNil(t, err)
}

func TestPartitionLeaderElection(t *testing.T) {
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	assert.Equal(t, raft.Leader, p.raftNode.State())
}

func TestPartitionCluster(t *testing.T) {
	partitions := make([]*Partition, 3)
	for idx := range partitions {
		var err error
		partitions[idx], err = setupTestPartition(idx, DEFAULT_TOPIC_NAME)
		defer teardownTestPartition(partitions[idx])
		require.NoError(t, err)
		if idx != 0 {
			err = partitions[0].Join(fmt.Sprintf("%d", idx), string(partitions[idx].raftNet.LocalAddr()))
			require.Nil(t, err)
		}
	}
	require.Eventually(t, func() bool {
		servers, err := partitions[0].GetServers()
		if err != nil {
			return false
		}
		require.Equal(t, 3, len(servers))
		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestPartitionReadStrong(t *testing.T) {
	partitions := make([]*Partition, 3)
	for idx := range partitions {
		var err error
		partitions[idx], err = setupTestPartition(idx, DEFAULT_TOPIC_NAME)
		defer teardownTestPartition(partitions[idx])
		require.NoError(t, err)
		if idx != 0 {
			err = partitions[0].Join(fmt.Sprintf("%d", idx), string(partitions[idx].raftNet.LocalAddr()))
			require.Nil(t, err)
		}
	}

	type TestData struct {
		Key   string
		Value string
	}

	Value := &TestData{
		Key:   "nb_clicks",
		Value: "12000",
	}

	valueBytes, _ := json.Marshal(Value)
	partitions[0].Write(valueBytes)

	// read from followers shall fail, to update if follower forward is implemented
	for i := 1; i < 3; i++ {
		_, err := partitions[i].Read(0, ReadConsistencyStrong)
		require.NotNil(t, err)
	}

	// Read from leader
	got, err := partitions[0].Read(0, ReadConsistencyStrong)
	require.Nil(t, err)
	if err != nil {
		return
	}
	require.Equal(t, valueBytes, got)
}

func TestPartitionRead(t *testing.T) {
	partitions := make([]*Partition, 3)
	for idx := range partitions {
		var err error
		partitions[idx], err = setupTestPartition(idx, DEFAULT_TOPIC_NAME)
		defer teardownTestPartition(partitions[idx])
		require.NoError(t, err)
		if idx != 0 {
			err = partitions[0].Join(fmt.Sprintf("%d", idx), string(partitions[idx].raftNet.LocalAddr()))
			require.Nil(t, err)
		}
	}

	type TestData struct {
		Key   string
		Value string
	}

	Value := &TestData{
		Key:   "nb_clicks",
		Value: "12000",
	}

	valueBytes, _ := json.Marshal(Value)
	partitions[0].Write(valueBytes)

	require.Eventually(t, func() bool {
		for i := 1; i < 3; i++ {
			got, err := partitions[i].Read(0, ReadConsistencyDefault)
			if err != nil {
				return false
			}
			if !reflect.DeepEqual(valueBytes, got) {
				return false
			}
		}

		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestPartitionLeaderChange(t *testing.T) {
	partitions := make([]*Partition, 3)
	for idx := range partitions {
		var err error
		partitions[idx], err = setupTestPartition(idx, DEFAULT_TOPIC_NAME)
		defer teardownTestPartition(partitions[idx])
		require.NoError(t, err)
		if idx != 0 {
			err = partitions[0].Join(fmt.Sprintf("%d", idx), string(partitions[idx].raftNet.LocalAddr()))
			require.Nil(t, err)
		}
	}
	servers, err := partitions[0].GetServers()
	require.Nil(t, err)
	require.Equal(t, 3, len(servers))

	err = partitions[0].Leave(strconv.Itoa(0))
	require.Nil(t, err)

	var leaderIndex int

	// Wait for the cluster to elect a new leader
	require.Eventually(t, func() bool {
		for i, p := range partitions {
			if i != 0 && p.raftNode.State() == raft.Leader {
				leaderIndex = i
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "Failed to elect a new leader after removing the old one")

	servers, err = partitions[leaderIndex].GetServers()
	require.Nil(t, err)
	require.Equal(t, 2, len(servers))

	require.Equal(t, raft.Leader, partitions[leaderIndex].raftNode.State())
}

func TestPartitionRecoveryAfterCrash(t *testing.T) {
	// Setup initial partition
	p, err := setupTestPartition(0, DEFAULT_TOPIC_NAME)
	require.NoError(t, err)
	defer os.RemoveAll(p.dataDir)

	// Write some data
	testData := []byte("test message for crash recovery")
	err = p.Write(testData)
	require.NoError(t, err)

	// Wait for the write to be applied
	time.Sleep(500 * time.Millisecond)

	config := p.config

	p.raftNode.Shutdown()
	p.Close()

	newP, err := NewPartition(0, DEFAULT_TOPIC_NAME, config)
	require.NoError(t, err)
	defer newP.Close()

	_, err = newP.WaitForLeader(1 * time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		readData, err := newP.Read(0, ReadConsistencyStrong)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(testData, readData)
	}, 1*time.Second, 100*time.Millisecond, "Failed to recover data after crash")

}
func setupTestPartition(idx int, topicName string) (*Partition, error) {

	config := Config{}
	config.LocalID = raft.ServerID(fmt.Sprintf("%d", idx))
	config.HeartbeatTimeout = 100 * time.Millisecond
	config.ElectionTimeout = 100 * time.Millisecond
	config.LeaderLeaseTimeout = 100 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	config.logConf.Segment.FlushWrite = true
	if idx == 0 {
		config.Bootstrap = true
	}
	ports := dynaport.Get(1)
	config.BindAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])

	p, err := NewPartition(idx, topicName, config)
	if err != nil {
		return nil, err
	}
	if idx == 0 {
		_, err = p.WaitForLeader(5 * time.Second)
	}
	return p, err
}

func teardownTestPartition(p *Partition) {
	p.Close()
	os.RemoveAll(p.dataDir)
}
