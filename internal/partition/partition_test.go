package partition

import (
	"fmt"
	"os"
	"reflect"
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
	assert.Equal(t, 0, p.id)
	assert.Equal(t, DEFAULT_TOPIC_NAME, p.topicName)
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

func TestPartitionFollowerReplication(t *testing.T) {
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
	time.Sleep(5 * time.Second)
	require.Eventually(t, func() bool {
		servers, err := partitions[0].GetServers()
		if err != nil {
			return false
		}
		require.Equal(t, 3, len(servers))
		fmt.Printf("servers : %+v", servers)
		return true

	}, 500*time.Millisecond, 50*time.Millisecond)
}

func TestPartitionLeaderChange(t *testing.T) {
	t.Skip("Implement leader change test")
}

func TestPartitionRecoveryAfterCrash(t *testing.T) {
	t.Skip("Implement recovery after crash")
}

func setupTestPartition(idx int, topicName string) (*Partition, error) {

	config := Config{}
	config.LocalID = raft.ServerID(fmt.Sprintf("%d", idx))
	config.HeartbeatTimeout = 100 * time.Millisecond
	config.ElectionTimeout = 100 * time.Millisecond
	config.LeaderLeaseTimeout = 100 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
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
		_, err = p.WaitForLeader(10 * time.Second)
	}
	return p, err
}

func teardownTestPartition(p *Partition) {
	p.Close()
	os.RemoveAll(p.dataDir)
}
