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

func TestSinglePartition(t *testing.T) {

	initTestLogger()

	dir, err := os.MkdirTemp("", "partition-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := setupTestPartition(t, 0)
	defer teardownTestPartition(p)
	require.Nil(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, 1, p.id)
	assert.Equal(t, "test-topic", p.topicName)
	leaderAddr, leaderId := p.raftNode.LeaderWithID()
	assert.NotEqual(t, string(leaderAddr), "")
	assert.NotEqual(t, string(leaderId), "")
}

// Test shall fail as leader is not yet elected
func TestPartitionWriteFailAsNoLeader(t *testing.T) {
	p, err := setupTestPartition(t, 1)
	defer teardownTestPartition(p)

	require.NoError(t, err)
	assert.NotNil(t, p)

	err = p.Write([]byte("test message"))
	assert.NotNil(t, err)
}

func TestPartitionWrite(t *testing.T) {
	p, err := setupTestPartition(t, 0)
	defer teardownTestPartition(p)
	require.NoError(t, err)
	assert.NotNil(t, p)

	err = p.Write([]byte("test message"))
	assert.NoError(t, err)
}

func TestSinglePartitionReadStrong(t *testing.T) {
	p, err := setupTestPartition(t, 0)
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
	p, err := setupTestPartition(t, 0)
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
	p, err := setupTestPartition(t, 0)
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

func TestPartitionLeaderElection(t *testing.T) {
	p, err := setupTestPartition(t, 0)
	defer teardownTestPartition(p)
	require.NoError(t, err)

	assert.Equal(t, raft.Leader, p.raftNode.State())
}

func TestPartitionFollowerReplication(t *testing.T) {
	t.Skip("Implement follower replication test")
}

func TestPartitionLeaderChange(t *testing.T) {
	t.Skip("Implement leader change test")
}

func TestPartitionRecoveryAfterCrash(t *testing.T) {
	t.Skip("Implement recovery after crash")
}

func setupTestPartition(t *testing.T, idx int) (*Partition, error) {
	_, err := os.MkdirTemp("", "partition-test")
	_ = os.Remove("test-topic")
	require.NoError(t, err)

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

	p, err := NewPartition(1, "test-topic", config)
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
	os.RemoveAll(p.topicName)
}
