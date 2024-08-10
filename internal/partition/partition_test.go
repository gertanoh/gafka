package partition

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestNewPartition(t *testing.T) {
	dir, err := os.MkdirTemp("", "partition-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := setupTestPartition(t, 0)
	defer teardownTestPartition(p)

	require.NoError(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, 1, p.id)
	assert.Equal(t, "test-topic", p.topicName)

}

func TestPartitionWrite(t *testing.T) {
	p, err := setupTestPartition(t, 0)
	defer teardownTestPartition(p)

	require.NoError(t, err)
	assert.NotNil(t, p)

	err = p.Write([]byte("test message"))
	assert.NoError(t, err)
}

// func TestPartitionReadNonLinear(t *testing.T) {
// 	p := setupTestPartition(t)
// 	defer teardownTestPartition(p)

// 	message := []byte("test message")
// 	err := p.Write(message)
// 	require.NoError(t, err)

// 	// Wait for the write to be applied
// 	time.Sleep(100 * time.Millisecond)

// 	read, err := p.ReadNonLinear(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, read)
// }

// func TestPartitionReadLinearFromLeader(t *testing.T) {
// 	p := setupTestPartition(t)
// 	defer teardownTestPartition(p)

// 	message := []byte("test message")
// 	err := p.Write(message)
// 	require.NoError(t, err)

// 	// Wait for the write to be applied
// 	time.Sleep(100 * time.Millisecond)

// 	read, err := p.ReadLinearFromLeader(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, read)
// }

// func TestPartitionLeaderElection(t *testing.T) {
// 	p := setupTestPartition(t)
// 	defer teardownTestPartition(p)

// 	// Wait for leader election
// 	time.Sleep(1 * time.Second)

// 	assert.Equal(t, raft.Leader, p.raftNode.State())
// }

// func TestPartitionFollowerReplication(t *testing.T) {
// 	// This test requires setting up multiple nodes in a Raft cluster
// 	// Implement mock followers or use a test Raft implementation
// 	t.Skip("Implement follower replication test")
// }

// func TestPartitionLeaderChange(t *testing.T) {
// 	// This test requires setting up multiple nodes and forcing a leader change
// 	t.Skip("Implement leader change test")
// }

// func TestPartitionReadConsistencyLevels(t *testing.T) {
// 	p := setupTestPartition(t)
// 	defer teardownTestPartition(p)

// 	message := []byte("test message")
// 	err := p.Write(message)
// 	require.NoError(t, err)

// 	// Test different read methods
// 	nonLinearRead, err := p.ReadNonLinear(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, nonLinearRead)

// 	linearRead, err := p.ReadLinearFromLeader(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, linearRead)

// 	readIndexRead, err := p.Read(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, readIndexRead)
// }

// func TestPartitionRecoveryAfterCrash(t *testing.T) {
// 	p := setupTestPartition(t)

// 	message := []byte("test message")
// 	err := p.Write(message)
// 	require.NoError(t, err)

// 	// Simulate a crash by closing the partition
// 	p.raftNode.Shutdown()

// 	// Recreate the partition
// 	newP, err := NewPartition(p.id, p.topicName, log.Config{})
// 	require.NoError(t, err)
// 	defer teardownTestPartition(newP)

// 	// Wait for the new partition to elect a leader
// 	time.Sleep(1 * time.Second)

// 	// Verify that the data is still there
// 	read, err := newP.ReadNonLinear(0)
// 	assert.NoError(t, err)
// 	assert.Equal(t, message, read)
// }

func setupTestPartition(t *testing.T, idx int) (*Partition, error) {
	_, err := os.MkdirTemp("", "partition-test")
	require.NoError(t, err)

	config := Config{}
	config.LocalID = raft.ServerID(fmt.Sprintf("%d", idx))
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	if idx == 0 {
		config.Boostrap = true
	}
	ports := dynaport.Get(1)
	config.BindAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])

	p, err := NewPartition(1, "test-topic", config)
	if config.Boostrap {
		_, err = p.WaitForLeader(2 * time.Second)
		require.NoError(t, err)
	}
	require.NoError(t, err)

	return p, err
}

func teardownTestPartition(p *Partition) {
	p.Close()
	os.RemoveAll(p.topicName)
}
