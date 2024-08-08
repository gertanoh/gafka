// Package handles the partition. Partition is replicated using raft consensus module
// handles write/read

package partition

import (
	"bytes"
	"errors"
	"kafka-like/internal/log"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

var (
	ErrNotLeader                = errors.New("not leader")
	ErrTimeoutExpired           = errors.New("timeout expired")
	ErrTimeoutWaitingForApplied = errors.New("request timeout waiting for appliedIndex to match commit")
)

const (
	AppliedDelay = 100 * time.Millisecond
)

type Partition struct {
	id        int
	topicName string
	Log       *log.Log
	raftNode  *raft.Raft
	mu        sync.RWMutex
	raft      struct {
		raft.Config
		BindAddr string
		Boostrap bool
	}
}

func NewPartition(id int, topicName string, config log.Config) (*Partition, error) {

	p := &Partition{}
	if err := os.MkdirAll(topicName, 0755); err != nil {
		return nil, err
	}

	var err error
	p.Log, err = log.NewLog(config)
	if err != nil {
		return nil, err
	}

	if err := p.setupRaft(topicName); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Partition) Write(message []byte) error {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(AppendRequestType)})
	if err != nil {
		return err
	}

	_, err = buf.Write(message)
	if err != nil {
		return err
	}

	timeout := 10 * time.Second
	future := p.raftNode.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		zap.S().Error("Fail to commit log entry", zap.Error(future.Error()))
		return future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return err
	}
	return nil
}

// Read from any node. Might serve stale read
func (p *Partition) ReadNonLinear(offset uint64) ([]byte, error) {
	return p.Log.Read(offset)
}

// Read served from Read, check are applied to prevent loss of leadership
func (p *Partition) ReadLinearFromLeader(offset uint64) ([]byte, error) {
	if p.raftNode.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	future := p.raftNode.VerifyLeader()
	if future.Error() != nil {
		return nil, ErrNotLeader
	}
	// wait for commit index to match appliedIndex
	if p.WaitForAppliedIndex(offset, 1*time.Second) != nil {
		return nil, ErrTimeoutWaitingForApplied
	}
	return p.Log.Read(offset)
}

// Read is done from follower/leader, attempt to implement readIndex optimization
// Due to the network call to get the commit index, this function has a higher latency
func (p *Partition) Read(offset uint64) ([]byte, error) {
	// Get leader commit Index
	// called WaitForAppliedIndex
	return nil, nil
}

func (p *Partition) WaitForAppliedIndex(offset uint64, timeout time.Duration) error {
	ticker := time.NewTicker(AppliedDelay)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if p.raftNode.AppliedIndex() >= offset {
				return nil
			}
		case <-timer.C:
			return ErrTimeoutExpired
		}
	}
}
