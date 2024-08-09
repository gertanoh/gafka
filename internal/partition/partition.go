// Package handles the partition. Partition is replicated using raft consensus module
// handles write/read

package partition

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/gertanoh/gafka/internal/log"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

var (
	ErrNotLeader                = errors.New("not leader")
	ErrTimeoutExpired           = errors.New("timeout expired")
	ErrTimeoutWaitingForApplied = errors.New("request timeout waiting for appliedIndex to match commit")
)

const (
	AppliedDelayTicker = 100 * time.Millisecond
	WaitAppliedFSM     = 1 * time.Second
)

type Partition struct {
	id        int
	topicName string
	log       *log.Log
	raftNode  *raft.Raft
	mu        sync.RWMutex
	raft      struct {
		raft.Config
		raftNet  *Transport
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
	p.log, err = log.NewLog(topicName, config)
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

// Return leader commit index. For followers, it is retrieved atomically from the appendentries sent by the leader over the network
func (p *Partition) LeaderCommitIndex() (uint64, error) {
	if p.raftNode.State() == raft.Leader {
		return p.raftNode.CommitIndex(), nil
	}
	return p.raft.raftNet.LeaderCommitIndex(), nil
}

// Read from any node local storage. Might serve stale read
func (p *Partition) ReadNonLinear(idx uint64) ([]byte, error) {
	return p.log.Read(idx)
}

// Read served from Leader, check are applied to prevent loss of leadership
func (p *Partition) ReadLinearFromLeader(idx uint64) ([]byte, error) {
	if p.raftNode.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	future := p.raftNode.VerifyLeader()
	if future.Error() != nil {
		return nil, ErrNotLeader
	}
	// wait for commit index to match appliedIndex
	if p.WaitForAppliedIndex(idx, 1*time.Second) != nil {
		return nil, ErrTimeoutWaitingForApplied
	}
	return p.log.Read(idx)
}

// Read is done from follower/leader, attempt to implement readIndex optimization
func (p *Partition) Read(idx uint64) ([]byte, error) {
	// Get leader commit Index
	// called WaitForAppliedIndex
	if p.raftNode.State() == raft.Leader {
		return p.ReadLinearFromLeader(idx)
	}

	leaderCommitIdx, err := p.LeaderCommitIndex()
	if err != nil {
		zap.S().Error("failed to get leader commit index", zap.Error(err))
		return nil, err
	}
	// wait for local commit index to be applied to fsm
	if p.WaitForAppliedIndex(leaderCommitIdx, WaitAppliedFSM) != nil {
		return nil, ErrTimeoutWaitingForApplied
	}
	return p.log.Read(idx)
}

func (p *Partition) WaitForAppliedIndex(offset uint64, timeout time.Duration) error {
	ticker := time.NewTicker(AppliedDelayTicker)
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
