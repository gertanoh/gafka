// Package handles the partition. Partition is replicated using raft consensus module
// handles write/read

package partition

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/gertanoh/gafka/internal/log"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

var (
	ErrNotLeader                = errors.New("not leader")
	ErrTimeoutExpired           = errors.New("timeout expired")
	ErrTimeoutWaitingForApplied = errors.New("request timeout waiting for appliedIndex to match commit")
	ErrTimeoutWaitingForLeader  = errors.New("request timeout waiting for leader to connect")
)

const (
	AppliedDelayTicker = 100 * time.Millisecond
	WaitAppliedFSM     = 1 * time.Second
)

type Config struct {
	raft.Config
	BindAddr string
	Boostrap bool
	logConf  log.Config
}

type Partition struct {
	id        int
	topicName string
	log       *log.Log
	raftNode  *raft.Raft
	raftNet   *Transport
	config    Config
}

func NewPartition(id int, topicName string, config Config) (*Partition, error) {

	p := &Partition{}
	if err := os.MkdirAll(topicName, 0755); err != nil {
		return nil, err
	}

	var err error
	p.log, err = log.NewLog(topicName, config.logConf)
	if err != nil {
		return nil, err
	}

	p.config = config
	if err := p.setupRaft(topicName); err != nil {
		zap.S().Error("Fail to setup raft.", zap.Error(err))
		return nil, err
	}

	p.id = id
	p.topicName = topicName
	return p, nil
}

func (p *Partition) Write(message []byte) error {
	var buf bytes.Buffer
	_, err := buf.Write(message)
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
	return p.raftNet.LeaderCommitIndex(), nil
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

// Handle another partition joining the replication cluster
func (p *Partition) Join(id string, addr string) error {
	f := p.raftNode.GetConfiguration()
	if err := f.Error(); err != nil {
		return err
	}

	raftConfig := p.raftNode.GetConfiguration()
	if err := raftConfig.Error(); err != nil {
		zap.S().Error("failed to retrieve raft configuration", zap.Error(err))
		return nil
	}

	// check if node is not already present
	// serverId and serverAddress need to match, else we will remove the node and add again
	for _, srv := range raftConfig.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
			zap.S().Info("node %s already present", id)
			return nil
		}

		// remove node
		removeF := p.raftNode.RemoveServer(raft.ServerID(id), 0, 0)
		if err := removeF.Error(); err != nil {
			return err
		}
	}
	addF := p.raftNode.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := addF.Error(); err != nil {
		zap.S().Error("failed to add voter ", zap.Error(err))
		return err
	}

	return nil
}

func (p *Partition) Leave(id string) error {
	removeF := p.raftNode.RemoveServer(raft.ServerID(id), 0, 0)
	return removeF.Error()
}
func (p *Partition) Close() error {
	future := p.raftNode.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}
	p.log.Close()
	return nil
}

func (p *Partition) WaitForLeader(timeout time.Duration) (string, error) {
	var err error
	var leaderAddr string

	check := func() bool {
		leaderAddr, _ := p.raftNode.LeaderWithID()
		if err == nil && leaderAddr != "" {
			return true
		}
		return false
	}

	// try the fast path
	if check() {
		return leaderAddr, nil
	}
	tck := time.NewTicker(1 * time.Second)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for {
		select {
		case <-tck.C:
			if check() {
				return leaderAddr, nil
			}
		case <-tmr.C:
			if err != nil {
				zap.S().Error("timed out waiting for leader, last error: ", zap.Error(err))

			}
			return "", ErrTimeoutWaitingForLeader
		}
	}
}
