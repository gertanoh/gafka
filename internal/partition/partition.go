// Package handles the partition. Partition is replicated using raft consensus module
// handles write/read

package partition

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/gertanoh/gafka/internal/log"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
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
	WriteTimeout       = 10 * time.Second
)

const (
	ReadConsistencyDefault = iota // default mode : read from followers node, might fail as follower might not have this offset
	ReadConsistencyWeak    = 1    // read from leader, check leader state locally
	ReadConsistencyStrong  = 2    // read from leader, check leader state locally
)

type Config struct {
	raft.Config
	BindAddr  string
	Bootstrap bool
	logConf   log.Config
}

type raftServer struct {
	Id       string
	Rpc      string
	isLeader bool
}

type LeadershipChangeHandler func(partition *Partition, isLeader bool)

type Partition struct {
	Id          int
	TopicName   string
	log         *log.Log
	raftNode    *raft.Raft
	raftNet     *Transport
	logStore    *logStore
	stableStore *raftboltdb.BoltStore
	config      Config
	dataDir     string

	leaderObserver *raft.Observer
	onLeaderChange LeadershipChangeHandler
}

func NewPartition(id int, TopicName string, config Config) (*Partition, error) {

	p := &Partition{}
	p.dataDir = TopicName + "-" + strconv.Itoa(id)
	if err := os.MkdirAll(p.dataDir, 0755); err != nil {
		return nil, err
	}

	var err error
	p.log, err = log.NewLog(p.dataDir, config.logConf)
	if err != nil {
		return nil, err
	}

	p.config = config
	if err := p.setupRaft(p.dataDir); err != nil {
		zap.S().Error("Fail to setup raft.", zap.Error(err))
		return nil, err
	}

	p.Id = id
	p.TopicName = TopicName
	return p, nil
}

func (p *Partition) Write(message []byte) error {

	if p.raftNode.State() != raft.Leader {
		return ErrNotLeader
	}

	future := p.raftNode.Apply(message, WriteTimeout)
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

// Read is done from follower/leader, attempt to implement readIndex optimization
func (p *Partition) Read(idx uint64, consistencyLevel uint8) ([]byte, error) {

	if consistencyLevel == ReadConsistencyStrong { // read only from leader
		if p.raftNode.State() != raft.Leader {
			return nil, ErrNotLeader
		}
		future := p.raftNode.VerifyLeader()
		if future.Error() != nil {
			return nil, ErrNotLeader
		}
		if p.WaitForAppliedIndex(p.raftNode.CommitIndex(), WaitAppliedFSM) != nil {
			return nil, ErrTimeoutWaitingForApplied
		}
	} else if consistencyLevel == ReadConsistencyWeak { // Read from any node, from follower check that we are up to date
		var commitIdx uint64
		if p.raftNode.State() == raft.Leader {
			commitIdx = p.raftNode.CommitIndex()
		} else {
			// Get leader commit Index
			var err error
			commitIdx, err = p.LeaderCommitIndex()
			if err != nil {
				zap.S().Error("failed to get leader commit index", zap.Error(err))
				return nil, err
			}
		}
		// wait for local commit index to be applied to fsm
		if p.WaitForAppliedIndex(commitIdx, WaitAppliedFSM) != nil {
			return nil, ErrTimeoutWaitingForApplied
		}
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
	raftConfig := p.raftNode.GetConfiguration()
	if err := raftConfig.Error(); err != nil {
		zap.S().Error("failed to retrieve raft configuration", zap.Error(err))
		return err
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
	if p.leaderObserver != nil {
		p.raftNode.DeregisterObserver(p.leaderObserver)
	}

	future := p.raftNode.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	if p.stableStore != nil {
		p.stableStore.Close()
	}
	if p.logStore != nil {
		p.logStore.log.Close()
	}
	if err := p.raftNet.Close(); err != nil {
		return err
	}
	p.log.Close()

	return nil
}

func (p *Partition) Remove() error {
	future := p.raftNode.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	if p.stableStore != nil {
		p.stableStore.Close()
	}
	if p.logStore != nil {
		p.logStore.log.Close()
	}
	if err := p.raftNet.Close(); err != nil {
		return err
	}
	p.log.Remove()

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

// return lists of servers in raft cluster
func (p *Partition) GetServers() ([]raftServer, error) {
	raftConfig := p.raftNode.GetConfiguration()
	if err := raftConfig.Error(); err != nil {
		zap.S().Error("failed to retrieve raft configuration", zap.Error(err))
		return nil, err
	}

	var res []raftServer

	// check if node is not already present
	// serverId and serverAddress need to match, else we will remove the node and add again
	for _, srv := range raftConfig.Configuration().Servers {
		raftS := raftServer{
			Id:       string(srv.ID),
			Rpc:      string(srv.Address),
			isLeader: p.raftNode.Leader() == srv.Address,
		}
		res = append(res, raftS)
	}
	return res, nil
}

// Setup leadership change observer
func (p *Partition) observeLeadership(handler LeadershipChangeHandler) {
	p.onLeaderChange = handler
	obsChan := make(chan raft.Observation, 1)
	observer := raft.NewObserver(obsChan, false,
		func(o *raft.Observation) bool {
			switch o.Data.(type) {
			case raft.LeaderObservation, raft.RaftState:
				return true
			default:
				return false
			}
		})

	p.raftNode.RegisterObserver(observer)
	p.leaderObserver = observer

	// Start goroutine to watch leadership changes
	go func() {
		for range obsChan {
			isLeader := p.raftNode.State() == raft.Leader
			if handler != nil {
				handler(p, isLeader)
			}
		}
	}()
}
