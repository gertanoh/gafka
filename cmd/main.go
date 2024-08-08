package main

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

type FSM struct {
	state map[string]string
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var command map[string]string
	if err := json.Unmarshal(log.Data, &command); err != nil {
		return err
	}
	for k, v := range command {
		f.state[k] = v
	}
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{state: f.state}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	var state map[string]string
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.state = state
	return nil
}

type fsmSnapshot struct {
	state map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.state)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (f *fsmSnapshot) Release() {}

func main() {
	// Initialize zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// Raft configuration
	config := raft.DefaultConfig()

	// Initialize the FSM
	fsm := &FSM{state: make(map[string]string)}

	// Initialize BoltDB for the Raft log store
	logStore, err := raftboltdb.NewBoltStore("raft-log.bolt")
	if err != nil {
		logger.Fatal("failed to create BoltDB log store", zap.Error(err))
	}

	// Initialize stable store
	stableStore, err := raftboltdb.NewBoltStore("raft-stable.bolt")
	if err != nil {
		logger.Fatal("failed to create BoltDB stable store", zap.Error(err))
	}

	// Initialize snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore("raft-snapshots", 1, os.Stderr)
	if err != nil {
		logger.Fatal("failed to create snapshot store", zap.Error(err))
	}

	// Initialize TCP transport
	transport, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, 10*time.Second, os.Stderr)
	if err != nil {
		logger.Fatal("failed to create TCP transport", zap.Error(err))
	}

	// Initialize the Raft node
	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logger.Fatal("failed to create Raft node", zap.Error(err))
	}

	// Example command to apply to the Raft log
	command := map[string]string{"foo": "bar"}
	data, err := json.Marshal(command)
	if err != nil {
		logger.Fatal("failed to marshal command", zap.Error(err))
	}

	// Apply a command to the Raft log
	future := raftNode.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		logger.Fatal("failed to apply command to Raft log", zap.Error(err))
	}

	// Simulate reading the state after applying the command
	readState(raftNode, logger)
}

func readState(raftNode *raft.Raft, logger *zap.Logger) {
	future := raftNode.GetConfiguration()
	if err := future.Error(); err != nil {
		logger.Fatal("failed to get Raft configuration", zap.Error(err))
	}

	// Request the read index
	readFuture := raftNode.(nil)
	if err := readFuture.Error(); err != nil {
		logger.Fatal("failed to get read index", zap.Error(err))
	}

	// Wait for the read index to be acknowledged
	readIndex := readFuture.Index()

	// Read the state using the read index
	state := raftNode.FSM().(*FSM)
	logger.Info("Read state",
		zap.String("foo", state.state["foo"]),
		zap.Uint64("readIndex", readIndex),
	)
}
