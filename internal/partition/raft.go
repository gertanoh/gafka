package partition

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/gertanoh/gafka/internal/log"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

// Raft layer for replication
// Inspired from book Distributed services in go
/*
A Raft instance comprises:
	A finite-state machine that applies the commands you give Raft;
	A log store where Raft stores those commands;
	A stable store where Raft stores the cluster’s configuration—the servers
	in the cluster, their addresses, and so on;
	A snapshot store where Raft stores compact snapshots of its data; and
	A transport that Raft uses to connect with the server’s peers.
*/

const (
	raftTransportMaxPool = 5
	raftTransportTimeout = 10 * time.Second
	snapshotRetain       = 2
)

type record struct {
	Data       []byte
	Offset     uint64
	Term       uint64
	RecordType uint8
}

// Possible optim use protobuf
func (r *record) serializeRecord() ([]byte, error) {
	bin, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return bin, nil
}

func deserializeRecord(data []byte) (record, error) {
	var rec record
	err := json.Unmarshal(data, &rec)
	return rec, err
}

type fsm struct {
	log *log.Log
}

var _ raft.FSM = (*fsm)(nil)

// use log structure for log store. Will increase testing
var _ raft.LogStore = (*logStore)(nil)

type snapshot struct {
	reader io.Reader
}

type logStore struct {
	log *log.Log
}

func newLogStore(dir string, c log.Config) (*logStore, error) {
	log, err := log.NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.log.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.log.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.log.Read(index)
	if err != nil {
		zap.S().Error("GetLog : fail to read", zap.Error(err))
		return err
	}
	rec, err := deserializeRecord(in)
	if err != nil {
		return err
	}
	out.Data = rec.Data
	out.Index = index
	out.Term = rec.Term
	out.Type = raft.LogType(rec.RecordType)
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, raftRec := range records {
		r := &record{
			Data:       raftRec.Data,
			Term:       raftRec.Term,
			RecordType: uint8(raftRec.Type),
		}
		message, err := r.serializeRecord()
		if err != nil {
			return err
		}
		if _, err = l.log.Append(message); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.log.Truncate(max)
}

func (l *fsm) Apply(record *raft.Log) interface{} {
	return l.applyAppend(record.Data)
}

func (l *fsm) applyAppend(data []byte) interface{} {
	offset, err := l.log.Append(data)
	if err != nil {
		return err
	}
	return offset
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

func (f *fsm) Restore(reader io.ReadCloser) error {
	b := make([]byte, log.EncLenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(reader, b)
		if err == io.EOF {
			break
		}
		size := int64(log.Enc.Uint64(b))
		if _, err = io.CopyN(&buf, reader, size); err != nil {
			return err
		}

		var rec record
		if err = json.Unmarshal(buf.Bytes(), &rec); err != nil {
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = rec.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(buf.Bytes()); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

func (p *Partition) setupRaft(dataDir string) error {

	config := raft.DefaultConfig()
	config.LocalID = p.config.LocalID

	// config.Logger = hclog.New(&hclog.LoggerOptions{
	// 	Name:       "raft",
	// 	Level:      hclog.Debug,
	// 	Output:     os.Stderr,
	// 	JSONFormat: true,
	// })

	// below constants are to speed up tests
	if p.config.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = p.config.HeartbeatTimeout
	}
	if p.config.ElectionTimeout != 0 {
		config.ElectionTimeout = p.config.ElectionTimeout
	}
	if p.config.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = p.config.LeaderLeaseTimeout
	}
	if p.config.CommitTimeout != 0 {
		config.CommitTimeout = p.config.CommitTimeout
	}

	fsm := &fsm{log: p.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := p.log.Config
	logConfig.Segment.InitialOffset = 1             // needed by the raft interface
	logStore, err := newLogStore(logDir, logConfig) // TODO optimi use log.NewCache
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft", "snapshot"), snapshotRetain, os.Stderr)
	if err != nil {
		return err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", p.config.BindAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(p.config.BindAddr, tcpAddr, raftTransportMaxPool, raftTransportTimeout, os.Stderr) // TODO let's use global logger here
	if err != nil {
		return err
	}

	p.raftNet = NewTransport(transport)

	p.raftNode, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		p.raftNet,
	)

	if err != nil {
		zap.S().Error("raft setup failed", zap.Error(err))
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if p.config.Bootstrap && !hasState {
		raftConfiguration := raft.Configuration{
			Servers: []raft.Server{{
				ID:       config.LocalID,
				Address:  transport.LocalAddr(),
				Suffrage: raft.Voter,
			}},
		}
		err = p.raftNode.BootstrapCluster(raftConfiguration).Error()
		if err != nil {
			zap.S().Error("Failed to bootstrap cluster", zap.Error(err))
			return err
		}
	}

	return nil
}
