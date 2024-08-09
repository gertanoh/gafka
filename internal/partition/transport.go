package partition

import (
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// Custom transport layer to add custom functionality
// storing atomically the commit index, which is sent by the leader for each appendentries request
type Transport struct {
	*raft.NetworkTransport
	leaderCommitIndex *atomic.Uint64
}

func NewTransport(transport *raft.NetworkTransport) *Transport {
	return &Transport{
		NetworkTransport:  transport,
		leaderCommitIndex: &atomic.Uint64{},
	}
}

func (ct *Transport) LeaderCommitIndex() uint64 {
	return ct.leaderCommitIndex.Load()
}

func (ct *Transport) Consumer() <-chan raft.RPC {
	ch := make(chan raft.RPC)
	srcCh := ct.NetworkTransport.Consumer()
	go func() {
		for rpc := range srcCh {
			if cmd, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
				ct.leaderCommitIndex.Store(cmd.LeaderCommitIndex)
			}
			ch <- rpc
		}
	}()
	return ch
}
