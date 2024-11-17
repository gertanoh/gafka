package discovery

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			len(handler.leaves) == 0
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].Leave())
	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)

	m, h4 := setupMember(t, m)
	require.Eventually(t, func() bool {
		return len(handler.joins) == 3 &&
			len(m[0].Members()) == 4 &&
			len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)

	// Test topic creation and query
	topicName := "test-topic"
	topicData := make([]MetadataUpdate, 1)
	topicData[0] = MetadataUpdate{
		TopicName:   topicName,
		PartitionId: 0,
		LeaderAddr:  "leader-1",
	}

	// Broadcast topic creation
	payload, err := json.Marshal(topicData)
	require.NoError(t, err)
	require.NoError(t, m[0].Serf.UserEvent("topic_created", payload, true))

	// Wait for the event to be processed
	time.Sleep(1 * time.Second)

	// Verify that the topic data was updated in the handler
	require.Eventually(t, func() bool {
		h4.mu.Lock()
		defer h4.mu.Unlock()
		data, exists := h4.topicData[topicName]
		if !exists {
			return false
		}
		addr, exists := data[topicData[0].PartitionId]
		if !exists {
			return false
		}
		return addr == "leader-1"
	}, 1*time.Second, 250*time.Millisecond)

	// Test query for topic leader
	queryPayload, err := json.Marshal(struct {
		Name        string
		PartitionId int
	}{
		Name:        topicName,
		PartitionId: 0,
	})
	require.NoError(t, err)

	resp, err := m[1].Serf.Query("get_partition_leader", queryPayload, &serf.QueryParam{
		Timeout: 1 * time.Second,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var receivedTopicData MetadataUpdate
		for r := range resp.ResponseCh() {
			err = json.Unmarshal(r.Payload, &receivedTopicData)
			require.NoError(t, err)
			break
		}
		return reflect.DeepEqual(topicData[0], receivedTopicData)

	}, time.Second, 250*time.Millisecond)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership,
	*handler) {

	id := len(members)
	port := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 5)
		h.leaves = make(chan string, 5)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}
	h.topicData = make(map[string]map[int]string)
	h.nodeName = fmt.Sprintf("%d", id)
	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

type handler struct {
	joins     chan map[string]string
	leaves    chan string
	topicData map[string]map[int]string
	mu        sync.RWMutex
	nodeName  string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func (h *handler) UpdateTopicData(data []MetadataUpdate) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, topicData := range data {
		if h.topicData[topicData.TopicName] == nil {
			h.topicData[topicData.TopicName] = make(map[int]string)
		}
		h.topicData[topicData.TopicName][topicData.PartitionId] = topicData.LeaderAddr
	}
}

func (h *handler) GetTopicData(topicName string, partitionId int) (MetadataUpdate, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.topicData[topicName] == nil {
		h.topicData[topicName] = make(map[int]string)
	}
	data, exists := h.topicData[topicName]
	if !exists {
		return MetadataUpdate{}, exists
	}
	return MetadataUpdate{TopicName: topicName, PartitionId: partitionId, LeaderAddr: data[partitionId]}, exists
}

func (h *handler) GetAllTopics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	topics := make([]string, 0, len(h.topicData))
	for topicName := range h.topicData {
		topics = append(topics, topicName)
	}
	return topics
}
