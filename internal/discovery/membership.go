// Package handle broker self discovery using serf package. Functions handle Join and leave of brokers
// Instead of using zookeeper with controllers, gossip protocol with serf has been used
// Serf implements a SWIM protocol, that is eventually consistent
package discovery

import (
	"encoding/json"
	"net"
	"sync"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Serf membership config
type Config struct {
	NodeName       string            // node unique name
	BindAddr       string            // addr for gossiping
	Tags           map[string]string // use to share information
	StartJoinAddrs []string
}

// TODO add versionning to metadata
type MetadataUpdate struct {
	TopicName   string
	PartitionId int
	LeaderAddr  string
}

type Membership struct {
	Config
	handler Handler
	Serf    *serf.Serf
	events  chan serf.Event
	done    chan struct{}
	logger  *zap.SugaredLogger
	wg      sync.WaitGroup
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
	UpdateTopicData(data []MetadataUpdate)
	GetTopicData(topicName string, partitionId int) (MetadataUpdate, bool)
	GetAllTopics() []string
}

func New(handler Handler, config Config) (*Membership, error) {

	m := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.S().Named("membership"),
		done:    make(chan struct{}),
	}
	if err := m.setupSerf(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()

	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName

	m.Serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	m.wg.Add(1)
	go m.eventHandler()

	if m.StartJoinAddrs != nil {
		_, err = m.Serf.Join(m.StartJoinAddrs, true) // TODO do a retry here
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Membership) eventHandler() {
	defer m.wg.Done()

	for {
		select {
		case e := <-m.events:
			switch e.EventType() {
			case serf.EventMemberJoin:
				for _, member := range e.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					m.handleJoin(member)
				}
			case serf.EventMemberLeave, serf.EventMemberFailed:
				for _, member := range e.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						return
					}
					m.handleLeave(member)
				}
			case serf.EventUser:
				m.handleUserEvent(e.(serf.UserEvent))

			case serf.EventQuery:
				m.handleQuery(e.(*serf.Query))
			}
		case <-m.done:
			return
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logger.Errorln("Failed to join", zap.Error(err), zap.String("node name ", member.Name))
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logger.Errorln("Failed to leave", zap.Error(err), zap.String("node name ", member.Name))

	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.Serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.Serf.Members()
}

func (m *Membership) Leave() error {
	close(m.done)
	if err := m.Serf.Leave(); err != nil {
		return err
	}
	m.wg.Wait()
	return nil
}

func (m *Membership) handleUserEvent(e serf.UserEvent) {
	if e.Name == "topic_created" || e.Name == "topic_updated" {
		var topicData []MetadataUpdate
		if err := json.Unmarshal(e.Payload, &topicData); err != nil {
			m.logger.Error("Failed to unmarshall topic data", zap.Error(err))
			return
		}

		// call to handler of broker
		m.handler.UpdateTopicData(topicData)
	}
}

func (m *Membership) handleQuery(q *serf.Query) {
	switch q.Name {
	case "get_partition_leader":
		var topicInfo struct {
			Name        string
			PartitionId int
		}
		if err := json.Unmarshal(q.Payload, &topicInfo); err != nil {
			m.logger.Error("Failed to unmarshal topic name from query", zap.Error(err))
			return
		}

		topicData, exists := m.handler.GetTopicData(topicInfo.Name, topicInfo.PartitionId)
		if !exists {
			return
		}

		res, err := json.Marshal(topicData)
		if err != nil {
			m.logger.Error("Failed to marshal topic response", zap.Error(err))
			return
		}

		err = q.Respond(res)
		if err != nil {
			m.logger.Error("Failed to respond to topic leader query", zap.Error(err))
		}
	case "list_topics":
		topics := m.handler.GetAllTopics()
		res, err := json.Marshal(topics)
		if err != nil {
			m.logger.Error("Failed to marshal all topics response", zap.Error(err))
			return
		}
		err = q.Respond(res)
		if err != nil {
			m.logger.Error("Failed to respond to list_topics_query", zap.Error(err))
		}
	}
}
