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
type TopicData struct {
	Partitions map[int]string `json:"partitions"` // Partition Id to leader rpc
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	done    chan struct{}
	logger  *zap.SugaredLogger
	wg      sync.WaitGroup
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
	UpdateTopicData(topicName string, data TopicData)
	GetTopicData(topicName string) (TopicData, bool)
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

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	m.wg.Add(1)
	go m.eventHandler()

	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true) // TODO do a retry here
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
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	close(m.done)
	if err := m.serf.Leave(); err != nil {
		return err
	}
	m.wg.Wait()
	return nil
}

func (m *Membership) handleUserEvent(e serf.UserEvent) {
	if e.Name == "topic_created" || e.Name == "topic_updated" {
		var topicData struct {
			Name string
			Data TopicData
		}
		if err := json.Unmarshal(e.Payload, &topicData); err != nil {
			m.logger.Error("Failed to unmarshall topic data", zap.Error(err))
			return
		}

		// call to handler of broker
		m.handler.UpdateTopicData(topicData.Name, topicData.Data)
	}
}

func (m *Membership) handleQuery(q *serf.Query) {
	if q.Name == "get_topic_leader" {
		var topicName string
		if err := json.Unmarshal(q.Payload, &topicName); err != nil {
			m.logger.Error("Failed to unmarshal topic name from query", zap.Error(err))
			return
		}

		topicData, exists := m.handler.GetTopicData(topicName)
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
			m.logger.Error("Failed to respond to query", zap.Error(err))
		}
	}
}
