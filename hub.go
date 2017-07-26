package flows

import (
	"fmt"
	"log"
	"sync"

	"github.com/gomodule/redigo/redis"
)

type Hub struct {
	redis         *redis.PubSubConn
	subscriptions map[string]map[string]*Subscription
	mutex         *sync.RWMutex
}

var errEmptyTopicsList = fmt.Errorf("can't create subscription. empty topics list")

func NewHub(c redis.Conn) *Hub {
	return &Hub{
		subscriptions: make(map[string]map[string]*Subscription),
		redis: &redis.PubSubConn{
			Conn: c,
		},
		mutex: &sync.RWMutex{},
	}
}

func (h *Hub) Subscribe(id string, topics []string) (*Subscription, error) {
	if len(topics) == 0 {
		return nil, errEmptyTopicsList
	}

	args := make([]interface{}, 0)

	h.mutex.RLock()

	for _, topic := range topics {
		if _, ok := h.subscriptions[topic]; ok {
			continue
		}

		args = append(args, topic)
	}

	h.mutex.RUnlock()

	if len(args) > 0 {
		if err := h.redis.Subscribe(args...); err != nil {
			return nil, err
		}
	}

	s := newSubscription(h, id, topics)

	h.mutex.Lock()

	for _, topic := range topics {
		if _, ok := h.subscriptions[topic]; !ok {
			h.subscriptions[topic] = make(map[string]*Subscription, 0)
		}

		h.subscriptions[topic][s.id] = s
	}

	h.mutex.Unlock()
	return s, nil
}

func (h *Hub) Deliver() {
	for {
		switch v := h.redis.Receive().(type) {
		case redis.Message:
			receivers := make(map[string]*Subscription)
			h.mutex.RLock()

			if subscriptions, ok := h.subscriptions[v.Channel]; ok {
				for id, subscription := range subscriptions {
					receivers[id] = subscription
				}
			} else {
				log.Printf("topic “%s” was not found in the hub\n", v)
			}

			h.mutex.RUnlock()

			for _, subscription := range receivers {
				subscription.send(v.Channel, v.Data)
			}
		case redis.Subscription:
			log.Printf("topic “%s” has %d subscribers\n", v.Channel, v.Count)
		case error:
			log.Printf("error receiving data from redis: %s\n", v)
		}
	}
}

func (h *Hub) unsubscribe(id string, topics []string) error {
	discardedTopics := make([]interface{}, 0)
	h.mutex.Lock()

	for _, topic := range topics {
		if _, ok := h.subscriptions[topic]; !ok {
			continue
		}

		if _, ok := h.subscriptions[topic][id]; ok {
			delete(h.subscriptions[topic], id)
		}

		if len(h.subscriptions[topic]) > 0 {
			continue
		}

		discardedTopics = append(discardedTopics, topic)
		delete(h.subscriptions, topic)
	}

	h.mutex.Unlock()

	if len(discardedTopics) == 0 {
		return nil
	}

	return h.redis.Unsubscribe(discardedTopics...)
}
