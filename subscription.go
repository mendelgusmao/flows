package flows

import "sync"

type unsubscriber interface {
	unsubscribe(string, []string) error
}

type Subscription struct {
	unsubscriber unsubscriber
	channel      chan Response
	active       bool
	topics       []string
	id           string
	mutex        *sync.RWMutex
}

func newSubscription(u unsubscriber, id string, topics []string) *Subscription {
	return &Subscription{
		unsubscriber: u,
		channel:      make(chan Response, 0),
		topics:       topics,
		id:           id,
		active:       true,
		mutex:        &sync.RWMutex{},
	}
}

func (s *Subscription) send(channel string, data []byte) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.active {
		s.channel <- Response{
			Topic:   channel,
			Content: data,
		}
	}
}

func (s *Subscription) Unsubscribe() error {
	s.mutex.Lock()

	if s.active {
		s.active = false
		close(s.channel)

		s.mutex.Unlock()
		return s.unsubscriber.unsubscribe(s.id, s.topics)
	}

	s.mutex.Unlock()
	return nil
}

func (s *Subscription) Channel() chan Response {
	return s.channel
}
