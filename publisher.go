package flows

import (
	"encoding/json"

	"github.com/gomodule/redigo/redis"
)

type Publisher struct {
	redis redis.Conn
}

func NewPublisher(c redis.Conn) *Publisher {
	return &Publisher{
		redis: c,
	}
}

func (p *Publisher) Publish(topic string, payload []byte) error {
	_, err := p.redis.Do("PUBLISH", topic, payload)

	return err
}

func (p *Publisher) PublishJSON(topic string, object interface{}) error {
	payload, err := json.Marshal(object)

	if err != nil {
		return err
	}

	return p.Publish(topic, payload)
}
