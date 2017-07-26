package flows

import (
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	topic := "test"
	s := newSubscription(nil, "", []string{topic})
	timeout := time.Tick(3 * time.Second)
	hello := "hello, world"

	go func(s *Subscription) {
		s.send(topic, []byte(hello))
	}(s)

	for {
		select {
		case <-timeout:
			t.Fatal("didn't receive anything from the channel")
		case v := <-s.Channel():
			if string(v.Topic) != topic {
				t.Fatalf("received ´%s´, expecting ´%s´", v, hello)
			}

			if string(v.Content) != hello {
				t.Fatalf("received ´%s´, expecting ´%s´", v, hello)
			}

			return
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	scenarios := []struct {
		description string
		active      bool
	}{
		{
			description: "it should inactivate a subscription",
			active:      true,
		},
		{
			description: "it should do nothing because a subscription is already inactive",
			active:      false,
		},
	}

	for i, scenario := range scenarios {
		s := newSubscription(&unsubscriberMock{}, "", nil)
		s.active = scenario.active

		if err := s.Unsubscribe(); err != nil {
			t.Fatal(err)
		}

		if s.active {
			t.Errorf("scenario %d (%s): subscription still active after unsubscribing", i, scenario.description)
		}
	}
}

type unsubscriberMock struct{}

func (h *unsubscriberMock) unsubscribe(id string, topics []string) error {
	return nil
}
