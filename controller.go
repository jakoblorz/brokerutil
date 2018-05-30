package singapoor

import (
	"github.com/jakoblorz/singapoor/stream"
	uuid "github.com/satori/go.uuid"
)

type SubscriberIdentifier string
type SubscriberFunc func(stream.Message) error

type subscriberWrapper struct {
	sig chan error
	fn  SubscriberFunc
}

type subscriberController struct {
	subscribers map[SubscriberIdentifier]subscriberWrapper
}

func newSubscriberController() subscriberController {
	return subscriberController{
		subscribers: make(map[SubscriberIdentifier]subscriberWrapper),
	}
}

func (s *subscriberController) NotifySubscribers(msg stream.Message) error {

	for identifier, subscriber := range s.subscribers {

		if err := subscriber.fn(msg); err != nil {

			subscriber.sig <- err

			delete(s.subscribers, identifier)
		}
	}

	return nil
}

func (s *subscriberController) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	identifier := SubscriberIdentifier(uuid.NewV4().String())

	var sig = make(chan error)

	s.subscribers[identifier] = subscriberWrapper{
		sig: sig,
		fn:  fn,
	}

	return sig, identifier
}

func (s *subscriberController) SubscribeSync(fn SubscriberFunc) error {

	c, _ := s.SubscribeAsync(fn)

	return <-c
}

func (s *subscriberController) Unsubscribe(identifier SubscriberIdentifier) {

	subscriber, ok := s.subscribers[identifier]
	if ok {
		subscriber.sig <- nil
	}

	delete(s.subscribers, identifier)
}

func (s *subscriberController) UnsubscribeAll() {

	for identifier := range s.subscribers {
		s.Unsubscribe(identifier)
	}
}
