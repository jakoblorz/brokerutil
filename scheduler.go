package brokerutil

import (
	uuid "github.com/satori/go.uuid"
)

type SubscriberIdentifier string
type SubscriberFunc func(interface{}) error

type subscriberWrapper struct {
	sig chan error
	fn  SubscriberFunc
}

type scheduler struct {
	subscribers map[SubscriberIdentifier]subscriberWrapper
}

func newScheduler() scheduler {
	return scheduler{
		subscribers: make(map[SubscriberIdentifier]subscriberWrapper),
	}
}

func (s *scheduler) NotifySubscribers(msg interface{}) error {

	for identifier, subscriber := range s.subscribers {

		if err := subscriber.fn(msg); err != nil {

			subscriber.sig <- err

			delete(s.subscribers, identifier)
		}
	}

	return nil
}

func (s *scheduler) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	identifier := SubscriberIdentifier(uuid.NewV4().String())

	var sig = make(chan error)

	s.subscribers[identifier] = subscriberWrapper{
		sig: sig,
		fn:  fn,
	}

	return sig, identifier
}

func (s *scheduler) SubscribeSync(fn SubscriberFunc) error {

	c, _ := s.SubscribeAsync(fn)

	return <-c
}

func (s *scheduler) Unsubscribe(identifier SubscriberIdentifier) {

	subscriber, ok := s.subscribers[identifier]
	if ok {
		subscriber.sig <- nil
	}

	delete(s.subscribers, identifier)
}

func (s *scheduler) UnsubscribeAll() {

	for identifier := range s.subscribers {
		s.Unsubscribe(identifier)
	}
}
