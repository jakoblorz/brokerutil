package brokerutil

import (
	uuid "github.com/satori/go.uuid"
)

// SubscriberIdentifier uniquely identifies a callback function
//
// These identifiers are generated once a callback function is
// created in the internal subscriber map. You can use it
// to remove a subscriber from scheduling rotation
// which is commonly called unsubscribing.
type SubscriberIdentifier string

// SubscriberFunc is the type of a callback function
type SubscriberFunc func(interface{}) error

type subscriberWrapper struct {
	sig chan error
	fn  SubscriberFunc
}

type scheduler struct {
	subscribers map[SubscriberIdentifier]subscriberWrapper
}

func newScheduler() scheduler {

	// return new scheduler with initialized subscribers map
	return scheduler{
		subscribers: make(map[SubscriberIdentifier]subscriberWrapper),
	}
}

func (s *scheduler) NotifySubscribers(msg interface{}) error {

	// iterate over all subscribers, calling the callback function
	// in case of an error, send the error in the subscriber's
	// error channel, then remove the subscriber from the rotation
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
