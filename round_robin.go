package brokerutil

import (
	"sync"

	uuid "github.com/satori/go.uuid"
)

type roundRobinScheduler struct {
	subscribers map[SubscriberIdentifier]subscriberWrapper
	m           *sync.Mutex
}

func newRoundRobinScheduler() roundRobinScheduler {

	// return new scheduler with initialized subscribers map
	return roundRobinScheduler{
		subscribers: make(map[SubscriberIdentifier]subscriberWrapper),
		m:           &sync.Mutex{},
	}
}

func (s *roundRobinScheduler) NotifySubscribers(msg interface{}) error {

	s.m.Lock()

	// copy subscribers
	subscribers := s.subscribers

	s.m.Unlock()

	// iterate over all subscribers, calling the callback function
	// in case of an error, send the error in the subscriber's
	// error channel, then remove the subscriber from the rotation
	for identifier, subscriber := range subscribers {

		if err := subscriber.fn(msg); err != nil {

			subscriber.sig <- err

			delete(s.subscribers, identifier)
		}
	}

	return nil
}

func (s *roundRobinScheduler) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	identifier := SubscriberIdentifier(uuid.NewV4().String())

	var sig = make(chan error)

	s.m.Lock()

	s.subscribers[identifier] = subscriberWrapper{
		sig: sig,
		fn:  fn,
	}

	s.m.Unlock()

	return sig, identifier
}

func (s *roundRobinScheduler) SubscribeSync(fn SubscriberFunc) error {

	c, _ := s.SubscribeAsync(fn)

	return <-c
}

func (s *roundRobinScheduler) Unsubscribe(identifier SubscriberIdentifier) {

	s.m.Lock()

	subscriber, ok := s.subscribers[identifier]

	s.m.Unlock()

	if ok {
		subscriber.sig <- nil
	}

	delete(s.subscribers, identifier)
}

func (s *roundRobinScheduler) UnsubscribeAll() {

	s.m.Lock()

	subscribers := s.subscribers

	s.m.Unlock()

	for identifier := range subscribers {
		s.Unsubscribe(identifier)
	}
}
