package brokerutil

import uuid "github.com/satori/go.uuid"

type roundRobinScheduler struct {
	subscribers map[SubscriberIdentifier]subscriberWrapper
}

func newRoundRobinScheduler() roundRobinScheduler {

	// return new scheduler with initialized subscribers map
	return roundRobinScheduler{
		subscribers: make(map[SubscriberIdentifier]subscriberWrapper),
	}
}

func (s roundRobinScheduler) NotifySubscribers(msg interface{}) error {

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

func (s roundRobinScheduler) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	identifier := SubscriberIdentifier(uuid.NewV4().String())

	var sig = make(chan error)

	s.subscribers[identifier] = subscriberWrapper{
		sig: sig,
		fn:  fn,
	}

	return sig, identifier
}

func (s roundRobinScheduler) SubscribeSync(fn SubscriberFunc) error {

	c, _ := s.SubscribeAsync(fn)

	return <-c
}

func (s roundRobinScheduler) Unsubscribe(identifier SubscriberIdentifier) {

	subscriber, ok := s.subscribers[identifier]
	if ok {
		subscriber.sig <- nil
	}

	delete(s.subscribers, identifier)
}

func (s roundRobinScheduler) UnsubscribeAll() {

	for identifier := range s.subscribers {
		s.Unsubscribe(identifier)
	}
}
