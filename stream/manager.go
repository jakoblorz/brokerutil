package stream

type SubscriberFunc func(interface{}) error

type subscriberEntry struct {
	sig chan error
	fn  SubscriberFunc
}

type SubscriberManager struct {
	subscribers []subscriberEntry
}

func NewSubscriberManager() *SubscriberManager {
	return &SubscriberManager{
		subscribers: make([]subscriberEntry, 0),
	}
}

func (s *SubscriberManager) NotifyOnMessageRecieve(msg interface{}) error {

	for i, l := range s.subscribers {

		if err := l.fn(msg); err != nil {

			s.subscribers[i] = s.subscribers[len(s.subscribers)-1]
			s.subscribers = s.subscribers[:len(s.subscribers)-1]

			l.sig <- err
		}
	}

	return nil
}

func (s *SubscriberManager) NotifyOnStreamClose() error {
	s.UnsubscribeAll()

	return nil
}

func (s *SubscriberManager) Subscribe(fn SubscriberFunc) chan error {

	var sig = make(chan error)

	s.subscribers = append(s.subscribers, subscriberEntry{
		sig: sig,
		fn:  fn,
	})

	return sig
}

func (s *SubscriberManager) UnsubscribeAll() {

	for _, l := range s.subscribers {

		// signal no error (nil) but
		// sigTerm event
		l.sig <- nil
	}
}
