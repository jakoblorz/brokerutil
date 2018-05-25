package stream

type Subscriber interface {
	AddSubscriber(fn func(interface{}) error) chan error
	GetMessageChannel() (<-chan interface{}, error)
	NotifyOnMessageRecieve(interface{}) error
	NotifyOnStreamClose() error
}

type SubscriberEntry struct {
	sig chan error
	fn  func(interface{}) error
}

type SubscriberHost struct {
	subscribers []SubscriberEntry
}

func NewSubscriberHost() *SubscriberHost {
	return &SubscriberHost{
		subscribers: make([]SubscriberEntry, 0),
	}
}

func (s *SubscriberHost) NotifyOnMessageRecieve(msg interface{}) error {

	for i, l := range s.subscribers {

		if err := l.fn(msg); err != nil {

			s.subscribers[i] = s.subscribers[len(s.subscribers)-1]
			s.subscribers = s.subscribers[:len(s.subscribers)-1]

			l.sig <- err
		}
	}

	return nil
}

func (s *SubscriberHost) NotifyOnStreamClose() error {
	s.UnsubscribeAll()

	return nil
}

func (s *SubscriberHost) AddSubscriber(fn func(interface{}) error) chan error {

	var sig = make(chan error)

	s.subscribers = append(s.subscribers, SubscriberEntry{
		sig: sig,
		fn:  fn,
	})

	return sig
}

func (s *SubscriberHost) UnsubscribeAll() {

	for _, l := range s.subscribers {

		// signal no error (nil) but
		// sigTerm event
		l.sig <- nil
	}
}
