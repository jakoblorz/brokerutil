package stream

type Subscriber interface {
	Subscribe(SubscriberFunc) chan error
	GetMessageChannel() (<-chan interface{}, error)
	NotifyOnMessageRecieve(interface{}) error
	NotifyOnStreamClose() error
}
