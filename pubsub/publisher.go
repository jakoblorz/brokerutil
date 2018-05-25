package pubsub

type Publisher interface {
	GetMessageChannel() (chan<- interface{}, error)
	NotifyOnMessagePublish(interface{}) error
}
