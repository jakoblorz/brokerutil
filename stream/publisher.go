package stream

type Publisher interface {
	GetMessageChannel() (chan<- interface{}, error)
	NotifyOnMessagePublish(interface{}) error
}
