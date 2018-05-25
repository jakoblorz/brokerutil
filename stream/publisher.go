package stream

type Publisher interface {
	NotifyOnMessagePublish(interface{}) error
}
