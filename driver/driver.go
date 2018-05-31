package driver

type Type int

const (
	MultiThreadPubSubDriver  Type = iota
	SingleThreadPubSubDriver Type = iota
)

type PubSubDriverScaffold interface {
	GetDriverType() Type

	NotifyStreamClose() error
	NotifyStreamOpen() error
}

type SingleThreadPubSubDriverScaffold interface {
	PubSubDriverScaffold
	NotifyMessageTest() (bool, error)
	NotifyMessageRecieve() (interface{}, error)
	NotifyMessagePublish(interface{}) error
}

type MultiThreadPubSubDriverScaffold interface {
	PubSubDriverScaffold
	GetMessageWriterChannel() (chan<- interface{}, error)
	GetMessageReaderChannel() (<-chan interface{}, error)
}
