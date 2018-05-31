package driver

type Type int

const (
	MultiThreadDriver  Type = iota
	SingleThreadDriver Type = iota
)

type Scaffold interface {
	GetDriverType() Type

	NotifyStreamClose() error
	NotifyStreamOpen() error
}

type SingleThreadScaffold interface {
	Scaffold
	NotifyMessageTest() (bool, error)
	NotifyMessageRecieve() (interface{}, error)
	NotifyMessagePublish(interface{}) error
}

type MultiThreadScaffold interface {
	Scaffold
	GetMessageWriterChannel() (chan<- interface{}, error)
	GetMessageReaderChannel() (<-chan interface{}, error)
}
