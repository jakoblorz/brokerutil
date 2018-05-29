package driver

import "github.com/jakoblorz/singapoor/stream"

type DriverType int

const (
	MultithreadDriver  DriverType = iota
	SinglethreadDriver DriverType = iota
)

type DriverScaffold interface {
	GetDriverType() DriverType
	NotifyStreamClose() error
	NotifyStreamOpen() error
}

type SingleThreadDriverScaffold interface {
	DriverScaffold
	NotifyMessageTest() (bool, error)
	NotifyMessageRecieve() (stream.Message, error)
	NotifyMessagePublish(stream.Message) error
}

type MultiThreadDriverScaffold interface {
	DriverScaffold
	GetMessageWriterChannel() (chan<- stream.Message, error)
	GetMessageReaderChannel() (<-chan stream.Message, error)
}
