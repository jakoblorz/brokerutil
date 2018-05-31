package driver

import "github.com/jakoblorz/brokerutil/stream"

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
	NotifyMessageRecieve() (stream.Message, error)
	NotifyMessagePublish(stream.Message) error
}

type MultiThreadScaffold interface {
	Scaffold
	GetMessageWriterChannel() (chan<- stream.Message, error)
	GetMessageReaderChannel() (<-chan stream.Message, error)
}
