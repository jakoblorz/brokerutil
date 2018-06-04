package loopback

import (
	"github.com/jakoblorz/brokerutil"
)

// PubSub is the struct to implement both the
// SingleThreadPubSubDriverScaffold interface and the
// MultiThreadPubSubDriverScaffold interface
type PubSub struct {
	executionFlag brokerutil.Flag
	channel       chan interface{}
}

// NewLoopbackPubSub returns a new loopback driver
func NewLoopbackPubSub(executionFlag brokerutil.Flag) (*PubSub, error) {
	return &PubSub{
		executionFlag: executionFlag,
		channel:       make(chan interface{}),
	}, nil
}

// NewLoopbackBlockingPubSub returns a new loopback driver as single thread driver
func NewLoopbackBlockingPubSub() (brokerutil.BlockingPubSubDriverScaffold, error) {
	return NewLoopbackPubSub(brokerutil.RequiresBlockingExecution)
}

// NewLoopbackConcurrentPubSub returns a new loopback driver as multi thread driver
func NewLoopbackConcurrentPubSub() (brokerutil.ConcurrentPubSubDriverScaffold, error) {
	return NewLoopbackPubSub(brokerutil.RequiresConcurrentExecution)
}

// GetDriverFlags returns the driver type to indicate the
// ability to be used in concurrent environments
func (d PubSub) GetDriverFlags() []brokerutil.Flag {
	return []brokerutil.Flag{d.executionFlag}
}

// CloseStream can be called to close the stream
func (d PubSub) CloseStream() error {
	return nil
}

// OpenStream can be called to open the stream
func (d PubSub) OpenStream() error {
	return nil
}

// ReceiveMessage can be called to recieve a message
func (d PubSub) ReceiveMessage() (interface{}, error) {
	return <-d.channel, nil
}

// PublishMessage can be called to publish a message
func (d PubSub) PublishMessage(msg interface{}) error {

	d.channel <- msg
	return nil
}

// GetMessageWriterChannel returns the writer (publish) channel
// of the driver
func (d PubSub) GetMessageWriterChannel() (chan<- interface{}, error) {
	return d.channel, nil
}

// GetMessageReaderChannel returns the reader (subscribe) channel
// of the driver
func (d PubSub) GetMessageReaderChannel() (<-chan interface{}, error) {
	return d.channel, nil
}
