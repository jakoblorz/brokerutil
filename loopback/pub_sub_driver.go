package loopback

import (
	"github.com/jakoblorz/brokerutil"
)

// PubSubDriver is the struct to implement both the
// SingleThreadPubSubDriverScaffold interface and the
// MultiThreadPubSubDriverScaffold interface
type PubSubDriver struct {
	executionFlag brokerutil.Flag
	channel       chan interface{}
}

// NewLoopbackPubSubDriver returns a new loopback driver
func NewLoopbackPubSubDriver(executionFlag brokerutil.Flag) (*PubSubDriver, error) {
	return &PubSubDriver{
		executionFlag: executionFlag,
		channel:       make(chan interface{}, 1),
	}, nil
}

// NewLoopbackBlockingPubSubDriver returns a new loopback driver as single thread driver
func NewLoopbackBlockingPubSubDriver() (*PubSubDriver, error) {
	return NewLoopbackPubSubDriver(brokerutil.BlockingExecution)
}

// NewLoopbackConcurrentPubSubDriver returns a new loopback driver as multi thread driver
func NewLoopbackConcurrentPubSubDriver() (*PubSubDriver, error) {
	return NewLoopbackPubSubDriver(brokerutil.ConcurrentExecution)
}

// GetDriverFlags returns the driver type to indicate the
// ability to be used in concurrent environments
func (d *PubSubDriver) GetDriverFlags() []brokerutil.Flag {
	return []brokerutil.Flag{d.executionFlag}
}

// OpenStream can be called to open the stream
func (d *PubSubDriver) OpenStream() error {
	return nil
}

// CloseStream can be called to close the stream
func (d *PubSubDriver) CloseStream() error {
	return nil
}

// ReceiveMessage can be called to recieve a message
func (d *PubSubDriver) ReceiveMessage() (interface{}, error) {
	return <-d.channel, nil
}

// PublishMessage can be called to publish a message
func (d *PubSubDriver) PublishMessage(msg interface{}) error {

	d.channel <- msg
	return nil
}

// GetMessageWriterChannel returns the writer (publish) channel
// of the driver
func (d *PubSubDriver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return d.channel, nil
}

// GetMessageReaderChannel returns the reader (subscribe) channel
// of the driver
func (d *PubSubDriver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return d.channel, nil
}
