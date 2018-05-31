package loopback

import (
	"github.com/jakoblorz/brokerutil/driver"
)

// Driver is the struct to implement both the
// SingleThreadPubSubDriverScaffold interface and the
// MultiThreadPubSubDriverScaffold interface
type Driver struct {
	driverType driver.PubSubDriverType
	channel    chan interface{}
}

// NewDriver returns a new loopback driver
func NewDriver(driverType driver.PubSubDriverType) (*Driver, error) {
	return &Driver{
		driverType: driverType,
		channel:    make(chan interface{}),
	}, nil
}

// NewSingleThreadDriver returns a new loopback driver as single thread driver
func NewSingleThreadDriver() (driver.SingleThreadPubSubDriverScaffold, error) {
	return NewDriver(driver.SingleThreadPubSubDriver)
}

// NewMultiThreadDriver returns a new loopback driver as multi thread driver
func NewMultiThreadDriver() (driver.MultiThreadPubSubDriverScaffold, error) {
	return NewDriver(driver.MultiThreadPubSubDriver)
}

// GetDriverType returns the driver type to indicate the
// ability to be used in concurrent environments
func (d Driver) GetDriverType() driver.PubSubDriverType {
	return d.driverType
}

// NotifyStreamClose can be called to close the stream
func (d Driver) NotifyStreamClose() error {
	return nil
}

// NotifyStreamOpen can be called to open the stream
func (d Driver) NotifyStreamOpen() error {
	return nil
}

// NotifyMessageTest can be called to test if a new message
// is pending
func (d Driver) NotifyMessageTest() (bool, error) {
	return true, nil
}

// NotifyMessageRecieve can be called to recieve a message
func (d Driver) NotifyMessageRecieve() (interface{}, error) {
	return <-d.channel, nil
}

// NotifyMessagePublish can be called to publish a message
func (d Driver) NotifyMessagePublish(msg interface{}) error {

	d.channel <- msg
	return nil
}

// GetMessageWriterChannel returns the writer (publish) channel
// of the driver
func (d Driver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return d.channel, nil
}

// GetMessageReaderChannel returns the reader (subscribe) channel
// of the driver
func (d Driver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return d.channel, nil
}
