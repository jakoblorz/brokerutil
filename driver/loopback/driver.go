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

// CloseStream can be called to close the stream
func (d Driver) CloseStream() error {
	return nil
}

// OpenStream can be called to open the stream
func (d Driver) OpenStream() error {
	return nil
}

// CheckForPendingMessage can be called to test if a new message
// is pending
func (d Driver) CheckForPendingMessage() (bool, error) {
	return true, nil
}

// ReceivePendingMessage can be called to recieve a message
func (d Driver) ReceivePendingMessage() (interface{}, error) {
	return <-d.channel, nil
}

// PublishMessage can be called to publish a message
func (d Driver) PublishMessage(msg interface{}) error {

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
