package loopback

import (
	"github.com/jakoblorz/brokerutil/driver"
)

type Driver struct {
	driverType driver.Type
	channel    chan interface{}
}

func NewDriver(driverType driver.Type) (*Driver, error) {
	return &Driver{
		driverType: driverType,
		channel:    make(chan interface{}),
	}, nil
}

func NewSingleThreadDriver() (driver.SingleThreadPubSubDriverScaffold, error) {
	return NewDriver(driver.SingleThreadPubSubDriver)
}

func NewMultiThreadDriver() (driver.MultiThreadPubSubDriverScaffold, error) {
	return NewDriver(driver.MultiThreadPubSubDriver)
}

func (d Driver) GetDriverType() driver.Type {
	return d.driverType
}

func (d Driver) NotifyStreamClose() error {
	return nil
}

func (d Driver) NotifyStreamOpen() error {
	return nil
}

func (d Driver) NotifyMessageTest() (bool, error) {
	return true, nil
}

func (d Driver) NotifyMessageRecieve() (interface{}, error) {
	return <-d.channel, nil
}

func (d Driver) NotifyMessagePublish(msg interface{}) error {

	d.channel <- msg
	return nil
}

func (d Driver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return d.channel, nil
}

func (d Driver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return d.channel, nil
}
