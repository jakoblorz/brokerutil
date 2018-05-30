package loopback

import (
	"github.com/jakoblorz/singapoor/driver"
	"github.com/jakoblorz/singapoor/stream"
)

type Driver struct {
	driverType driver.Type
	channel    chan stream.Message
}

func NewDriver(driverType driver.Type) (*Driver, error) {
	return &Driver{
		driverType: driverType,
		channel:    make(chan stream.Message),
	}, nil
}

func NewSingleThreadDriver() (driver.SingleThreadScaffold, error) {
	return NewDriver(driver.SingleThreadDriver)
}

func NewMultiThreadDriver() (driver.MultiThreadScaffold, error) {
	return NewDriver(driver.MultiThreadDriver)
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

func (d Driver) NotifyMessageRecieve() (stream.Message, error) {
	return <-d.channel, nil
}

func (d Driver) NotifyMessagePublish(msg stream.Message) error {

	d.channel <- msg
	return nil
}

func (d Driver) GetMessageWriterChannel() (chan<- stream.Message, error) {
	return d.channel, nil
}

func (d Driver) GetMessageReaderChannel() (<-chan stream.Message, error) {
	return d.channel, nil
}
