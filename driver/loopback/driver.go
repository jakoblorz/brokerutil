package loopback

import "github.com/jakoblorz/singapoor/stream"

type Driver struct {
	channel chan interface{}
}

func NewDriver() (*Driver, error) {
	return &Driver{
		channel: make(chan interface{}),
	}, nil
}

func (d Driver) Open() error {
	return nil
}

func (d Driver) Close() error {
	return nil
}

func (d Driver) Publisher() (stream.Publisher, error) {
	return Publisher{
		channel: d.channel,
	}, nil
}

func (d Driver) Subscriber() (stream.Subscriber, error) {
	return Subscriber{
		channel: d.channel,
	}, nil
}
