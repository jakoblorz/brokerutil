package redis

import (
	"github.com/go-redis/redis"
	"github.com/jakoblorz/singapoor/stream"
)

type Driver struct {
	opts    *DriverOptions
	client  *redis.Client
	channel *redis.PubSub
}

func NewDriver(opts *DriverOptions) (*Driver, error) {

	client := redis.NewClient(opts.driver)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Driver{
		opts:    opts,
		client:  client,
		channel: nil,
	}, nil
}

func (d Driver) Open() error {

	d.channel = d.client.Subscribe(d.opts.channel)

	return nil
}

func (d Driver) Close() error {
	return d.channel.Close()
}

func (d Driver) Publisher() (stream.Publisher, error) {
	return Publisher{
		channel: d.opts.channel,
		client:  d.client,
	}, nil
}

func (d Driver) Subscriber() (stream.Subscriber, error) {
	return Subscriber{
		channel: d.channel,
		manager: stream.NewSubscriberHost(),
	}, nil
}
