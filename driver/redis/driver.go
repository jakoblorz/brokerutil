package redis

import (
	"github.com/go-redis/redis"
	"github.com/jakoblorz/brokerutil/driver"
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

func (d Driver) GetDriverFlags() driver.Flag {
	return driver.BlocksConcurrency
}

func (d Driver) CloseStream() error {
	return d.channel.Close()
}

func (d Driver) OpenStream() error {

	d.channel = d.client.Subscribe(d.opts.channel)

	return nil
}

func (d Driver) CheckForPendingMessage() (bool, error) {
	return true, nil
}

func (d Driver) ReceivePendingMessage() (interface{}, error) {

	msg, err := d.channel.ReceiveMessage()
	if err != nil {
		return nil, err
	}

	return interface{}(msg.Payload), nil
}

func (d Driver) PublishMessage(msg interface{}) error {

	err := d.client.Publish(d.opts.channel, msg)
	if err != nil {
		return err.Err()
	}

	return nil
}
