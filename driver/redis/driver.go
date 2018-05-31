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

func (d Driver) GetDriverType() driver.PubSubDriverType {
	return driver.SingleThreadPubSubDriver
}

func (d Driver) NotifyStreamClose() error {
	return d.channel.Close()
}

func (d Driver) NotifyStreamOpen() error {

	d.channel = d.client.Subscribe(d.opts.channel)

	return nil
}

func (d Driver) NotifyMessageTest() (bool, error) {
	return true, nil
}

func (d Driver) NotifyMessageRecieve() (interface{}, error) {

	msg, err := d.channel.ReceiveMessage()
	if err != nil {
		return nil, err
	}

	return interface{}(msg.Payload), nil
}

func (d Driver) NotifyMessagePublish(msg interface{}) error {

	err := d.client.Publish(d.opts.channel, msg)
	if err != nil {
		return err.Err()
	}

	return nil
}
