package redis

import (
	"github.com/go-redis/redis"
	"github.com/jakoblorz/singapoor/stream"
)

type RedisDriverOptions struct {
	driver  *redis.Options
	channel string
}

type RedisDriverPublisher struct {
	client  *redis.Client
	channel string
}

func (r RedisDriverPublisher) NotifyOnMessagePublish(msg interface{}) error {

	err := r.client.Publish(r.channel, msg)
	if err != nil {
		return err.Err()
	}

	return nil
}

type RedisDriver struct {
	opts           *RedisDriverOptions
	client         *redis.Client
	subscriberHost *stream.SubscriberHost
	channel        *redis.PubSub
}

func NewRedisDriver(opts *RedisDriverOptions) (*RedisDriver, error) {

	client := redis.NewClient(opts.driver)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &RedisDriver{
		opts:           opts,
		client:         client,
		subscriberHost: stream.NewSubscriberHost(),
	}, nil
}

func (r RedisDriver) Open() error {

	r.channel = r.client.Subscribe(r.opts.channel)

	return nil
}

func (r RedisDriver) Close() error {
	return r.channel.Close()
}

func (r RedisDriver) Publisher() (stream.Publisher, error) {
	return RedisDriverPublisher{
		channel: r.opts.channel,
		client:  r.client,
	}, nil
}
