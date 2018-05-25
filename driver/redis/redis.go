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

type RedisDriverSubscriber struct {
	channel        *redis.PubSub
	subscriberHost *stream.SubscriberHost
}

func (r RedisDriverSubscriber) AddSubscriber(fn func(interface{}) error) chan error {
	return r.subscriberHost.AddSubscriber(fn)
}

func (r RedisDriverSubscriber) NotifyOnMessageRecieve(msg interface{}) error {
	return r.subscriberHost.NotifyOnMessageRecieve(msg)
}

func (r RedisDriverSubscriber) NotifyOnStreamClose() error {
	return r.subscriberHost.NotifyOnStreamClose()
}

func (r RedisDriverSubscriber) GetMessageChannel() (<-chan interface{}, error) {

	redisMsgChannel := r.channel.Channel()
	var channel = make(chan interface{})

	go func() {

		var message *redis.Message

		for {
			message = <-redisMsgChannel
			channel <- message.Payload
		}
	}()

	return channel, nil
}

type RedisDriver struct {
	opts    *RedisDriverOptions
	client  *redis.Client
	channel *redis.PubSub
}

func NewRedisDriver(opts *RedisDriverOptions) (*RedisDriver, error) {

	client := redis.NewClient(opts.driver)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &RedisDriver{
		opts:   opts,
		client: client,
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

func (r RedisDriver) Subscriber() (stream.Subscriber, error) {
	return RedisDriverSubscriber{
		channel:        r.channel,
		subscriberHost: stream.NewSubscriberHost(),
	}, nil
}
