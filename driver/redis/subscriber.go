package redis

import (
	"github.com/go-redis/redis"
	"github.com/jakoblorz/singapoor/stream"
)

type Subscriber struct {
	channel *redis.PubSub
	manager *stream.SubscriberManager
}

func NewSubscriber(channel *redis.PubSub) Subscriber {
	return Subscriber{
		channel: channel,
		manager: stream.NewSubscriberManager(),
	}
}

func (s Subscriber) GetMessageChannel() (<-chan interface{}, error) {

	channelIncoming := s.channel.Channel()
	channelOutgoing := make(chan interface{})

	go func() {

		var message *redis.Message

		for {
			message = <-channelIncoming
			channelOutgoing <- message.Payload
		}
	}()

	return channelOutgoing, nil
}

func (s Subscriber) Subscribe(fn stream.SubscriberFunc) chan error {
	return s.manager.Subscribe(fn)
}

func (s Subscriber) NotifyOnMessageRecieve(msg interface{}) error {
	return s.manager.NotifyOnMessageRecieve(msg)
}

func (s Subscriber) NotifyOnStreamClose() error {
	return s.manager.NotifyOnStreamClose()
}
