package redis

import "github.com/go-redis/redis"

type Publisher struct {
	client  *redis.Client
	channel string
}

func NewPublisher(client *redis.Client, channel string) Publisher {
	return Publisher{
		client:  client,
		channel: channel,
	}
}

func (p Publisher) NotifyOnMessagePublish(msg interface{}) error {

	err := p.client.Publish(p.channel, msg)
	if err != nil {
		return err.Err()
	}

	return nil
}
