package loopback

import "github.com/jakoblorz/singapoor/stream"

type Subscriber struct {
	channel chan interface{}
	manager *stream.SubscriberManager
}

func (s Subscriber) GetMessageChannel() (<-chan interface{}, error) {
	return s.channel, nil
}

func (s Subscriber) AddSubscriber(fn stream.SubscriberFunc) chan error {
	return s.manager.AddSubscriber(fn)
}

func (s Subscriber) NotifyOnMessageRecieve(msg interface{}) error {
	return s.manager.NotifyOnMessageRecieve(msg)
}

func (s Subscriber) NotifyOnStreamClose() error {
	return s.manager.NotifyOnStreamClose()
}
