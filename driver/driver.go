package driver

import "github.com/jakoblorz/singapoor/pubsub"

type Implementation interface {
	Opener
	Closer
	Publisher
	Subscriber
}

type Opener interface {
	Open() error
}

type Closer interface {
	Close() error
}

type Publisher interface {
	Publisher() (pubsub.Publisher, error)
}

type Subscriber interface {
	Subscriber() (pubsub.Subscriber, error)
}
