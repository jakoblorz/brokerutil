package driver

import "github.com/jakoblorz/singapoor/stream"

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
	Publisher() (stream.Publisher, error)
}

type Subscriber interface {
	Subscriber() (stream.Subscriber, error)
}
