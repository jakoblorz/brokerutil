package redis

import (
	"github.com/go-redis/redis"
	"github.com/jakoblorz/brokerutil"
)

// PubSubDriver implements the brokerutil concurrent driver interface
// to allow pub sub functionality over redis
type PubSubDriver struct {
	client       *redis.Client
	channelNames []string
	transmitCh   chan interface{}
	receiveCh    chan interface{}
	signal       chan int
}

// NewRedisPubSubDriver creates a new redis pub sub driver
func NewRedisPubSubDriver(channels []string, opts *redis.Options) (*PubSubDriver, error) {

	var channelNames []string
	if len(channels) == 0 {
		channelNames = []string{""}
	} else {
		channelNames = channels
	}

	client := redis.NewClient(opts)

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &PubSubDriver{
		client:       client,
		channelNames: channelNames,
		signal:       make(chan int), // sync signal chan
		transmitCh:   make(chan interface{}, 1),
		receiveCh:    make(chan interface{}, 1),
	}, nil
}

// GetDriverFlags returns flags which indicate the capabilities
func (p PubSubDriver) GetDriverFlags() []brokerutil.Flag {
	return []brokerutil.Flag{brokerutil.ConcurrentExecution}
}

// OpenStream initializes the communication channels protocol + network
func (p PubSubDriver) OpenStream() error {

	channel := p.client.Subscribe(p.channelNames...)

	receiveCh := make(chan interface{}, 1)
	transmitCh := make(chan interface{}, 1)

	// channel communitions merger
	go func() {
		for {
			select {
			case <-p.signal:
				channel.Close()
				return
			case msg := <-p.transmitCh:
				transmitCh <- msg
			case msg := <-receiveCh:
				p.receiveCh <- msg
			}
		}
	}()

	// rx go routine
	go func() {
		for {
			select {
			case <-p.signal:
				return
			default:
				msg, err := channel.ReceiveMessage()
				if err == nil {
					receiveCh <- msg.Payload
				}
			}
		}
	}()

	// tx go routine
	go func() {
		for {
			select {
			case <-p.signal:
				return
			case msg := <-transmitCh:
				p.client.Publish(p.channelNames[0], msg)
			}
		}
	}()

	return nil
}

// CloseStream cleans the communication routines up
func (p PubSubDriver) CloseStream() error {
	p.signal <- 1
	p.signal <- 1
	p.signal <- 1

	return nil
}

// GetMessageWriterChannel returns the channel to write to
// if a message needs to be sent / published
func (p PubSubDriver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return p.transmitCh, nil
}

// GetMessageReaderChannel returns the channel to read from
// if a message was received
func (p PubSubDriver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return p.receiveCh, nil
}
