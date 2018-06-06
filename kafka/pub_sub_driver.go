package kafka

import (
	"github.com/jakoblorz/brokerutil"
)

// PubSubDriver implements the brokerutil concurrent driver interface
// to allow pub sub functionality over kafka
type PubSubDriver struct {
	transmitChan chan interface{}
	receiveChan  chan interface{}
}

// NewKafkaPubSubDriver creates a new kafka pub sub driver
func NewKafkaPubSubDriver() (*PubSubDriver, error) {
	return &PubSubDriver{
		transmitChan: make(chan interface{}, 1),
		receiveChan:  make(chan interface{}, 1),
	}, nil
}

// GetDriverFlags returns flags which indicate the capabilites
// and execution plan
func (p PubSubDriver) GetDriverFlags() []brokerutil.Flag {
	return []brokerutil.Flag{brokerutil.RequiresConcurrentExecution}
}

// OpenStream initializes the communication channels
func (p PubSubDriver) OpenStream() error {
	return nil
}

// CloseStream cleans the communication routines
func (p PubSubDriver) CloseStream() error {
	return nil
}

// GetMessageWriterChannel returns the channel to write to
// to send / publish a message
func (p PubSubDriver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return p.transmitChan, nil
}

// GetMessageReaderChannel returns the channel to read messages
// from
func (p PubSubDriver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return p.receiveChan, nil
}
