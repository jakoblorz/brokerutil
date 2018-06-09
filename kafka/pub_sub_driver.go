package kafka

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/jakoblorz/brokerutil"
)

// PubSubDriver implements the brokerutil concurrent driver interface
// to allow pub sub functionality over kafka
type PubSubDriver struct {
	signal       chan int
	config       *sarama.Config
	client       *sarama.Client
	topic        string
	transmitChan chan interface{}
	receiveChan  chan interface{}

	m *sync.Mutex
}

// NewKafkaPubSubDriver creates a new kafka pub sub driver
func NewKafkaPubSubDriver(topic string, brokers []string, config *sarama.Config) (*PubSubDriver, error) {

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	return &PubSubDriver{
		signal:       make(chan int),
		config:       config,
		client:       &client,
		topic:        topic,
		transmitChan: make(chan interface{}, 1),
		receiveChan:  make(chan interface{}, 1),
		m:            &sync.Mutex{},
	}, nil
}

func (p *PubSubDriver) getSyncedClient() sarama.Client {

	p.m.Lock()

	client := *p.client

	p.m.Unlock()

	return client
}

// GetDriverFlags returns flags which indicate the capabilites
// and execution plan
func (p *PubSubDriver) GetDriverFlags() []brokerutil.Flag {
	return []brokerutil.Flag{brokerutil.RequiresConcurrentExecution}
}

// OpenStream initializes the communication channels
func (p *PubSubDriver) OpenStream() error {

	// tx routine
	go func() {

		client := p.getSyncedClient()

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			log.Fatalf("PubSubDriver.OpenStream() AsyncProducer error = %v", err)
			return
		}

		defer func() {
			if err := producer.Close(); err != nil {
				log.Fatalf("PubSubDriver.OpenStream() AsyncProducer error = %v", err)
			}
		}()

		go func() {
			for err := range producer.Errors() {
				log.Printf("PubSubDriver.OpenStream() AsyncProducer error = %v", err)
			}
		}()

		for {
			select {
			case <-p.signal:
				return
			case msg := <-p.transmitChan:
				producer.Input() <- &sarama.ProducerMessage{
					Topic: p.topic,
					Value: sarama.StringEncoder(msg.(string)),
				}
			}
		}

	}()

	// rx routine
	go func() {

		client := p.getSyncedClient()

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			log.Fatalf("PubSubDriver.OpenStream() Consumer error = %v", err)
			return
		}

		defer func() {
			if err := consumer.Close(); err != nil {
				log.Fatalf("PubSubDriver.OpenStream() Consumer error = %v", err)
			}
		}()

		partitionConsumer, err := consumer.ConsumePartition(p.topic, 0, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("PubSubDriver.OpenStream() Consumer error = %v", err)
			return
		}

		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalf("PubSubDriver.OpenStream() Consumer error = %v", err)
			}
		}()

		for {
			select {
			case <-p.signal:
				return
			case msg := <-partitionConsumer.Messages():
				p.receiveChan <- string(msg.Value)
			}
		}
	}()

	return nil
}

// CloseStream cleans the communication routines
func (p *PubSubDriver) CloseStream() error {
	p.signal <- 1
	p.signal <- 1
	return nil
}

// GetMessageWriterChannel returns the channel to write to
// to send / publish a message
func (p *PubSubDriver) GetMessageWriterChannel() (chan<- interface{}, error) {
	return p.transmitChan, nil
}

// GetMessageReaderChannel returns the channel to read messages
// from
func (p *PubSubDriver) GetMessageReaderChannel() (<-chan interface{}, error) {
	return p.receiveChan, nil
}
