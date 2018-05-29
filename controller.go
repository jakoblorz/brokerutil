package singapoor

import (
	"github.com/jakoblorz/singapoor/driver"
	"github.com/jakoblorz/singapoor/stream"
)

// Controller manages a drivers stream events
type Controller struct {
	driver  driver.Implementation
	sigTerm chan int
	backlog chan interface{}
}

// NewController creates a new singapoor stream manager based on the given
// driver implementation
func NewController(d driver.Implementation) (*Controller, error) {

	m := &Controller{
		driver: d,

		sigTerm: make(chan int),
		backlog: make(chan interface{}),
	}

	return m, nil
}

// Open initializes the driver so that the driver can instantiate
// message channels and connections
func (m *Controller) Open() error {
	return m.driver.Open()
}

// Run starts a for { } loop which merges all stream actions such as
// publishing, notifying subscribers etc
func (m *Controller) Run() error {

	subscriber, err := m.driver.Subscriber()
	if err != nil {
		return err
	}

	stream, err := subscriber.GetMessageChannel()
	if err != nil {
		return err
	}

	publisher, err := m.driver.Publisher()
	if err != nil {
		return err
	}

	for {

		select {

		case msg := <-m.backlog:
			err := publisher.NotifyOnMessagePublish(msg)

			if err != nil {
				return err
			}

		case msg := <-stream:
			err := subscriber.NotifyOnMessageRecieve(msg)

			if err != nil {
				return err
			}

		case <-m.sigTerm:
			err := subscriber.NotifyOnStreamClose()

			if err != nil {
				return err
			}

			return nil
		}
	}
}

// Close signals the Run() loop to stop and closed the drivers
// interface
func (m *Controller) Close() error {
	m.sigTerm <- 1

	return m.driver.Close()
}

// Publish adds a new message to the backlog so that the node can
// communicate that message to the others using the drivers
// interface
func (m *Controller) Publish(i interface{}) {
	m.backlog <- i
}

// Subscribe adds a subscriber so that a specific function can
// be invoked once a message was recieved
func (m *Controller) Subscribe(fn stream.SubscriberFunc) chan error {

	errChan := make(chan error)

	sub, err := m.driver.Subscriber()
	if err != nil {
		errChan <- err
		return errChan
	}

	return sub.Subscribe(fn)
}

// BlockingSubscribe is the same as Subscribe only differing in the
// return value: it returns an error instead of an error channel so
// that this method waits for a error thus being blocking
func (m *Controller) BlockingSubscribe(fn stream.SubscriberFunc) error {
	channel := m.Subscribe(fn)
	return <-channel
}

func (m *Controller) Driver() driver.Implementation {
	return m.driver
}
