package singapoor

import (
	"github.com/jakoblorz/singapoor/driver"
)

// Manager manages a drivers stream events
type Manager struct {
	driver  driver.Implementation
	sigTerm chan int
	backlog chan interface{}
}

// NewManager creates a new singapoor stream manager based on the given
// driver implementation
func NewManager(d driver.Implementation) (*Manager, error) {

	m := &Manager{
		driver: d,

		sigTerm: make(chan int),
		backlog: make(chan interface{}),
	}

	return m, nil
}

// Open initializes the driver so that the driver can instantiate
// message channels and connections
func (m *Manager) Open() error {
	return m.driver.Open()
}

// Run starts a for { } loop which merges all stream actions such as
// publishing, notifying subscribers etc
func (m *Manager) Run() error {

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
func (m *Manager) Close() error {
	m.sigTerm <- 1

	return m.driver.Close()
}

// Publish adds a new message to the backlog so that the node can
// communicate that message to the others using the drivers
// interface
func (m *Manager) Publish(i interface{}) {
	m.backlog <- i
}

// Subscribe adds a subscriber so that a specific function can
// be invoked once a message was recieved
func (m *Manager) Subscribe(fn func(interface{}) error) chan error {

	errChan := make(chan error)

	sub, err := m.driver.Subscriber()
	if err != nil {
		errChan <- err
		return errChan
	}

	return sub.AddSubscriber(fn)
}

// BlockingSubscribe is the same as Subscribe only differing in the
// return value: it returns an error instead of an error channel so
// that this method waits for a error thus being blocking
func (m *Manager) BlockingSubscribe(fn func(interface{}) error) error {
	channel := m.Subscribe(fn)
	return <-channel
}

func (m *Manager) Driver() driver.Implementation {
	return m.driver
}
