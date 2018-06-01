package brokerutil

import (
	"errors"

	"github.com/jakoblorz/brokerutil/driver"
)

// PubSub is the common interface for any kind of Publish / Subscribe
// actions. Independently from the implementation of the driver, it guarantees that
// the exposed functions will work as expected.
type PubSub interface {

	// SubscribeAsync creates a new callback function which is invoked
	// on any incomming messages.
	//
	// It returns a error chan which will contain
	// all occuring / returned errors of the SubscriberFunc. A nil error
	// indicates the auto-unsubscribe after the call of UnsubscribeAll().
	// Use the SubscriberIdentifier to Unsubscribe later.
	SubscribeAsync(SubscriberFunc) (chan error, SubscriberIdentifier)

	// SubscribeSync creates a new callback function like SubscribeAsync().
	//
	// It will block until recieving error or nil in the error chan, then returns it.
	SubscribeSync(SubscriberFunc) error

	// Unsubscribe removes a previously added callback function from the invokation
	// loop.
	//
	// Use the SubscriberIdentifier created when calling SubscribeAsync(). It will
	// send a nil error in the callback function's error chan.
	Unsubscribe(SubscriberIdentifier)

	// UnsubscribeAll removes all added callback functions from the invokation
	// loop.
	//
	// It will send a nil error in the callback's function's error chans.
	UnsubscribeAll()

	// Publish sends a message to the message broker.
	Publish(interface{}) error

	// Listen starts the relay goroutine which uses the provided driver to
	// communicate with the message broker.
	Listen() error
}

// NewPubSubFromDriver creates a new PubSub from the provided driver
//
// Depending on the implementation of the driver (single- or multithreaded)
// a different PubSub implementation will be chosen.
func NewPubSubFromDriver(d driver.PubSubDriverScaffold) (PubSub, error) {

	switch d.GetDriverType() {
	case driver.MultiThreadPubSubDriver:
		return newMultiThreadPubSubDriverWrapper(d.(driver.MultiThreadPubSubDriverScaffold))

	case driver.SingleThreadPubSubDriver:
		return newSingleThreadPubSubDriverWrapper(d.(driver.SingleThreadPubSubDriverScaffold))
	}

	// driver does not seem to follow required patterns.
	return nil, errors.New("could not match driver architecture to driver wrapper")
}

// NewPubSubFromMultiThreadDriver creates a new PubSub from the provided multi-thread
// pub sub driver
func NewPubSubFromMultiThreadDriver(d driver.MultiThreadPubSubDriverScaffold) (PubSub, error) {
	return newMultiThreadPubSubDriverWrapper(d)
}

// NewPubSubFromSingleThreadDriver creates a new PubSub from the provided single-thread
// pub sub driver
func NewPubSubFromSingleThreadDriver(d driver.SingleThreadPubSubDriverScaffold) (PubSub, error) {
	return newSingleThreadPubSubDriverWrapper(d)
}

type multiThreadPubSubDriverWrapper struct {
	driver    driver.MultiThreadPubSubDriverScaffold
	scheduler scheduler

	terminate chan int
	backlog   chan interface{}
}

func newMultiThreadPubSubDriverWrapper(d driver.MultiThreadPubSubDriverScaffold) (multiThreadPubSubDriverWrapper, error) {
	m := multiThreadPubSubDriverWrapper{
		driver:    d,
		scheduler: newScheduler(),
		terminate: make(chan int),
		backlog:   make(chan interface{}),
	}

	return m, nil
}

func (m multiThreadPubSubDriverWrapper) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.scheduler.SubscribeAsync(fn)
}

func (m multiThreadPubSubDriverWrapper) SubscribeSync(fn SubscriberFunc) error {
	return m.scheduler.SubscribeSync(fn)
}

func (m multiThreadPubSubDriverWrapper) Unsubscribe(identifier SubscriberIdentifier) {
	m.scheduler.Unsubscribe(identifier)
}

func (m multiThreadPubSubDriverWrapper) UnsubscribeAll() {
	m.scheduler.UnsubscribeAll()
}

func (m multiThreadPubSubDriverWrapper) Publish(msg interface{}) error {
	m.backlog <- msg
	return nil
}

func (m multiThreadPubSubDriverWrapper) Listen() error {

	defer m.scheduler.UnsubscribeAll()

	if err := m.driver.OpenStream(); err != nil {
		return err
	}

	defer m.driver.CloseStream()

	inbound, err := m.driver.GetMessageReaderChannel()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-m.terminate:
				return
			case msg := <-inbound:
				m.scheduler.NotifySubscribers(msg)
			}
		}
	}()

	outbound, err := m.driver.GetMessageWriterChannel()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-m.terminate:
				return
			case msg := <-m.backlog:
				outbound <- msg
			}
		}
	}()

	<-m.terminate

	return nil
}

type singleThreadPubSubDriverWrapper struct {
	driver    driver.SingleThreadPubSubDriverScaffold
	scheduler scheduler

	terminate chan int
	backlog   chan interface{}
}

func newSingleThreadPubSubDriverWrapper(d driver.SingleThreadPubSubDriverScaffold) (singleThreadPubSubDriverWrapper, error) {
	m := singleThreadPubSubDriverWrapper{
		driver:    d,
		scheduler: newScheduler(),
		terminate: make(chan int),
		backlog:   make(chan interface{}),
	}

	return m, nil
}

func (m singleThreadPubSubDriverWrapper) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.scheduler.SubscribeAsync(fn)
}

func (m singleThreadPubSubDriverWrapper) SubscribeSync(fn SubscriberFunc) error {
	return m.scheduler.SubscribeSync(fn)
}

func (m singleThreadPubSubDriverWrapper) Unsubscribe(identifier SubscriberIdentifier) {
	m.scheduler.Unsubscribe(identifier)
}

func (m singleThreadPubSubDriverWrapper) UnsubscribeAll() {
	m.scheduler.UnsubscribeAll()
}

func (m singleThreadPubSubDriverWrapper) Publish(msg interface{}) error {
	m.backlog <- msg
	return nil
}

func (m singleThreadPubSubDriverWrapper) Listen() error {

	defer m.scheduler.UnsubscribeAll()

	if err := m.driver.OpenStream(); err != nil {
		return err
	}

	defer m.driver.CloseStream()

	for {
		select {

		case <-m.terminate:
			return nil

		case msg := <-m.backlog:
			m.driver.PublishMessage(msg)

		default:

			avail, err := m.driver.CheckForPendingMessage()
			if err != nil {
				return err
			}

			if avail {
				msg, err := m.driver.RecievePendingMessage()
				if err != nil {
					return err
				}

				m.scheduler.NotifySubscribers(msg)
			}
		}
	}
}
