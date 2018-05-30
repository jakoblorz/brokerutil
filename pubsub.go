// Package singapoor provides a common interface to message-brokers for pub-sub
// applications.
//
// Use singapoor to be able to build pub-sub applications which are not
// highly dependent on the message-brokers driver implementation quirks.
// singapoor provides a common interface which enables developers to switch
// the message broker without having to rewrite major parts of the applications
// pub-sub logic.
package singapoor

import (
	"errors"

	"github.com/jakoblorz/singapoor/driver"
	"github.com/jakoblorz/singapoor/stream"
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
	Publish(stream.Message) error

	// Listen starts the relay goroutine which uses the provided driver to
	// communicate with the message broker.
	Listen() error
}

// NewPubSubFromDriver creates a new PubSub from the provided driver
//
// Depending on the implementation of the driver (single- or multithreaded)
// a different PubSub implementation will be chosen.
func NewPubSubFromDriver(d driver.Scaffold) (PubSub, error) {

	switch d.GetDriverType() {
	case driver.MultiThreadDriver:
		return newMultiThreadPubSubDriverWrapper(d.(driver.MultiThreadScaffold))

	case driver.SingleThreadDriver:
		return newSingleThreadPubSubDriverWrapper(d.(driver.SingleThreadScaffold))
	}

	// driver does not seem to follow required patterns.
	return nil, errors.New("could not match driver architecture to driver wrapper")
}

type multiThreadPubSubDriverWrapper struct {
	driver     driver.MultiThreadScaffold
	controller subscriberController

	terminate chan int
	backlog   chan stream.Message
}

func newMultiThreadPubSubDriverWrapper(d driver.MultiThreadScaffold) (multiThreadPubSubDriverWrapper, error) {
	m := multiThreadPubSubDriverWrapper{
		driver:     d,
		controller: newSubscriberController(),
		terminate:  make(chan int),
		backlog:    make(chan stream.Message),
	}

	return m, nil
}

func (m multiThreadPubSubDriverWrapper) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.controller.SubscribeAsync(fn)
}

func (m multiThreadPubSubDriverWrapper) SubscribeSync(fn SubscriberFunc) error {
	return m.controller.SubscribeSync(fn)
}

func (m multiThreadPubSubDriverWrapper) Unsubscribe(identifier SubscriberIdentifier) {
	m.controller.Unsubscribe(identifier)
}

func (m multiThreadPubSubDriverWrapper) UnsubscribeAll() {
	m.controller.UnsubscribeAll()
}

func (m multiThreadPubSubDriverWrapper) Publish(msg stream.Message) error {
	m.backlog <- msg
	return nil
}

func (m multiThreadPubSubDriverWrapper) Listen() error {

	defer m.controller.UnsubscribeAll()

	if err := m.driver.NotifyStreamOpen(); err != nil {
		return err
	}

	defer m.driver.NotifyStreamClose()

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
				m.controller.NotifySubscribers(msg)
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
	driver     driver.SingleThreadScaffold
	controller subscriberController

	terminate chan int
	backlog   chan stream.Message
}

func newSingleThreadPubSubDriverWrapper(d driver.SingleThreadScaffold) (singleThreadPubSubDriverWrapper, error) {
	m := singleThreadPubSubDriverWrapper{
		driver:     d,
		controller: newSubscriberController(),
		terminate:  make(chan int),
		backlog:    make(chan stream.Message),
	}

	return m, nil
}

func (m singleThreadPubSubDriverWrapper) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.controller.SubscribeAsync(fn)
}

func (m singleThreadPubSubDriverWrapper) SubscribeSync(fn SubscriberFunc) error {
	return m.controller.SubscribeSync(fn)
}

func (m singleThreadPubSubDriverWrapper) Unsubscribe(identifier SubscriberIdentifier) {
	m.controller.Unsubscribe(identifier)
}

func (m singleThreadPubSubDriverWrapper) UnsubscribeAll() {
	m.controller.UnsubscribeAll()
}

func (m singleThreadPubSubDriverWrapper) Publish(msg stream.Message) error {
	m.backlog <- msg
	return nil
}

func (m singleThreadPubSubDriverWrapper) Listen() error {

	defer m.controller.UnsubscribeAll()

	if err := m.driver.NotifyStreamOpen(); err != nil {
		return err
	}

	defer m.driver.NotifyStreamClose()

	for {
		select {

		case <-m.terminate:
			return nil

		case msg := <-m.backlog:
			m.driver.NotifyMessagePublish(msg)

		default:

			avail, err := m.driver.NotifyMessageTest()
			if err != nil {
				return err
			}

			if avail {
				msg, err := m.driver.NotifyMessageRecieve()
				if err != nil {
					return err
				}

				m.controller.NotifySubscribers(msg)
			}
		}
	}
}
