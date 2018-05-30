package singapoor

import (
	"errors"

	"github.com/jakoblorz/singapoor/driver"
	"github.com/jakoblorz/singapoor/stream"
)

type PubSub interface {
	SubscribeAsync(SubscriberFunc) (chan error, SubscriberIdentifier)
	SubscribeSync(SubscriberFunc) error
	Unsubscribe(SubscriberIdentifier)
	UnsubscribeAll()
	Listen() error
}

func NewPubSubFromDriver(d driver.Scaffold) (PubSub, error) {

	switch d.GetDriverType() {
	case driver.MultiThreadDriver:
		return newMultiThreadPubSubDriverWrapper(d.(driver.MultiThreadScaffold))

	case driver.SingleThreadDriver:
		return newSingleThreadPubSubDriverWrapper(d.(driver.SingleThreadScaffold))
	}

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
