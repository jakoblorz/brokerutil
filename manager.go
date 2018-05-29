package singapoor

import (
	"errors"

	"github.com/jakoblorz/singapoor/driver"
	"github.com/jakoblorz/singapoor/stream"
)

type StreamManager interface {
	SubscribeAsync(SubscriberFunc) (chan error, SubscriberIdentifier)
	SubscribeSync(SubscriberFunc) error
	Unsubscribe(SubscriberIdentifier)
	UnsubscribeAll()
	Listen() error
}

func NewStreamManager(d driver.Scaffold) (StreamManager, error) {

	switch d.GetDriverType() {
	case driver.MultithreadDriver:
		return newMultiThreadStreamManager(d.(driver.MultiThreadScaffold))

	case driver.SinglethreadDriver:
		return newSingleThreadStreamManager(d.(driver.SingleThreadScaffold))
	}

	return nil, errors.New("could not match driver architecture to driver wrapper")
}

type multiThreadStreamManager struct {
	driver     driver.MultiThreadScaffold
	controller subscriberController

	terminate chan int
	backlog   chan stream.Message
}

func newMultiThreadStreamManager(d driver.MultiThreadScaffold) (multiThreadStreamManager, error) {
	m := multiThreadStreamManager{
		driver:     d,
		controller: newSubscriberController(),
		terminate:  make(chan int),
		backlog:    make(chan stream.Message),
	}

	return m, nil
}

func (m multiThreadStreamManager) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.controller.SubscribeAsync(fn)
}

func (m multiThreadStreamManager) SubscribeSync(fn SubscriberFunc) error {
	return m.controller.SubscribeSync(fn)
}

func (m multiThreadStreamManager) Unsubscribe(identifier SubscriberIdentifier) {
	m.controller.Unsubscribe(identifier)
}

func (m multiThreadStreamManager) UnsubscribeAll() {
	m.controller.UnsubscribeAll()
}

func (m multiThreadStreamManager) Listen() error {

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

type singleThreadStreamManager struct {
	driver     driver.SingleThreadScaffold
	controller subscriberController

	terminate chan int
	backlog   chan stream.Message
}

func newSingleThreadStreamManager(d driver.SingleThreadScaffold) (singleThreadStreamManager, error) {
	m := singleThreadStreamManager{
		driver:     d,
		controller: newSubscriberController(),
		terminate:  make(chan int),
		backlog:    make(chan stream.Message),
	}

	return m, nil
}

func (m singleThreadStreamManager) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return m.controller.SubscribeAsync(fn)
}

func (m singleThreadStreamManager) SubscribeSync(fn SubscriberFunc) error {
	return m.controller.SubscribeSync(fn)
}

func (m singleThreadStreamManager) Unsubscribe(identifier SubscriberIdentifier) {
	m.controller.Unsubscribe(identifier)
}

func (m singleThreadStreamManager) UnsubscribeAll() {
	m.controller.UnsubscribeAll()
}

func (m singleThreadStreamManager) Listen() error {

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
