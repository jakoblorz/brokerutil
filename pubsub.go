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

	// Terminate send a termination signal so that the blocking Listen will
	// be released.
	//
	// Subscribers will be unsubscribed was well.
	Terminate() error
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

type architectureAwarePubSub struct {
	driver              driver.PubSubDriverScaffold
	scheduler           scheduler
	supportsConcurrency bool

	terminate chan int
	backlog   chan interface{}
}

func (a architectureAwarePubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return a.scheduler.SubscribeAsync(fn)
}

func (a architectureAwarePubSub) SubscribeSync(fn SubscriberFunc) error {
	return a.scheduler.SubscribeSync(fn)
}

func (a architectureAwarePubSub) Unsubscribe(identifier SubscriberIdentifier) {
	a.scheduler.Unsubscribe(identifier)
}

func (a architectureAwarePubSub) UnsubscribeAll() {
	a.scheduler.UnsubscribeAll()
}

func (a architectureAwarePubSub) Publish(msg interface{}) error {
	a.backlog <- msg
	return nil
}

func (a architectureAwarePubSub) ListenAsync() chan error {

	errCh := make(chan error, 1)

	go func() {
		errCh <- a.Listen()
	}()

	return errCh
}

func (a architectureAwarePubSub) Listen() error {

	defer a.scheduler.UnsubscribeAll()

	if err := a.driver.OpenStream(); err != nil {
		return err
	}

	defer a.driver.CloseStream()

	//
	// relay code for driver which supports concurrency
	// such as recieving and writing from different goroutines
	//
	if a.supportsConcurrency {

		d, ok := a.driver.(driver.MultiThreadPubSubDriverScaffold)

		if !ok {
			return errors.New("driver does not support concurrency, could not cast to correct interface")
		}

		tx, err := d.GetMessageWriterChannel()
		if err != nil {
			return err
		}

		rx, err := d.GetMessageReaderChannel()
		if err != nil {
			return err
		}

		go func() {

			for {
				select {
				case <-a.terminate:
					return
				case msg := <-a.backlog:
					tx <- msg
				}
			}
		}()

		for {
			select {
			case <-a.terminate:
				return nil
			case msg := <-rx:
				err := a.scheduler.NotifySubscribers(msg)
				if err != nil {
					return err
				}
			}
		}
	}

	//
	// relay code for driver which does not support concurrency
	// such as recieving and writing from different goroutines
	//

	d, ok := a.driver.(driver.SingleThreadPubSubDriverScaffold)

	if !ok {
		return errors.New("driver does support concurrency but not with the pub-sub implementation, could not cast to correct interface")
	}

	for {
		select {
		case <-a.terminate:
			return nil
		case msg := <-a.backlog:
			err := d.PublishMessage(msg)
			if err != nil {
				return err
			}

		default:

			avail, err := d.CheckForPendingMessage()
			if err != nil {
				return err
			}

			if avail {
				msg, err := d.ReceivePendingMessage()
				if err != nil {
					return err
				}

				a.scheduler.NotifySubscribers(msg)
			}
		}
	}
}

func (a architectureAwarePubSub) Terminate() error {
	a.terminate <- 1
	return nil
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

func (m multiThreadPubSubDriverWrapper) Terminate() error {
	m.terminate <- 1
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

func (s singleThreadPubSubDriverWrapper) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return s.scheduler.SubscribeAsync(fn)
}

func (s singleThreadPubSubDriverWrapper) SubscribeSync(fn SubscriberFunc) error {
	return s.scheduler.SubscribeSync(fn)
}

func (s singleThreadPubSubDriverWrapper) Unsubscribe(identifier SubscriberIdentifier) {
	s.scheduler.Unsubscribe(identifier)
}

func (s singleThreadPubSubDriverWrapper) UnsubscribeAll() {
	s.scheduler.UnsubscribeAll()
}

func (s singleThreadPubSubDriverWrapper) Publish(msg interface{}) error {
	s.backlog <- msg
	return nil
}

func (s singleThreadPubSubDriverWrapper) Listen() error {

	defer s.scheduler.UnsubscribeAll()

	if err := s.driver.OpenStream(); err != nil {
		return err
	}

	defer s.driver.CloseStream()

	for {
		select {

		case <-s.terminate:
			return nil

		case msg := <-s.backlog:
			s.driver.PublishMessage(msg)

		default:

			avail, err := s.driver.CheckForPendingMessage()
			if err != nil {
				return err
			}

			if avail {
				msg, err := s.driver.ReceivePendingMessage()
				if err != nil {
					return err
				}

				s.scheduler.NotifySubscribers(msg)
			}
		}
	}
}

func (s singleThreadPubSubDriverWrapper) Terminate() error {
	s.terminate <- 1
	return nil
}
