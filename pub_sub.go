package brokerutil

import (
	"errors"
)

var (

	// ErrConcurrentDriverCast is the error thrown when a cast to a concurrent driver
	// failed
	ErrConcurrentDriverCast = errors.New("could not cast driver to concurrency driver")

	// ErrBlockingDriverCast is the error thrown when a cast to a blocking driver
	// failed
	ErrBlockingDriverCast = errors.New("could not cast driver to blocking driver")

	// ErrMissingExecutionFlag is the error thrown when the GetDriverFlags() function
	// returned an array / slice missing a execution flag
	ErrMissingExecutionFlag = errors.New("could not find execution flag")
)

type pubSuber interface {
	SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier)
	SubscribeSync(fn SubscriberFunc) error
	Unsubscribe(identifier SubscriberIdentifier)
	UnsubscribeAll()
	Publish(msg interface{}) error
	ListenAsync() chan error
	ListenSync() error
	Terminate() error
}

// PubSub is the common "gateway" to reach to interact with the message broker such
// as Publish / Subscribe. Independently from the implementation of the driver, it
// guarantees that the exposed functions will work as expected.
type PubSub struct {
	driver              PubSubDriver
	scheduler           scheduler
	supportsConcurrency bool

	terminate chan int
	backlog   chan interface{}
}

// NewPubSubFromDriver creates a new PubSub from the provided driver
//
// Depending on the implementation of the driver (single- or multithreaded)
// a different PubSub implementation will be chosen.
func NewPubSubFromDriver(d PubSubDriver) (*PubSub, error) {

	var supportsConcurrency bool

	if containsFlag(d.GetDriverFlags(), ConcurrentExecution) {
		_, ok := d.(ConcurrentPubSubDriver)

		if !ok {
			return nil, ErrConcurrentDriverCast
		}

		supportsConcurrency = true
	} else if containsFlag(d.GetDriverFlags(), BlockingExecution) {
		_, ok := d.(BlockingPubSubDriver)

		if !ok {
			return nil, ErrBlockingDriverCast
		}

		supportsConcurrency = false
	} else {
		return nil, ErrMissingExecutionFlag
	}

	return &PubSub{
		driver:              d,
		scheduler:           newScheduler(),
		supportsConcurrency: supportsConcurrency,
		backlog:             make(chan interface{}),
		terminate:           make(chan int),
	}, nil
}

// NewPubSubFromDrivers creates a new PubSub from the provided drivers
//
// Only the first driver is used to publish messages, for further functionality
// use DriverAwarePubSub
func NewPubSubFromDrivers(drivers ...PubSubDriver) (*PubSub, error) {

	driverOptions := syntheticDriverOptions{
		UseSyntheticMessageWithSource: false,
		UseSyntheticMessageWithTarget: false,
	}

	driverPtr, err := newSyntheticDriver(&driverOptions, drivers...)
	if err != nil {
		return nil, err
	}

	return NewPubSubFromDriver(driverPtr)
}

// SubscribeAsync creates a new callback function which is invoked
// on any incomming messages.
//
// It returns a error chan which will contain
// all occuring / returned errors of the SubscriberFunc. A nil error
// indicates the auto-unsubscribe after the call of UnsubscribeAll().
// Use the SubscriberIdentifier to Unsubscribe later.
func (a *PubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return a.scheduler.SubscribeAsync(fn)
}

// SubscribeSync creates a new callback function like SubscribeAsync().
//
// It will block until recieving error or nil in the error chan, then returns it.
func (a *PubSub) SubscribeSync(fn SubscriberFunc) error {
	return a.scheduler.SubscribeSync(fn)
}

// Unsubscribe removes a previously added callback function from the invokation
// loop.
//
// Use the SubscriberIdentifier created when calling SubscribeAsync(). It will
// send a nil error in the callback function's error chan.
func (a *PubSub) Unsubscribe(identifier SubscriberIdentifier) {
	a.scheduler.Unsubscribe(identifier)
}

// UnsubscribeAll removes all added callback functions from the invokation
// loop.
//
// It will send a nil error in the callback's function's error chans.
func (a *PubSub) UnsubscribeAll() {
	a.scheduler.UnsubscribeAll()
}

// Publish sends a message to the message broker.
func (a *PubSub) Publish(msg interface{}) error {
	a.backlog <- msg
	return nil
}

// ListenAsync starts the relay goroutine which uses the provided driver
// to communicate with the message broker.
func (a *PubSub) ListenAsync() chan error {

	errCh := make(chan error, 1)

	go func() {
		errCh <- a.ListenSync()
	}()

	return errCh
}

// ListenSync starts relay loops which use the provided driver to
// communicate with the message broker.
func (a *PubSub) ListenSync() error {

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

		d, ok := a.driver.(ConcurrentPubSubDriver)

		if !ok {
			return ErrConcurrentDriverCast
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

	d, ok := a.driver.(BlockingPubSubDriver)

	if !ok {
		return ErrBlockingDriverCast
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
			msg, err := d.ReceiveMessage()
			if err != nil {
				return err
			}

			err = a.scheduler.NotifySubscribers(msg)
			if err != nil {
				return err
			}
		}
	}
}

// Terminate send a termination signal so that the blocking Listen will
// be released.
//
// Subscribers will be unsubscribed was well.
func (a *PubSub) Terminate() error {

	// send single termination signal for
	// blocking driver
	var signalCount = 1

	// send two termination signal for concurrent
	// driver
	if a.supportsConcurrency {
		signalCount = 2
	}

	for i := 0; i < signalCount; i++ {
		a.terminate <- 1
	}

	return nil
}
