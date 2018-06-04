package brokerutil

import (
	"errors"
)

// PubSub is the common "gateway" to reach to interact with the message broker such
// as Publish / Subscribe. Independently from the implementation of the driver, it
// guarantees that the exposed functions will work as expected.
type PubSub struct {
	driver              PubSubDriverScaffold
	scheduler           scheduler
	supportsConcurrency bool

	terminate chan int
	backlog   chan interface{}
}

// NewPubSubFromDriver creates a new PubSub from the provided driver
//
// Depending on the implementation of the driver (single- or multithreaded)
// a different PubSub implementation will be chosen.
func NewPubSubFromDriver(d PubSubDriverScaffold) (*PubSub, error) {

	var supportsConcurrency bool

	if containsFlag(d.GetDriverFlags(), RequiresConcurrentExecution) {
		_, ok := d.(ConcurrentPubSubDriverScaffold)

		if !ok {
			return nil, errors.New("could not cast driver to concurrency supporting driver")
		}

		supportsConcurrency = true
	} else if containsFlag(d.GetDriverFlags(), RequiresBlockingExecution) {
		_, ok := d.(BlockingPubSubDriverScaffold)

		if !ok {
			return nil, errors.New("could not cast driver to plain driver")
		}

		supportsConcurrency = false
	} else {
		return nil, errors.New("could not create execution plan for given driver - flags do not reflect requirements")
	}

	return &PubSub{
		driver:              d,
		scheduler:           newScheduler(),
		supportsConcurrency: supportsConcurrency,
		backlog:             make(chan interface{}),
		terminate:           make(chan int),
	}, nil
}

// SubscribeAsync creates a new callback function which is invoked
// on any incomming messages.
//
// It returns a error chan which will contain
// all occuring / returned errors of the SubscriberFunc. A nil error
// indicates the auto-unsubscribe after the call of UnsubscribeAll().
// Use the SubscriberIdentifier to Unsubscribe later.
func (a PubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return a.scheduler.SubscribeAsync(fn)
}

// SubscribeSync creates a new callback function like SubscribeAsync().
//
// It will block until recieving error or nil in the error chan, then returns it.
func (a PubSub) SubscribeSync(fn SubscriberFunc) error {
	return a.scheduler.SubscribeSync(fn)
}

// Unsubscribe removes a previously added callback function from the invokation
// loop.
//
// Use the SubscriberIdentifier created when calling SubscribeAsync(). It will
// send a nil error in the callback function's error chan.
func (a PubSub) Unsubscribe(identifier SubscriberIdentifier) {
	a.scheduler.Unsubscribe(identifier)
}

// UnsubscribeAll removes all added callback functions from the invokation
// loop.
//
// It will send a nil error in the callback's function's error chans.
func (a PubSub) UnsubscribeAll() {
	a.scheduler.UnsubscribeAll()
}

// Publish sends a message to the message broker.
func (a PubSub) Publish(msg interface{}) error {
	a.backlog <- msg
	return nil
}

// ListenAsync starts the relay goroutine which uses the provided driver
// to communicate with the message broker.
func (a PubSub) ListenAsync() chan error {

	errCh := make(chan error, 1)

	go func() {
		errCh <- a.ListenSync()
	}()

	return errCh
}

// ListenSync starts relay loops which use the provided driver to
// communicate with the message broker.
func (a PubSub) ListenSync() error {

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

		d, ok := a.driver.(ConcurrentPubSubDriverScaffold)

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

	d, ok := a.driver.(BlockingPubSubDriverScaffold)

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

// Terminate send a termination signal so that the blocking Listen will
// be released.
//
// Subscribers will be unsubscribed was well.
func (a PubSub) Terminate() error {
	a.terminate <- 1
	return nil
}
