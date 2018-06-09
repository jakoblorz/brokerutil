package brokerutil

// SubscriberFuncWithSource is the type of a subscriber func with provided driver information
type SubscriberFuncWithSource func(interface{}, PubSubDriver) error

// DriverAwarePubSub is an extension of PubSub with multiple drivers which enables its consumers
// to control / be informed from which / to which broker a message is sent / received from
type DriverAwarePubSub struct {
	pubSub pubSuber
}

// NewDriverAwarePubSub creates a new DriverAwarePubSub from the provided drivers
func NewDriverAwarePubSub(drivers ...PubSubDriver) (*DriverAwarePubSub, error) {

	driverOptions := syntheticDriverOptions{
		UseSyntheticMessageWithSource: true,
		UseSyntheticMessageWithTarget: true,
	}

	driverPtr, err := newSyntheticDriver(&driverOptions, drivers...)
	if err != nil {
		return nil, err
	}

	pubSub, _ := NewPubSubFromDriver(driverPtr)

	return &DriverAwarePubSub{
		pubSub: pubSub,
	}, nil
}

// SubscribeAsync creates a new callback function which is invoked
// on any incomming messages.
//
// It returns a error chan which will contain
// all occuring / returned errors of the SubscriberFunc. A nil error
// indicates unsubscription. Use the SubscriberIdentifier to unsubscribe later.
func (a *DriverAwarePubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	return a.pubSub.SubscribeAsync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg)
		}

		return fn(message.message)
	})
}

// SubscribeAsyncWithSource creates a new callback function which is invoked
// on any incomming message with the driver ptr it came from (aka source).
//
// It returns a error chan which will contain
// all occuring / returned errors of the SubscriberFunc. A nil error
// indicates unsubscription. Use the SubscriberIdentifier to unsubscribe later.
func (a *DriverAwarePubSub) SubscribeAsyncWithSource(fn SubscriberFuncWithSource) (chan error, SubscriberIdentifier) {

	return a.pubSub.SubscribeAsync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg, nil)
		}

		return fn(message.message, message.source)
	})
}

// SubscribeSync creates a new callback function like SubscriberAsync().
//
// It will block until receiving error or nil in error chan, then returns it.
func (a *DriverAwarePubSub) SubscribeSync(fn SubscriberFunc) error {

	return a.pubSub.SubscribeSync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg)
		}

		return fn(message.message)
	})
}

// SubscribeSyncWithSource creates a new callback function like SubscribeAsyncWithSource():
// it will be invoked also with the driver ptr the message was received from
//
// It will block until receiving error or nil in error chan, then returns it.
func (a *DriverAwarePubSub) SubscribeSyncWithSource(fn SubscriberFuncWithSource) error {

	return a.pubSub.SubscribeSync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg, nil)
		}

		return fn(message.message, message.source)
	})
}

// Unsubscribe removes a previously added callback function from the invokation
// loop.
//
// Use the SubscriberIdentifier created when calling SubscribeAsync(). It will
// send a nil error in the callback function's error chan.
func (a *DriverAwarePubSub) Unsubscribe(identifier SubscriberIdentifier) {
	a.pubSub.Unsubscribe(identifier)
}

// UnsubscribeAll removes all added callback functions from the invokation
// loop.
//
// It will send a nil error in the callback's function's error chans.
func (a *DriverAwarePubSub) UnsubscribeAll() {
	a.pubSub.UnsubscribeAll()
}

// Publish sends a message to the message broker.
func (a *DriverAwarePubSub) Publish(msg interface{}) error {
	return a.pubSub.Publish(syntheticMessageWithTarget{
		message: msg,
	})
}

// PublishWithTarget sends a message to the message broker. Specify the driver ptr to
// send the message to.
func (a *DriverAwarePubSub) PublishWithTarget(msg interface{}, target PubSubDriver) error {
	return a.pubSub.Publish(syntheticMessageWithTarget{
		message: msg,
		target:  target,
	})
}

// ListenAsync starts the relay goroutine which uses the provided drivers
// to communicate with the message broker.
func (a *DriverAwarePubSub) ListenAsync() chan error {
	return a.pubSub.ListenAsync()
}

// ListenSync starts relay loops which use the provided drivers to
// communicate with the message broker.
func (a *DriverAwarePubSub) ListenSync() error {
	return a.pubSub.ListenSync()
}

// Terminate send a termination signal so that the blocking Listen will
// be released.
//
// Subscribers will be unsubscribed was well.
func (a *DriverAwarePubSub) Terminate() error {
	return a.pubSub.Terminate()
}
