package brokerutil

type SubscriberFuncWithSource func(interface{}, PubSubDriverScaffold) error

type DriverAwarePubSub struct {
	pubSub *PubSub
}

func NewDriverAwarePubSub(drivers ...PubSubDriverScaffold) (*DriverAwarePubSub, error) {

	driverOptions := syntheticDriverOptions{
		WrapMessageWithSource: true,
		WrapMessageWithTarget: true,
	}

	driver, err := newSyntheticDriver(&driverOptions, drivers...)
	if err != nil {
		return nil, err
	}

	pubSub, err := NewPubSubFromDriver(driver)
	if err != nil {
		return nil, err
	}

	return &DriverAwarePubSub{
		pubSub: pubSub,
	}, nil
}

func (a DriverAwarePubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
	return a.pubSub.SubscribeAsync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg)
		}

		return fn(message.message)
	})
}

func (a DriverAwarePubSub) SubscribeAsyncWithSource(fn SubscriberFuncWithSource) (chan error, SubscriberIdentifier) {

	return a.pubSub.SubscribeAsync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg, nil)
		}

		return fn(message.message, message.source)
	})
}

func (a DriverAwarePubSub) SubscribeSync(fn SubscriberFunc) error {

	return a.pubSub.SubscribeSync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg)
		}

		return fn(message.message)
	})
}

func (a DriverAwarePubSub) SubscribeSyncWithSource(fn SubscriberFuncWithSource) error {

	return a.pubSub.SubscribeSync(func(msg interface{}) error {

		message, ok := msg.(syntheticMessageWithSource)
		if !ok {
			return fn(msg, nil)
		}

		return fn(message.message, message.source)
	})
}

func (a DriverAwarePubSub) Unsubscribe(identifier SubscriberIdentifier) {
	a.pubSub.Unsubscribe(identifier)
}

func (a DriverAwarePubSub) UnsubscribeAll() {
	a.pubSub.UnsubscribeAll()
}

func (a DriverAwarePubSub) Publish(msg interface{}) error {
	return a.pubSub.Publish(syntheticMessageWithTarget{
		message: msg,
	})
}

func (a DriverAwarePubSub) PublishWithTarget(msg interface{}, target PubSubDriverScaffold) error {
	return a.pubSub.Publish(syntheticMessageWithTarget{
		message: msg,
		target:  target,
	})
}

func (a DriverAwarePubSub) ListenAsync() chan error {
	return a.pubSub.ListenAsync()
}

func (a DriverAwarePubSub) ListenSync() error {
	return a.pubSub.ListenSync()
}
