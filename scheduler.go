package brokerutil

// SubscriberIdentifier uniquely identifies a callback function
//
// These identifiers are generated once a callback function is
// created in the internal subscriber map. You can use it
// to remove a subscriber from scheduling rotation
// which is commonly called unsubscribing.
type SubscriberIdentifier string

// SubscriberFunc is the type of a callback function
type SubscriberFunc func(interface{}) error

type subscriberWrapper struct {
	sig chan error
	fn  SubscriberFunc
}

type scheduler interface {
	NotifySubscribers(interface{}) error
	SubscribeAsync(SubscriberFunc) (chan error, SubscriberIdentifier)
	SubscribeSync(SubscriberFunc) error
	Unsubscribe(SubscriberIdentifier)
	UnsubscribeAll()
}

func newScheduler() scheduler {
	return newRoundRobinScheduler()
}
