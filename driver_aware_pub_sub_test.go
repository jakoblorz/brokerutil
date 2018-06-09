package brokerutil

import (
	"reflect"
	"testing"
)

type observableTestPubSub struct {
	subscribeAsyncCallbackFunc func(SubscriberFunc) (chan error, SubscriberIdentifier)
	subscribeSyncCallbackFunc  func(SubscriberFunc) error
	unsubscribeCallbackFunc    func(SubscriberIdentifier)
	unsubscribeAllCallbackFunc func()
	publishCallbackFunc        func(interface{}) error
	listenAsyncCallbackFunc    func() chan error
	listenSyncCallbackFunc     func() error
	terminateCallbackFunc      func() error
}

func (o *observableTestPubSub) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	if o.subscribeAsyncCallbackFunc != nil {
		return o.subscribeAsyncCallbackFunc(fn)
	}

	return nil, ""
}

func (o *observableTestPubSub) SubscribeSync(fn SubscriberFunc) error {

	if o.subscribeSyncCallbackFunc != nil {
		return o.subscribeSyncCallbackFunc(fn)
	}

	return nil
}

func (o *observableTestPubSub) Unsubscribe(id SubscriberIdentifier) {

	if o.unsubscribeCallbackFunc != nil {
		o.unsubscribeCallbackFunc(id)
	}
}

func (o *observableTestPubSub) UnsubscribeAll() {

	if o.unsubscribeAllCallbackFunc != nil {
		o.unsubscribeAllCallbackFunc()
	}
}

func (o *observableTestPubSub) Publish(msg interface{}) error {

	if o.publishCallbackFunc != nil {
		return o.publishCallbackFunc(msg)
	}

	return nil
}

func (o *observableTestPubSub) ListenAsync() chan error {

	if o.listenAsyncCallbackFunc != nil {
		return o.listenAsyncCallbackFunc()
	}

	return nil
}

func (o *observableTestPubSub) ListenSync() error {

	if o.listenSyncCallbackFunc != nil {
		return o.listenSyncCallbackFunc()
	}

	return nil
}

func (o *observableTestPubSub) Terminate() error {

	if o.terminateCallbackFunc != nil {
		return o.terminateCallbackFunc()
	}

	return nil
}

func TestNewDriverAwarePubSub(t *testing.T) {

	t.Run("should return error when synthetic driver constructor fails", func(t *testing.T) {

		md := missingExecutionFlagPubSubDriver{}
		_, err := NewDriverAwarePubSub(&md)
		if err == nil {
			t.Errorf("NewDriverAwarePubSub() did not return error when synthetic driver constructor fails")
		}
	})

	t.Run("should not return any errors", func(t *testing.T) {

		od := observableTestDriver{
			executionFlag: RequiresBlockingExecution,
		}

		_, err := NewDriverAwarePubSub(&od)
		if err != nil {
			t.Errorf("NewDriverAwarePubSub() error = %v", err)
		}
	})
}

func TestDriverAwarePubSub_SubscribeAsync(t *testing.T) {

	t.Run("should invoke SubscribeAsync on pub sub", func(t *testing.T) {

		var subscribeAsyncInvoked = false
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

			subscribeAsyncInvoked = true

			return nil, ""
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		d.SubscribeAsync(func(interface{}) error { return nil })

		if subscribeAsyncInvoked == false {
			t.Error("DriverAwarePubSub.SubscribeAsync() did not invoke SubscribeAsync on pub sub")
		}
	})

	t.Run("should invoke SubscribeAsync with message extractor function", func(t *testing.T) {

		var subscribeAsyncFn func(interface{}) error
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
			subscribeAsyncFn = fn
			return nil, ""
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}
		var extractedMessage interface{}
		d.SubscribeAsync(func(msg interface{}) error {
			extractedMessage = msg
			return nil
		})

		t.Run("should extract wrapped message", func(t *testing.T) {

			var messagePayloadTestWrapped = "test message test wrapped"

			subscribeAsyncFn(syntheticMessageWithSource{
				message: messagePayloadTestWrapped,
				source:  &observableTestDriver{},
			})

			if !reflect.DeepEqual(messagePayloadTestWrapped, extractedMessage) {
				t.Errorf("DriverAwarePubSub.SubscribeAsync() did not extract wrapped message")
			}
		})

		t.Run("should extract plain message", func(t *testing.T) {

			var messagePayloadTestPlain = "test message test plain"

			subscribeAsyncFn(messagePayloadTestPlain)

			if !reflect.DeepEqual(messagePayloadTestPlain, extractedMessage) {
				t.Errorf("DriverAwarePubSub.SubscribeAsync() did not extract plain message")
			}
		})
	})
}

func TestDriverAwarePubSub_SubscribeAsyncWithSource(t *testing.T) {

	t.Run("should invoke SubscribeAsync on pub sub", func(t *testing.T) {

		var subscribeAsyncInvoked = false
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

			subscribeAsyncInvoked = true

			return nil, ""
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		d.SubscribeAsyncWithSource(func(interface{}, PubSubDriverScaffold) error { return nil })

		if subscribeAsyncInvoked == false {
			t.Error("DriverAwarePubSub.SubscribeAsyncWithSource() did not invoked SubscribeAsync on pub sub")
		}
	})

	t.Run("should invoke SubscribeAsync with message extractor function", func(t *testing.T) {

		var subscribeAsyncFn func(interface{}) error
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
			subscribeAsyncFn = fn
			return nil, ""
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		var extractedMessage interface{}
		var extractedDriver PubSubDriverScaffold
		d.SubscribeAsyncWithSource(func(msg interface{}, driver PubSubDriverScaffold) error {
			extractedDriver = driver
			extractedMessage = msg
			return nil
		})

		t.Run("should extract wrapped message with driver", func(t *testing.T) {

			var messagePayloadTestWrapped = "test message test wrapped"

			driver := observableTestDriver{}

			subscribeAsyncFn(syntheticMessageWithSource{
				message: messagePayloadTestWrapped,
				source:  &driver,
			})

			if !reflect.DeepEqual(messagePayloadTestWrapped, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeAsyncWithSource() did not extract wrapped message")
			}

			if !reflect.DeepEqual(&driver, extractedDriver) {
				t.Error("DriverAwarePubSub.SubscribeAsyncWithSource() did not extract wrapped message with driver")
			}
		})

		t.Run("should extract plain message with nil driver", func(t *testing.T) {

			var messagePayloadTestPlain = "test message test plain"

			subscribeAsyncFn(messagePayloadTestPlain)

			if !reflect.DeepEqual(messagePayloadTestPlain, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeAsyncWithSource() did not extract plain message")
			}

			if !reflect.DeepEqual(nil, extractedDriver) {
				t.Error("DriverAwarePubSub.SubscribeAsyncWithSource() did not extract plain message with nil driver")
			}
		})
	})
}
