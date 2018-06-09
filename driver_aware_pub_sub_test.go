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
			executionFlag: BlockingExecution,
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
				t.Error("DriverAwarePubSub.SubscribeAsync() did not extract wrapped message")
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

		d.SubscribeAsyncWithSource(func(interface{}, PubSubDriver) error { return nil })

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
		var extractedDriver PubSubDriver
		d.SubscribeAsyncWithSource(func(msg interface{}, driver PubSubDriver) error {
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

func TestDriverAwarePubSub_SubscribeSync(t *testing.T) {

	t.Run("should invoke SubscribeSync on pub sub", func(t *testing.T) {

		var subscribeSyncInoked = false
		var obSubscribeSync = func(fn SubscriberFunc) error {
			subscribeSyncInoked = true
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeSyncCallbackFunc: obSubscribeSync,
			},
		}

		d.SubscribeSync(func(interface{}) error { return nil })

		if subscribeSyncInoked == false {
			t.Error("DriverAwarePubSub.SubscribeSync() did not invoked SubscribeSync on pub sub")
		}
	})

	t.Run("should invoked SubscribeSync with message extractor", func(t *testing.T) {

		var subscribeSyncFn func(interface{}) error
		var onSubscribeSync = func(fn SubscriberFunc) error {
			subscribeSyncFn = fn
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeSyncCallbackFunc: onSubscribeSync,
			},
		}

		var extractedMessage interface{}
		d.SubscribeSync(func(msg interface{}) error {
			extractedMessage = msg
			return nil
		})

		t.Run("should extract wrapped message", func(t *testing.T) {

			var messagePayloadTestWrapped = "test message test wrapped"

			subscribeSyncFn(syntheticMessageWithSource{
				message: messagePayloadTestWrapped,
				source:  &observableTestDriver{},
			})

			if !reflect.DeepEqual(messagePayloadTestWrapped, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeSync() did not extract wrapped message")
			}
		})

		t.Run("should extract plain message", func(t *testing.T) {

			var messagePayloadTestPlain = "test message test plain"

			subscribeSyncFn(messagePayloadTestPlain)

			if !reflect.DeepEqual(messagePayloadTestPlain, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeSync() did not extract plain message")
			}
		})
	})
}

func TestDriverAwarePubSub_SubscribeSyncWithSource(t *testing.T) {

	t.Run("should invoke SubscribeSync on pub sub", func(t *testing.T) {

		var subscribeSyncInoked = false
		var obSubscribeSync = func(fn SubscriberFunc) error {
			subscribeSyncInoked = true
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeSyncCallbackFunc: obSubscribeSync,
			},
		}

		d.SubscribeSyncWithSource(func(interface{}, PubSubDriver) error { return nil })

		if subscribeSyncInoked == false {
			t.Error("DriverAwarePubSub.SubscribeSynWithSource() did not invoke SubscribeSync on pub sub")
		}

	})

	t.Run("should invoked SubscribeSync with extractor function", func(t *testing.T) {

		var subscribeSyncFn func(interface{}) error
		var onSubscribeSync = func(fn SubscriberFunc) error {
			subscribeSyncFn = fn
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				subscribeSyncCallbackFunc: onSubscribeSync,
			},
		}

		var extractedMessage interface{}
		var extractedDriver PubSubDriver
		d.SubscribeSyncWithSource(func(msg interface{}, driver PubSubDriver) error {
			extractedDriver = driver
			extractedMessage = msg
			return nil
		})

		t.Run("should extract wrapped message with driver", func(t *testing.T) {

			var messagePayloadTestWrapped = "test message test wrapped"

			driver := observableTestDriver{}

			subscribeSyncFn(syntheticMessageWithSource{
				message: messagePayloadTestWrapped,
				source:  &driver,
			})

			if !reflect.DeepEqual(messagePayloadTestWrapped, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeSyncWithSource() did not extract wrapped message")
			}

			if !reflect.DeepEqual(&driver, extractedDriver) {
				t.Error("DriverAwarePubSub.SubscribeSyncWithSource() did not extract wrapped message with driver")
			}
		})

		t.Run("should extract plain message with nil driver", func(t *testing.T) {

			var messagePayloadTestPlain = "test message test plain"

			subscribeSyncFn(messagePayloadTestPlain)

			if !reflect.DeepEqual(messagePayloadTestPlain, extractedMessage) {
				t.Error("DriverAwarePubSub.SubscribeSyncWithSource() did not extract plain message")
			}

			if !reflect.DeepEqual(nil, extractedDriver) {
				t.Error("DriverAwarePubSub.SubscribeSyncWithSource() did not extract plain message with nil driver")
			}
		})

	})
}

func TestDriverAwarePubSub_Unsubscribe(t *testing.T) {

	t.Run("should invoke Unsubscribe on pub sub", func(t *testing.T) {

		var unsubscribeInvoked = false
		var onUnsubscribe = func(SubscriberIdentifier) {
			unsubscribeInvoked = true
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				unsubscribeCallbackFunc: onUnsubscribe,
			},
		}

		d.Unsubscribe("")

		if unsubscribeInvoked == false {
			t.Error("DriverAwarePubSub.Unsubscribe() did not invoked Unsubscribe on pub sub")
		}
	})
}

func TestDriverAwarePubSub_UnsubscribeAll(t *testing.T) {

	t.Run("should invoke UnsubscribeAll on pub sub", func(t *testing.T) {

		var unsubscribeAllInvoked = false
		var onUnsubscribeAll = func() {
			unsubscribeAllInvoked = true
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				unsubscribeAllCallbackFunc: onUnsubscribeAll,
			},
		}

		d.UnsubscribeAll()

		if unsubscribeAllInvoked == false {
			t.Error("DriverAwarePubSub.UnsubscribeAll() did not invoked UnsubscribeAll on pub sub")
		}
	})
}

func TestDriverAwarePubSub_Publish(t *testing.T) {

	t.Run("should invoke Publish on pub sub with wrapped message", func(t *testing.T) {

		var message = "test message"

		var publishInvoked = false
		var onPublish = func(msg interface{}) error {
			publishInvoked = true

			m, ok := msg.(syntheticMessageWithTarget)
			if !ok {
				t.Error("DriverAwarePubSub.Publish() did not invoke Publish on pub sub with wrapped message")
			}

			if !reflect.DeepEqual(message, m.message) {
				t.Error("DriverAwarePubSub.Publish() did not invoke Publish on pub sub with wrapped message with correct content")
			}

			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				publishCallbackFunc: onPublish,
			},
		}

		d.Publish(message)

		if publishInvoked == false {
			t.Error("DriverAwarePubSub.Unsubscribe() did not invoke Publish on pub sub")
		}
	})
}

func TestDriverAwarePubSub_PublishWithTarget(t *testing.T) {

	t.Run("should invoke Publish on pub sub with wrapped message and driver", func(t *testing.T) {

		var message = "test message"
		var driver = observableTestDriver{}

		var publishInvoked = false
		var onPublish = func(msg interface{}) error {
			publishInvoked = true

			m, ok := msg.(syntheticMessageWithTarget)
			if !ok {
				t.Error("DriverAwarePubSub.PublishWithTarget() did not invoke Publish on pub sub with wrapped message")
			}

			if !reflect.DeepEqual(message, m.message) {
				t.Error("DriverAwarePubSub.PublishWithTarget() did not invoke Publish on pub sub with wrapped message with correct content")
			}

			if !reflect.DeepEqual(&driver, m.target) {
				t.Error("DriverAwarePubSub.PublishWithTarget() did not invoke Publish on pub sub with wrapped message with correct driver")
			}

			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				publishCallbackFunc: onPublish,
			},
		}

		d.PublishWithTarget(message, &driver)

		if publishInvoked == false {
			t.Error("DriverAwarePubSub.PublishWithTarget() did not invoke Publish on pub sub")
		}
	})
}

func TestDriverAwarePubSub_ListenAsync(t *testing.T) {

	t.Run("should invoke ListenAsync on pub sub", func(t *testing.T) {

		var listenAsyncInvoked = false
		var onListenAsync = func() chan error {
			listenAsyncInvoked = true
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				listenAsyncCallbackFunc: onListenAsync,
			},
		}

		d.ListenAsync()

		if listenAsyncInvoked == false {
			t.Error("DriverAwarePubSub.ListenAsync() did not invoked ListenAsync on pub sub")
		}
	})
}

func TestDriverAwarePubSub_ListenSync(t *testing.T) {

	t.Run("should invoke ListenSync on pub sub", func(t *testing.T) {

		var listenSyncInvoked = false
		var onListenSync = func() error {
			listenSyncInvoked = true
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				listenSyncCallbackFunc: onListenSync,
			},
		}

		d.ListenSync()

		if listenSyncInvoked == false {
			t.Error("DriverAwarePubSub.ListenSync() did not invoked ListenSync on pub sub")
		}
	})
}

func TestDriverAwarePubSub_Terminate(t *testing.T) {

	t.Run("should invoke Terminate on pub sub", func(t *testing.T) {

		var terminateInvoked = false
		var onTerminate = func() error {
			terminateInvoked = true
			return nil
		}

		d := DriverAwarePubSub{
			pubSub: &observableTestPubSub{
				terminateCallbackFunc: onTerminate,
			},
		}

		d.Terminate()

		if terminateInvoked == false {
			t.Error("DriverAwarePubSub.Terminate() did not invoked Terminate on pub sub")
		}
	})

}
