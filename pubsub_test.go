package brokerutil

import (
	"testing"

	"github.com/jakoblorz/brokerutil/driver"
	"github.com/jakoblorz/brokerutil/driver/loopback"
)

type dynamicDriverTestScaffold struct {
	driverType driver.PubSubDriverType
}

func (d dynamicDriverTestScaffold) GetDriverType() driver.PubSubDriverType {
	return d.driverType
}

func (d dynamicDriverTestScaffold) CloseStream() error {
	return nil
}

func (d dynamicDriverTestScaffold) OpenStream() error {
	return nil
}

type observableTestScheduler struct {
	notifySchedulerCallbackFunc func(interface{}) error
	subscribeAsyncCallbackFunc  func(SubscriberFunc) (chan error, SubscriberIdentifier)
	subscribeSyncCallbackFunc   func(SubscriberFunc) error
	unsubscribeCallbackFunc     func(SubscriberIdentifier)
	unsubscribeAllCallbackFunc  func()
}

func (o observableTestScheduler) NotifySubscribers(msg interface{}) error {

	if o.notifySchedulerCallbackFunc != nil {
		return o.notifySchedulerCallbackFunc(msg)
	}

	return nil
}

func (o observableTestScheduler) SubscribeAsync(fn SubscriberFunc) (chan error, SubscriberIdentifier) {

	if o.subscribeAsyncCallbackFunc != nil {
		return o.subscribeAsyncCallbackFunc(fn)
	}

	return nil, ""
}

func (o observableTestScheduler) SubscribeSync(fn SubscriberFunc) error {

	if o.subscribeSyncCallbackFunc != nil {
		return o.subscribeSyncCallbackFunc(fn)
	}

	return nil
}

func (o observableTestScheduler) Unsubscribe(id SubscriberIdentifier) {

	if o.unsubscribeCallbackFunc != nil {
		o.unsubscribeCallbackFunc(id)
	}
}

func (o observableTestScheduler) UnsubscribeAll() {

	if o.unsubscribeAllCallbackFunc != nil {
		o.unsubscribeAllCallbackFunc()
	}
}

func TestNewPubSubFromDriver(t *testing.T) {

	mt, err := loopback.NewMultiThreadDriver()
	if err != nil {
		t.Error(err)
	}

	st, err := loopback.NewSingleThreadDriver()
	if err != nil {
		t.Error(err)
	}

	t.Run("should return PubSub using proper driver wrapper", func(t *testing.T) {
		psMt, err := NewPubSubFromDriver(mt)
		if err != nil {
			t.Error(err)
		}

		if _, ok := psMt.(multiThreadPubSubDriverWrapper); !ok {
			t.Error("NewPubSubFromDriver() did not create proper driver multiThreadPubSubDriverWrapper")
		}

		psSt, err := NewPubSubFromDriver(st)
		if err != nil {
			t.Error(err)
		}

		if _, ok := psSt.(singleThreadPubSubDriverWrapper); !ok {
			t.Error("NewPubSubFromDriver() did not create proper driver singleThreadPubSubDriverWrapper")
		}

		_, err = NewPubSubFromDriver(dynamicDriverTestScaffold{
			driverType: driver.PubSubDriverType(3),
		})

		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when providing wrong driver type")
		}
	})
}

func TestNewPubSubFromMultiThreadDriver(t *testing.T) {

	mt, err := loopback.NewMultiThreadDriver()
	if err != nil {
		t.Error(err)
	}

	t.Run("should return PubSub using multi thread driver wrapper", func(t *testing.T) {
		psMt, err := NewPubSubFromMultiThreadDriver(mt)
		if err != nil {
			t.Error(err)
		}

		if _, ok := psMt.(multiThreadPubSubDriverWrapper); !ok {
			t.Error("NewPubSubFromMultiThreadDriver() did not use proper driver wrapper")
		}
	})
}

func TestNewPubSubFromSingleThreadDriver(t *testing.T) {

	st, err := loopback.NewSingleThreadDriver()
	if err != nil {
		t.Error(err)
	}

	t.Run("should return PubSub using single thread driver wrapper", func(t *testing.T) {
		stMt, err := NewPubSubFromSingleThreadDriver(st)
		if err != nil {
			t.Error(err)
		}

		if _, ok := stMt.(singleThreadPubSubDriverWrapper); !ok {
			t.Error("NewPubSubFromSingleThreadDriver() did not use proper driver wrapper")
		}
	})
}

func Test_multiThreadPubSubDriverWrapper_Publish(t *testing.T) {

	t.Run("should enqueue message when publishing", func(t *testing.T) {
		mt := &multiThreadPubSubDriverWrapper{
			backlog: make(chan interface{}, 1),
		}

		var msgSend interface{} = "test message"

		mt.Publish(msgSend)

		msgReceive := <-mt.backlog

		if msgSend != msgReceive {
			t.Error("multiThreadPubSubDriverWrapper.Publish() did not enqueue message")
		}
	})
}

func Test_singleThreadPubSubDriverWrapper_Publish(t *testing.T) {

	t.Run("should enqueue message when publishing", func(t *testing.T) {
		st := &singleThreadPubSubDriverWrapper{
			backlog: make(chan interface{}, 1),
		}

		var msgSend interface{} = "test message"

		st.Publish(msgSend)

		msgReceive := <-st.backlog

		if msgSend != msgReceive {
			t.Error("singleThreadPubSubDriverWrapper.Publish() did not enqueue message")
		}

	})

}

func Test_multiThreadPubSubDriverWrapper_SubscribeAsync(t *testing.T) {

	t.Run("should invoke SubscribeAsync from scheduler", func(t *testing.T) {

		var onSubscribeAsyncInvoked = false
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
			onSubscribeAsyncInvoked = true
			return nil, ""
		}

		mt := &multiThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		mt.SubscribeAsync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeAsyncInvoked == false {
			t.Error("multiThreadPubSubDriverWrapper.SubscribeAsync() did not invoke SubscribeAsync from scheduler")
		}
	})
}

func Test_singleThreadPubSubDriverWrapper_SubscribeAsync(t *testing.T) {

	t.Run("schould invoke SubscribeAsync from scheduler", func(t *testing.T) {

		var onSubscribeAsyncInvoked = false
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
			onSubscribeAsyncInvoked = true
			return nil, ""
		}

		st := &singleThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		st.SubscribeAsync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeAsyncInvoked == false {
			t.Error("singleThreadPubSubDriverWrapper.SubscribeAsync() did not invoke SubscribeAsync from scheduler")
		}
	})
}

func Test_multiThreadPubSubDriverWrapper_SubscribeSync(t *testing.T) {

	t.Run("should invoke SubscribeSync from scheduler", func(t *testing.T) {

		var onSubscribeSyncInvoked = false
		var onSubscribeSync = func(fn SubscriberFunc) error {
			onSubscribeSyncInvoked = true
			return nil
		}

		mt := &multiThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				subscribeSyncCallbackFunc: onSubscribeSync,
			},
		}

		mt.SubscribeSync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeSyncInvoked == false {
			t.Error("multiThreadPubSubDriverWrapper.SubscribeSync() did not invoke SubscribeSync from scheduler")
		}
	})
}

func Test_singleThreadPubSubDriverWrapper_SubscribeSync(t *testing.T) {

	t.Run("should invoke SubscribeSync from scheduler", func(t *testing.T) {

		var onSubscribeSyncInvoked = false
		var onSubscribeSync = func(fn SubscriberFunc) error {
			onSubscribeSyncInvoked = true
			return nil
		}

		st := &singleThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				subscribeSyncCallbackFunc: onSubscribeSync,
			},
		}

		st.SubscribeSync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeSyncInvoked == false {
			t.Error("singleThreadPubSubDriverWrapper.SubscribeSync() did not invoke SubscribeSync from scheduler")
		}
	})
}

func Test_multiThreadPubSubDriverWrapper_Unsubscribe(t *testing.T) {

	t.Run("should invoke Unsubscribe from scheduler", func(t *testing.T) {

		var onUnsubscribeInvoked = false
		var onUnsubscribe = func(id SubscriberIdentifier) {
			onUnsubscribeInvoked = true
		}

		mt := &multiThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				unsubscribeCallbackFunc: onUnsubscribe,
			},
		}

		mt.Unsubscribe(SubscriberIdentifier("test-identifier"))

		if onUnsubscribeInvoked == false {
			t.Error("multiThreadPubSubDriverWrapper.Unsubscribe() did not invoke Unsubscribe from scheduler")
		}
	})
}

func Test_singleThreadPubSubDriverWrapper_Unsubscribe(t *testing.T) {

	t.Run("should invoke Unsubscribe from scheduler", func(t *testing.T) {

		var onUnsubscribeInvoked = false
		var onUnsubscribe = func(id SubscriberIdentifier) {
			onUnsubscribeInvoked = true
		}

		st := &singleThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				unsubscribeCallbackFunc: onUnsubscribe,
			},
		}

		st.Unsubscribe(SubscriberIdentifier("test-identifier"))

		if onUnsubscribeInvoked == false {
			t.Error("singleThreadPubSubDriverWrapper.Unsubscribe() did not invoke Unsubscribe from scheduler")
		}
	})
}

func Test_multiThreadPubSubDriverWrapper_UnsubscribeAll(t *testing.T) {

	t.Run("should invoke UnsubscribeAll from scheduler", func(t *testing.T) {

		var onUnsubscribeAllInvoked = false
		var onUnsubscribeAll = func() {
			onUnsubscribeAllInvoked = true
		}

		mt := &multiThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				unsubscribeAllCallbackFunc: onUnsubscribeAll,
			},
		}

		mt.UnsubscribeAll()

		if onUnsubscribeAllInvoked == false {
			t.Error("multiThreadPubSubDriverWrapper.UnsubscribeAll() did not invoke UnsubscribeAll from scheduler")
		}
	})
}

func Test_singleThreadPubSubDriverWrapper_UnsubscribeAll(t *testing.T) {

	t.Run("should invoke UnsubscribeAll from scheduler", func(t *testing.T) {

		var onUnsubscribeAllInvoked = false
		var onUnsubscribeAll = func() {
			onUnsubscribeAllInvoked = true
		}

		st := &singleThreadPubSubDriverWrapper{
			scheduler: observableTestScheduler{
				unsubscribeAllCallbackFunc: onUnsubscribeAll,
			},
		}

		st.UnsubscribeAll()

		if onUnsubscribeAllInvoked == false {
			t.Error("multiThreadPubSubDriverWrapper.UnsubscribeAll() did not invoke UnsubscribeAll from scheduler")
		}
	})
}
