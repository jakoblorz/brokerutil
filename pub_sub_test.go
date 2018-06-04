package brokerutil

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

type observableTestDriver struct {
	executionFlag                       Flag
	getDriverTypeCallbackFunc           func() []Flag
	getMessageWriterChannelCallbackFunc func() (chan<- interface{}, error)
	getMessageReaderChannelCallbackFunc func() (<-chan interface{}, error)
	closeStreamCallbackFunc             func() error
	openStreamCallbackFunc              func() error
	checkForPendingMessageCallbackFunc  func() (bool, error)
	receivePendingMessageCallbackFunc   func() (interface{}, error)
	publishMessageCallbackFunc          func(interface{}) error
}

func (d observableTestDriver) GetDriverFlags() []Flag {

	if d.getDriverTypeCallbackFunc != nil {
		return d.getDriverTypeCallbackFunc()
	}

	return []Flag{d.executionFlag}
}

func (d observableTestDriver) CloseStream() error {

	if d.closeStreamCallbackFunc != nil {
		return d.closeStreamCallbackFunc()
	}

	return nil
}

func (d observableTestDriver) OpenStream() error {

	if d.openStreamCallbackFunc != nil {
		return d.openStreamCallbackFunc()
	}

	return nil
}

func (d observableTestDriver) CheckForPendingMessage() (bool, error) {

	if d.checkForPendingMessageCallbackFunc != nil {
		return d.checkForPendingMessageCallbackFunc()
	}

	return true, nil
}

func (d observableTestDriver) ReceivePendingMessage() (interface{}, error) {

	if d.receivePendingMessageCallbackFunc != nil {
		return d.receivePendingMessageCallbackFunc()
	}

	return nil, nil
}

func (d observableTestDriver) PublishMessage(msg interface{}) error {

	if d.publishMessageCallbackFunc != nil {
		return d.publishMessageCallbackFunc(msg)
	}

	return nil
}

func (d observableTestDriver) GetMessageWriterChannel() (chan<- interface{}, error) {

	if d.getMessageWriterChannelCallbackFunc != nil {
		return d.getMessageWriterChannelCallbackFunc()
	}

	return nil, nil
}

func (d observableTestDriver) GetMessageReaderChannel() (<-chan interface{}, error) {

	if d.getMessageReaderChannelCallbackFunc != nil {
		return d.getMessageReaderChannelCallbackFunc()
	}

	return nil, nil
}

type pubSubScaffoldImplementation struct {
	executionFlag Flag
}

func (i pubSubScaffoldImplementation) GetDriverFlags() []Flag {
	return []Flag{i.executionFlag}
}

func (i pubSubScaffoldImplementation) CloseStream() error {
	return nil
}

func (i pubSubScaffoldImplementation) OpenStream() error {
	return nil
}

type missingExecutionFlagPubSub struct {
}

func (missingExecutionFlagPubSub) GetDriverFlags() []Flag {
	return []Flag{}
}

func (missingExecutionFlagPubSub) CloseStream() error {
	return nil
}

func (missingExecutionFlagPubSub) OpenStream() error {
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

	t.Run("should set supportsConcurrency to true when supporting driver is present", func(t *testing.T) {

		d := observableTestDriver{
			executionFlag: RequiresConcurrentExecution,
		}

		ps, err := NewPubSubFromDriver(d)
		if err != nil {
			t.Error(err)
		}

		if ps.supportsConcurrency == false {
			t.Error("NewPubSubFromDriver() did not set correct supportsConcurrency flag")
		}
	})

	t.Run("should set supportsConcurrency to false when supporting driver is not present", func(t *testing.T) {

		d := observableTestDriver{
			executionFlag: RequiresBlockingExecution,
		}

		ps, err := NewPubSubFromDriver(d)
		if err != nil {
			t.Error(err)
		}

		if ps.supportsConcurrency == true {
			t.Error("NewPubSubFromDriver() did not set correct supportsConcurrency flag")
		}
	})

	t.Run("should return error when calling with incompatible concurrent driver", func(t *testing.T) {

		d := pubSubScaffoldImplementation{
			executionFlag: RequiresConcurrentExecution,
		}

		_, err := NewPubSubFromDriver(d)
		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when calling with incompatible concurrent driver")
		}
	})

	t.Run("should return error when calling with incompatible blocking driver", func(t *testing.T) {

		d := pubSubScaffoldImplementation{
			executionFlag: RequiresBlockingExecution,
		}

		_, err := NewPubSubFromDriver(d)
		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when calling with incompatible blocking driver")
		}
	})

	t.Run("should return error when driver does not return execution flag", func(t *testing.T) {

		d := missingExecutionFlagPubSub{}

		_, err := NewPubSubFromDriver(d)
		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when driver does not return execution flag")
		}
	})
}

func Test_PubSub_Publish(t *testing.T) {

	t.Run("should enqueue message when publishing", func(t *testing.T) {

		aw := &PubSub{
			backlog: make(chan interface{}, 1),
		}

		var msgSend interface{} = "test message"

		aw.Publish(msgSend)

		msgReceive := <-aw.backlog

		if msgSend != msgReceive {
			t.Error("PubSub.Publish() did not enqueue message")
		}
	})
}

func Test_PubSub_SubscribeAsync(t *testing.T) {

	t.Run("should invoke SubscribeAsync from scheduler", func(t *testing.T) {

		var onSubscribeAsyncInvoked = false
		var onSubscribeAsync = func(fn SubscriberFunc) (chan error, SubscriberIdentifier) {
			onSubscribeAsyncInvoked = true
			return nil, ""
		}

		aw := &PubSub{
			scheduler: observableTestScheduler{
				subscribeAsyncCallbackFunc: onSubscribeAsync,
			},
		}

		aw.SubscribeAsync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeAsyncInvoked == false {
			t.Error("PubSub.SubscribeAsync() did not invoke SubscribeAsync from scheduler")
		}
	})
}

func Test_PubSub_SubscribeSync(t *testing.T) {

	t.Run("should invoked SubscribeSync from scheduler", func(t *testing.T) {

		var onSubscribeSyncInvoked = false
		var onSubscribeSync = func(fn SubscriberFunc) error {
			onSubscribeSyncInvoked = true
			return nil
		}

		aw := &PubSub{
			scheduler: observableTestScheduler{
				subscribeSyncCallbackFunc: onSubscribeSync,
			},
		}

		aw.SubscribeSync(func(msg interface{}) error {
			return nil
		})

		if onSubscribeSyncInvoked == false {
			t.Error("PubSub.SubscribeSync() did not invoke SubscribeSync from scheduler")
		}

	})
}

func Test_PubSub_Unsubscribe(t *testing.T) {

	t.Run("should invoke Unsubscribe from scheduler", func(t *testing.T) {

		var onUnsubscribeInvoked = false
		var onUnsubscribe = func(id SubscriberIdentifier) {
			onUnsubscribeInvoked = true
		}

		aw := &PubSub{
			scheduler: observableTestScheduler{
				unsubscribeCallbackFunc: onUnsubscribe,
			},
		}

		aw.Unsubscribe(SubscriberIdentifier("test-identifier"))

		if onUnsubscribeInvoked == false {
			t.Error("PubSub.Unsubscribe() did not invoke Unsubscribe from scheduler")
		}

	})
}

func Test_PubSub_UnsubscribeAll(t *testing.T) {

	t.Run("should invoke UnsubscribeAll from scheduler", func(t *testing.T) {

		var onUnsubscribeAllInvoked = false
		var onUnsubscribeAll = func() {
			onUnsubscribeAllInvoked = true
		}

		aw := &PubSub{
			scheduler: observableTestScheduler{
				unsubscribeAllCallbackFunc: onUnsubscribeAll,
			},
		}

		aw.UnsubscribeAll()

		if onUnsubscribeAllInvoked == false {
			t.Error("PubSub.UnsubscribeAll() did not invoke UnsubscribeAll from scheduler")
		}

	})
}

func Test_PubSub_ListenSync(t *testing.T) {

	t.Run("should invoke OpenStream from driver", func(t *testing.T) {

		var onOpenStreamInvoked = false
		var onOpenStream = func() error {
			onOpenStreamInvoked = true
			return nil
		}

		ps, err := NewPubSubFromDriver(observableTestDriver{
			executionFlag:          RequiresBlockingExecution,
			openStreamCallbackFunc: onOpenStream,
		})

		if err != nil {
			t.Error(err)
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			ps.Terminate()
		}()

		ps.ListenSync()

		if onOpenStreamInvoked == false {
			t.Error("PubSub.ListenSync() did not invoke OpenStream from driver")
		}
	})

	t.Run("should return error from OpenStream from driver", func(t *testing.T) {
		var onOpenStreamError = errors.New("test error")
		var onOpenStream = func() error {
			return onOpenStreamError
		}

		ps, err := NewPubSubFromDriver(observableTestDriver{
			executionFlag:          RequiresBlockingExecution,
			openStreamCallbackFunc: onOpenStream,
		})

		if err != nil {
			t.Error(err)
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			ps.Terminate()
		}()

		err = ps.ListenSync()
		if !reflect.DeepEqual(err, onOpenStreamError) {
			t.Error("PubSub.ListenSync() did not return error from OpenStream from driver")
		}
	})

	t.Run("should invoke CloseStream from driver", func(t *testing.T) {

		var onCloseStreamInvoked = false
		var onCloseStream = func() error {
			onCloseStreamInvoked = true
			return nil
		}

		ps, err := NewPubSubFromDriver(observableTestDriver{
			executionFlag:           RequiresBlockingExecution,
			closeStreamCallbackFunc: onCloseStream,
		})

		if err != nil {
			t.Error(err)
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			ps.Terminate()
		}()

		ps.ListenSync()

		if onCloseStreamInvoked == false {
			t.Error("PubSub.ListenSync() did not invoke CloseStream from driver")
		}
	})
}

func Test_PubSub_Terminate(t *testing.T) {

	t.Run("should send termination signal in terminate channel", func(t *testing.T) {

		aw := &PubSub{
			terminate: make(chan int, 1),
		}

		aw.Terminate()

		<-aw.terminate
	})
}
