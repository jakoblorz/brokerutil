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

func (d observableTestDriver) ReceiveMessage() (interface{}, error) {

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

type missingImplementationPubSubDriver struct {
	executionFlag Flag
}

func (i missingImplementationPubSubDriver) GetDriverFlags() []Flag {
	return []Flag{i.executionFlag}
}

func (i missingImplementationPubSubDriver) CloseStream() error {
	return nil
}

func (i missingImplementationPubSubDriver) OpenStream() error {
	return nil
}

type missingExecutionFlagPubSubDriver struct {
}

func (missingExecutionFlagPubSubDriver) GetDriverFlags() []Flag {
	return []Flag{}
}

func (missingExecutionFlagPubSubDriver) CloseStream() error {
	return nil
}

func (missingExecutionFlagPubSubDriver) OpenStream() error {
	return nil
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

		d := missingImplementationPubSubDriver{
			executionFlag: RequiresConcurrentExecution,
		}

		_, err := NewPubSubFromDriver(d)
		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when calling with incompatible concurrent driver")
		}
	})

	t.Run("should return error when calling with incompatible blocking driver", func(t *testing.T) {

		d := missingImplementationPubSubDriver{
			executionFlag: RequiresBlockingExecution,
		}

		_, err := NewPubSubFromDriver(d)
		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when calling with incompatible blocking driver")
		}
	})

	t.Run("should return error when driver does not return execution flag", func(t *testing.T) {

		d := missingExecutionFlagPubSubDriver{}

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

func Test_PubSub_ListenAsync(t *testing.T) {

	t.Run("should return error channel", func(t *testing.T) {

		ps, err := NewPubSubFromDriver(observableTestDriver{})
		if err != nil {
			t.Error(err)
		}

		errChan := ps.ListenAsync()

		defer ps.Terminate()

		if errChan == nil {
			t.Error("PubSub.ListenAsync() did not return error channel")
		}
	})

	// t.Run("should return error channel containing errors from ListenSync", func(t *testing.T) {

	// 	var onOpenStreamError = errors.New("test error")
	// 	var onOpenStream = func() error {
	// 		return onOpenStreamError
	// 	}

	// 	ps, err := NewPubSubFromDriver(observableTestDriver{
	// 		openStreamCallbackFunc: onOpenStream,
	// 	})

	// 	if err != nil {
	// 		t.Error(err)
	// 	}

	// 	errChan := ps.ListenAsync()

	// 	defer ps.Terminate()

	// 	time.Sleep(10 * time.Millisecond)

	// 	if !reflect.DeepEqual(<-errChan, onOpenStreamError) {
	// 		t.Error("PubSub.ListenAsync() did not return error channel containing errors from ListenSync")
	// 	}
	// })
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

	t.Run("should invoke UnsubscribeAll from scheduler", func(t *testing.T) {

		var onUnsubscribeAllInvoked = false
		var onUnsubscribeAll = func() {
			onUnsubscribeAllInvoked = true
		}

		ps, err := NewPubSubFromDriver(observableTestDriver{
			executionFlag: RequiresBlockingExecution,
		})

		if err != nil {
			t.Error(err)
		}

		ps.scheduler = observableTestScheduler{
			unsubscribeAllCallbackFunc: onUnsubscribeAll,
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			ps.Terminate()
		}()

		ps.ListenSync()

		if onUnsubscribeAllInvoked == false {
			t.Error("PubSub.ListenSync() did not invoke UnsubscribeAll from scheduler")
		}
	})

	t.Run("concurrent behaviour test", func(t *testing.T) {

		t.Run("should return error when driver cannot be casted to concurrent driver interface", func(t *testing.T) {

			ps := PubSub{
				backlog:             make(chan interface{}),
				terminate:           make(chan int),
				supportsConcurrency: true,
				driver: missingImplementationPubSubDriver{
					executionFlag: RequiresConcurrentExecution,
				},
				scheduler: newScheduler(),
			}

			err := ps.ListenSync()

			if err == nil {
				t.Error("PubSub.ListenSync() did not return error when trying to cast concurrent driver interface")
			}
		})

		t.Run("should invoke GetMessageWriterChannel from driver", func(t *testing.T) {

			var onGetMessageWriterChannelInvoked = false
			var onGetMessageWriterChannel = func() (chan<- interface{}, error) {
				onGetMessageWriterChannelInvoked = true
				return make(chan<- interface{}), nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageWriterChannelCallbackFunc: onGetMessageWriterChannel,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)
				ps.Terminate()
			}()

			ps.ListenSync()

			if onGetMessageWriterChannelInvoked == false {
				t.Error("PubSub.ListenSync() did not invoke GetMessageWriterChannel from driver")
			}
		})

		t.Run("should return error from GetMessageWriterChannel from driver", func(t *testing.T) {

			var onGetMessageWriterChannelError = errors.New("test error")
			var onGetMessageWriterChannel = func() (chan<- interface{}, error) {
				return nil, onGetMessageWriterChannelError
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageWriterChannelCallbackFunc: onGetMessageWriterChannel,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)
				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onGetMessageWriterChannelError) {
				t.Error("PubSub.ListenSync() did not return error from GetMessageWriterChannel from driver")
			}
		})

		t.Run("should invoke GetMessageReaderChannel from driver", func(t *testing.T) {

			var onGetMessageReaderChannelInvoked = false
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				onGetMessageReaderChannelInvoked = true
				return make(<-chan interface{}), nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)
				ps.Terminate()
			}()

			ps.ListenSync()

			if onGetMessageReaderChannelInvoked == false {
				t.Error("PubSub.ListenSync() did not invoke GetMessageReaderChannel from driver")
			}
		})

		t.Run("should return error from GetMessageReaderChannel from driver", func(t *testing.T) {

			var onGetMessageReaderChannelError = errors.New("test error")
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				return nil, onGetMessageReaderChannelError
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)
				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onGetMessageReaderChannelError) {
				t.Error("PubSub.ListenSync() did not return error from GetMessageReaderChannel from driver")
			}
		})

		t.Run("should relay messages from backlog into tx channel", func(t *testing.T) {

			var message = "test message"
			var messageWriterChannel = make(chan interface{}, 1)
			var onGetMessageWriterChannel = func() (chan<- interface{}, error) {
				return messageWriterChannel, nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageWriterChannelCallbackFunc: onGetMessageWriterChannel,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.backlog <- message

				ps.Terminate()
			}()

			ps.ListenSync()

			if !reflect.DeepEqual(message, <-messageWriterChannel) {
				t.Error("PubSub.ListenSync() did not relay messages from backlog into tx channel")
			}
		})

		t.Run("should invoke NotifySubscribers when message was recieved on rx channel", func(t *testing.T) {

			var message = "test message"
			var messageReaderChannel = make(chan interface{}, 1)
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				return messageReaderChannel, nil
			}
			var onNotifySubscribers = func(msg interface{}) error {

				if !reflect.DeepEqual(msg, message) {
					t.Error("PubSub.ListenSync() did not invoke NotifySubscribers when message was recieved on rx channel")
				}

				return nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			})

			if err != nil {
				t.Error(err)
			}

			ps.scheduler = observableTestScheduler{
				notifySchedulerCallbackFunc: onNotifySubscribers,
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				messageReaderChannel <- message

				ps.Terminate()
			}()

			ps.ListenSync()
		})

		t.Run("should return error from NotifySubscribers when message was recieved on rx channel", func(t *testing.T) {

			var message = "test message"
			var messageReaderChannel = make(chan interface{}, 1)
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				return messageReaderChannel, nil
			}
			var onNotifySubscribersError = errors.New("test error")
			var onNotifySubscribers = func(msg interface{}) error {
				return onNotifySubscribersError
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			})

			if err != nil {
				t.Error(err)
			}

			ps.scheduler = observableTestScheduler{
				notifySchedulerCallbackFunc: onNotifySubscribers,
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				messageReaderChannel <- message

				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onNotifySubscribersError) {
				t.Error("PubSub.ListenSync() did not return error from NotifySubscribers when message was recieved on rx channel")
			}

		})
	})

	t.Run("blocking behaviour test", func(t *testing.T) {

		t.Run("should return error when driver cannot be casted to blocking driver interface", func(t *testing.T) {

			ps := PubSub{
				backlog:             make(chan interface{}),
				terminate:           make(chan int),
				supportsConcurrency: false,
				driver: missingImplementationPubSubDriver{
					executionFlag: RequiresBlockingExecution,
				},
				scheduler: newScheduler(),
			}

			err := ps.ListenSync()

			if err == nil {
				t.Error("PubSub.ListenSync() did not return error when trying to cast blocking driver interface")
			}
		})

		t.Run("should invoke PublishMessage from driver with message when recieved from backlog", func(t *testing.T) {

			var message = "test message"
			var onPublishMessageInvoked = false
			var onPublishMessage = func(msg interface{}) error {
				onPublishMessageInvoked = true

				if !reflect.DeepEqual(msg, message) {
					t.Error("PubSub.ListenSync() did not invoke PublishMessage from driver with message")
				}

				return nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				publishMessageCallbackFunc: onPublishMessage,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.backlog <- message

				ps.Terminate()
			}()

			ps.ListenSync()

			if onPublishMessageInvoked == false {
				t.Error("PubSub.ListenSync() did not invoke PublishMessage from driver")
			}
		})

		t.Run("should return error from PublishMessage from driver", func(t *testing.T) {

			var onPublishMessageError = errors.New("test error")
			var onPublishMessage = func(msg interface{}) error {
				return onPublishMessageError
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				publishMessageCallbackFunc: onPublishMessage,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.backlog <- "test message"

				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onPublishMessageError) {
				t.Error("PubSub.ListenSync() did not return error from PublishMessage from driver")
			}
		})

		t.Run("should invoke ReceivePendingMessage from driver", func(t *testing.T) {

			t.Run("should invoke ReceivePendingMessage from driver when CheckForPendingMessage returned true", func(t *testing.T) {

				var onReceivePendingMessageInvoked = false
				var onReceivePendingMessage = func() (interface{}, error) {
					onReceivePendingMessageInvoked = true
					return nil, nil
				}

				ps, err := NewPubSubFromDriver(observableTestDriver{
					executionFlag:                     RequiresBlockingExecution,
					receivePendingMessageCallbackFunc: onReceivePendingMessage,
				})

				if err != nil {
					t.Error(err)
				}

				go func() {
					time.Sleep(10 * time.Millisecond)

					ps.Terminate()
				}()

				ps.ListenSync()

				if onReceivePendingMessageInvoked == false {
					t.Error("PubSub.ListenSync() did not invoke ReceivePendingMessage from driver when CheckForPendingMessage returned true")
				}

			})

		})

		t.Run("should return error from ReceivePendingMessage from driver", func(t *testing.T) {

			var onReceivePendingMessageError = errors.New("test error")
			var onReceivePendingMessage = func() (interface{}, error) {
				return nil, onReceivePendingMessageError
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                     RequiresBlockingExecution,
				receivePendingMessageCallbackFunc: onReceivePendingMessage,
			})

			if err != nil {
				t.Error(err)
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onReceivePendingMessageError) {
				t.Error("PubSub.ListenSync() did not return error from ReceivePendingMessage from driver")
			}

		})

		t.Run("should invoke NotifySubscribers from scheduler with message", func(t *testing.T) {

			var message = "test message"
			var onNotifySubscribersInvoked = false
			var onNotifySubscribers = func(msg interface{}) error {

				onNotifySubscribersInvoked = true

				if !reflect.DeepEqual(msg, message) {
					t.Error("PubSub.ListenSync() did not invoke NotifySubscribers from scheduler with message")
				}

				return nil
			}

			var onRecievePendingMessage = func() (interface{}, error) {
				return message, nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                     RequiresBlockingExecution,
				receivePendingMessageCallbackFunc: onRecievePendingMessage,
			})

			if err != nil {
				t.Error(err)
			}

			ps.scheduler = observableTestScheduler{
				notifySchedulerCallbackFunc: onNotifySubscribers,
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.Terminate()
			}()

			ps.ListenSync()

			if onNotifySubscribersInvoked == false {
				t.Error("PubSub.ListenSync() did not invoke NotifySubscribers from scheduler")
			}
		})

		t.Run("should return error from NotifySubscribers from scheduler", func(t *testing.T) {

			var onNotifySubscribersError = errors.New("test error")
			var onNotifySubscribers = func(msg interface{}) error {
				return onNotifySubscribersError
			}

			var onReceivePendingMessage = func() (interface{}, error) {
				return "test message", nil
			}

			ps, err := NewPubSubFromDriver(observableTestDriver{
				executionFlag:                     RequiresBlockingExecution,
				receivePendingMessageCallbackFunc: onReceivePendingMessage,
			})

			if err != nil {
				t.Error(err)
			}

			ps.scheduler = observableTestScheduler{
				notifySchedulerCallbackFunc: onNotifySubscribers,
			}

			go func() {
				time.Sleep(10 * time.Millisecond)

				ps.Terminate()
			}()

			err = ps.ListenSync()

			if !reflect.DeepEqual(err, onNotifySubscribersError) {
				t.Error("PubSub.ListenSync() did not return error from NotifySubscribers from scheduler")
			}
		})
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
