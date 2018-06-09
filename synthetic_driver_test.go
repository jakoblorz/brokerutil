package brokerutil

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func Test_newSyntheticDriver(t *testing.T) {

	t.Run("should return new syntheticDriver with blocking driver without errors", func(t *testing.T) {

		_, err := newSyntheticDriver(nil, &observableTestDriver{executionFlag: RequiresBlockingExecution})
		if err != nil {
			t.Errorf("newSyntheticDriver() error = %v", err)
		}
	})

	t.Run("should return new syntheticDriver with concurrent driver without errors", func(t *testing.T) {

		_, err := newSyntheticDriver(nil, &observableTestDriver{executionFlag: RequiresConcurrentExecution})
		if err != nil {
			t.Errorf("newSyntheticDriver() error = %v", err)
		}
	})

	t.Run("should return error with missing execution flag driver", func(t *testing.T) {

		_, err := newSyntheticDriver(nil, missingExecutionFlagPubSubDriver{})
		if err == nil {
			t.Errorf("newSyntheticDriver() did not return error")
		}
	})
}

func Test_syntheticDriver_getOptions(t *testing.T) {

	t.Run("should return options", func(t *testing.T) {

		d := syntheticDriver{
			options: &syntheticDriverOptions{},
		}

		options := d.getOptions()

		if !reflect.DeepEqual(d.options, options) {
			t.Errorf("syntheticDriver.getOptions() did not return options")
		}
	})

	t.Run("should return default driver options when options is nil", func(t *testing.T) {

		d := syntheticDriver{
			options: nil,
		}

		options := d.getOptions()

		if !reflect.DeepEqual(defaultSyntheticDriverOptions, options) {
			t.Errorf("syntheticDriver.getOptions() did not return default driver options")
		}
	})
}

func Test_syntheticDriver_encodeMessage(t *testing.T) {

	t.Run("should return bare message when UseSyntheticMessageWithSource is set to false", func(t *testing.T) {

		td := syntheticDriver{
			options: &syntheticDriverOptions{
				UseSyntheticMessageWithSource: false,
			},
		}

		od := observableTestDriver{}

		var message = "test driver"
		var encoded = td.encodeMessage(message, &od)

		if !reflect.DeepEqual(message, encoded) {
			t.Errorf("syntheticDriver.encodeMessage() did not return bare message")
		}
	})

	t.Run("should return wrapped message when UseSyntheticMessageWithSource is set to true", func(t *testing.T) {

		td := syntheticDriver{
			options: &syntheticDriverOptions{
				UseSyntheticMessageWithSource: true,
			},
		}

		od := observableTestDriver{}

		var message = "test driver"
		var encoded = td.encodeMessage(message, &od)

		var compare = syntheticMessageWithSource{
			message: message,
			source:  &od,
		}

		if !reflect.DeepEqual(encoded, compare) {
			t.Errorf("syntheticDriver.encodeMesage() did not return wrapped message")
		}
	})
}

func Test_syntheticDriver_decodeMessage(t *testing.T) {

	t.Run("should return message with nil driver when UseSyntheticMessageWithTarget is set to false", func(t *testing.T) {

		td := syntheticDriver{
			options: &syntheticDriverOptions{
				UseSyntheticMessageWithTarget: false,
			},
		}

		var message = "test message"
		var decoded, driver = td.decodeMessage(message)

		if driver != nil {
			t.Errorf("syntheticDriver.decodeMessage() did not return nil driver")
		}

		if !reflect.DeepEqual(message, decoded) {
			t.Errorf("syntheticDriver.decodeMessage() did not return base message")
		}
	})

	t.Run("should return message with driver when UseSyntheticMessageWithTarget is set to true", func(t *testing.T) {

		od := observableTestDriver{}

		td := syntheticDriver{
			options: &syntheticDriverOptions{
				UseSyntheticMessageWithTarget: true,
			},
		}

		var message = "test message"
		var wrappedMessage = syntheticMessageWithTarget{
			message: message,
			target:  &od,
		}

		var decoded, driver = td.decodeMessage(wrappedMessage)
		if driver == nil || !reflect.DeepEqual(driver, &od) {
			t.Errorf("syntheticDriver.decodeMessage() did not return correct driver")
		}
		if !reflect.DeepEqual(decoded, message) {
			t.Errorf("syntheticDriver.decodeMessage() did not return correct message")
		}
	})

	t.Run("should return message with nil driver when message is not wrapped", func(t *testing.T) {

		td := syntheticDriver{
			options: &syntheticDriverOptions{
				UseSyntheticMessageWithTarget: true,
			},
		}

		var message = "test message"
		var decoded, driver = td.decodeMessage(message)
		if driver != nil {
			t.Errorf("syntheticDriver.decodeMessage() did not return nil driver")
		}
		if !reflect.DeepEqual(message, decoded) {
			t.Errorf("syntheticDriver.decodeMessage() did not return correct message")
		}
	})

}

func Test_syntheticDriver_GetDriverFlags(t *testing.T) {

	t.Run("should return RequiresConcurrentExecution flag", func(t *testing.T) {

		td := syntheticDriver{}

		if !containsFlag(td.GetDriverFlags(), RequiresConcurrentExecution) {
			t.Errorf("syntheticDriver.GetDriverFlags() did not return RequiresConcurrentExecution flag")
		}
	})
}

func Test_syntheticDriver_OpenStream(t *testing.T) {

	t.Run("should invoke OpenStream on each driver", func(t *testing.T) {

		var invokeCount = 0
		var onOpenStream = func() error {
			invokeCount++
			return nil
		}

		od1 := observableTestDriver{
			executionFlag:          RequiresConcurrentExecution,
			openStreamCallbackFunc: onOpenStream,
		}

		od2 := observableTestDriver{
			executionFlag:          RequiresConcurrentExecution,
			openStreamCallbackFunc: onOpenStream,
		}

		d, err := newSyntheticDriver(nil, &od1, &od2)
		if err != nil {
			t.Errorf("%v", err)
		}

		d.OpenStream()

		defer d.CloseStream()

		if invokeCount != 2 {
			t.Errorf("syntheticDriver.OpenStream() did not invoke OpenStream in each driver")
		}
	})

	t.Run("should return error from OpenStream from driver", func(t *testing.T) {

		var onOpenStreamError = errors.New("test error")
		var onOpenStream = func() error {
			return onOpenStreamError
		}

		od := observableTestDriver{
			executionFlag:          RequiresConcurrentExecution,
			openStreamCallbackFunc: onOpenStream,
		}

		d, err := newSyntheticDriver(nil, &od)
		if err != nil {
			t.Errorf("%v", err)
		}

		err = d.OpenStream()

		if !reflect.DeepEqual(onOpenStreamError, err) {
			t.Errorf("syntheticDriver.OpenStream() did not return error from OpenStream from driver")
		}
	})

	t.Run("should return error when failing to cast concurrent driver", func(t *testing.T) {

		md := missingImplementationPubSubDriver{
			executionFlag: RequiresConcurrentExecution,
		}

		d, err := newSyntheticDriver(nil, md)
		if err != nil {
			t.Errorf("%v", err)
		}

		err = d.OpenStream()

		if err == nil {
			t.Errorf("syntheticDriver.OpenStream() did not return error when failing to cast concurrent driver")
		}
	})

	t.Run("should return error when failing to cast blocking driver", func(t *testing.T) {

		md := missingImplementationPubSubDriver{
			executionFlag: RequiresBlockingExecution,
		}

		d, err := newSyntheticDriver(nil, md)
		if err != nil {
			t.Errorf("%v", err)
		}

		err = d.OpenStream()

		if err == nil {
			t.Errorf("syntheticDriver.OpenStream() did not return error when failing to cast blocking driver")
		}
	})

	t.Run("concurrent behaviour", func(t *testing.T) {

		t.Run("should relay received messages unencoded from driver", func(t *testing.T) {

			var messageReaderChannel = make(chan interface{}, 1)
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				return messageReaderChannel, nil
			}

			od := observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithSource: false}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var message = "test message"
			var messageReceiverChannel, _ = d.GetMessageReaderChannel()

			messageReaderChannel <- message

			if !reflect.DeepEqual(message, <-messageReceiverChannel) {
				t.Errorf("syntheticDriver.OpenStream() did not relay received messages unencoded from driver")
			}
		})

		t.Run("should relay received messages encoded from driver", func(t *testing.T) {

			var messageReaderChannel = make(chan interface{}, 1)
			var onGetMessageReaderChannel = func() (<-chan interface{}, error) {
				return messageReaderChannel, nil
			}

			od := observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageReaderChannelCallbackFunc: onGetMessageReaderChannel,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithSource: true}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var message = "test message"
			var messageReceiverChannel, _ = d.GetMessageReaderChannel()

			messageReaderChannel <- message

			msg, ok := (<-messageReceiverChannel).(syntheticMessageWithSource)
			if !ok {
				t.Errorf("syntheticDriver.OpenStream() did not relay received messages encoded from driver")
			}

			if !reflect.DeepEqual(msg.source, &od) {
				t.Errorf("syntheticDriver.OpenStream() did not encoded driver correctly")
			}

		})

		t.Run("should relay published messages unencoded to driver", func(t *testing.T) {

			var messageWriterChannel = make(chan interface{}, 1)
			var onGetMessageWriterChannel = func() (chan<- interface{}, error) {
				return messageWriterChannel, nil
			}

			od := observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageWriterChannelCallbackFunc: onGetMessageWriterChannel,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithTarget: false}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var message = "test message"
			var messageSenderChannel, _ = d.GetMessageWriterChannel()

			messageSenderChannel <- message

			if receivedMessage := <-messageWriterChannel; !reflect.DeepEqual(message, receivedMessage) {
				t.Errorf("syntheticDriver.OpenStream() did not relay published messages unencoded to driver")
			}
		})

		t.Run("should relay published message unwrapped encoded to correct driver", func(t *testing.T) {

			var messageWriterChannel = make(chan interface{}, 1)
			var onGetMessageWriterChannel = func() (chan<- interface{}, error) {
				return messageWriterChannel, nil
			}

			od1 := observableTestDriver{
				executionFlag:                       RequiresConcurrentExecution,
				getMessageWriterChannelCallbackFunc: onGetMessageWriterChannel,
			}

			od2 := observableTestDriver{
				executionFlag: RequiresConcurrentExecution,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithTarget: true}, &od1, &od2)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var messagePayload = "test message"
			var message = syntheticMessageWithTarget{
				message: messagePayload,
				target:  &od1,
			}

			var messageSenderChannel, _ = d.GetMessageWriterChannel()

			messageSenderChannel <- message

			if !reflect.DeepEqual(messagePayload, <-messageWriterChannel) {
				t.Errorf("syntheticDriver.OpenStream() did not relay published message unwrapped encoded to correct driver")
			}
		})
	})

	t.Run("blocking behaviour", func(t *testing.T) {

		t.Run("should relay received message unencoded from driver", func(t *testing.T) {

			var message = "test message"
			var onReceiveMessage = func() (interface{}, error) {
				return message, nil
			}

			od := observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				receiveMessageCallbackFunc: onReceiveMessage,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithSource: false}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var messageReaderChannel, _ = d.GetMessageReaderChannel()

			if !reflect.DeepEqual(message, <-messageReaderChannel) {
				t.Errorf("syntheticDriver.OpenStream() did not relay received message unencoded from driver")
			}
		})

		t.Run("should relay received messages encoded from driver", func(t *testing.T) {

			var message = "test message"
			var onReceiveMessage = func() (interface{}, error) {
				return message, nil
			}

			od := observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				receiveMessageCallbackFunc: onReceiveMessage,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithSource: true}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var messageReaderChannel, _ = d.GetMessageReaderChannel()
			var expectedMessage = syntheticMessageWithSource{
				message: message,
				source:  &od,
			}

			if !reflect.DeepEqual(expectedMessage, <-messageReaderChannel) {
				t.Errorf("syntheticDriver.OpenStream() did not relay received message encoded from driver")
			}
		})

		t.Run("should relay published message unencoded to driver", func(t *testing.T) {

			var s = &sync.WaitGroup{}

			var message = "test message"
			var onReceiveMessage = func() (interface{}, error) {
				time.Sleep(time.Millisecond)
				return nil, nil
			}

			s.Add(1)

			var onPublishMessage = func(msg interface{}) error {

				if !reflect.DeepEqual(message, msg) {
					t.Errorf("syntheticDriver.OpenStream() did not relay received message unencoded to driver")
				}

				s.Done()

				return nil
			}

			od := observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				receiveMessageCallbackFunc: onReceiveMessage,
				publishMessageCallbackFunc: onPublishMessage,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithTarget: false}, &od)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var messagePublishChannel, _ = d.GetMessageWriterChannel()

			messagePublishChannel <- message

			s.Wait()
		})

		t.Run("should relay published encoded message unencoded to correct driver", func(t *testing.T) {

			var s = &sync.WaitGroup{}

			var messagePayload = "test message"
			var onReceiveMessage = func() (interface{}, error) {
				time.Sleep(time.Millisecond)
				return nil, nil
			}

			s.Add(1)

			var onPublishMessage = func(msg interface{}) error {

				if !reflect.DeepEqual(messagePayload, msg) {
					t.Errorf("syntheticDriver.OpenStream() did not relay published encoded message unencoded to driver")
				}

				s.Done()

				return nil
			}

			var onPublishMessageWrongDriver = func(msg interface{}) error {

				t.Errorf("syntheticDriver.OpenStream() did not relay message to correct driver")

				return nil
			}

			od1 := observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				receiveMessageCallbackFunc: onReceiveMessage,
				publishMessageCallbackFunc: onPublishMessage,
			}

			od2 := observableTestDriver{
				executionFlag:              RequiresBlockingExecution,
				receiveMessageCallbackFunc: onReceiveMessage,
				publishMessageCallbackFunc: onPublishMessageWrongDriver,
			}

			var message = syntheticMessageWithTarget{
				message: messagePayload,
				target:  &od1,
			}

			d, err := newSyntheticDriver(&syntheticDriverOptions{UseSyntheticMessageWithTarget: true}, &od1, &od2)
			if err != nil {
				t.Errorf("%v", err)
			}

			d.OpenStream()

			defer d.CloseStream()

			var messagePublishChannel, _ = d.GetMessageWriterChannel()

			messagePublishChannel <- message

			s.Wait()
		})
	})
}

func Test_syntheticDriver_GetMessageWriterChannel(t *testing.T) {

	t.Run("should not return error", func(t *testing.T) {

		baseWriterChan := make(chan interface{})

		td := syntheticDriver{
			out: baseWriterChan,
		}

		_, err := td.GetMessageWriterChannel()
		if err != nil {
			t.Errorf("syntheticDriver.GetMessageWriterChannel() error = %v", err)
		}
	})

	t.Run("should return message writer channel", func(t *testing.T) {

		baseWriterChan := make(chan interface{})

		td := syntheticDriver{
			out: baseWriterChan,
		}

		channel, _ := td.GetMessageWriterChannel()
		if channel == nil {
			t.Errorf("syntheticDriver.GetMessageWriterChannel() did not return message writer channel")
		}
	})
}

func Test_syntheticDriver_GetMessageReaderChannel(t *testing.T) {

	t.Run("should not return error", func(t *testing.T) {

		baseReaderChan := make(chan interface{})

		td := syntheticDriver{
			in: baseReaderChan,
		}

		_, err := td.GetMessageReaderChannel()
		if err != nil {
			t.Errorf("syntheticDriver.GetMessageReaderChannel() error = %v", err)
		}
	})

	t.Run("should return message writer channel", func(t *testing.T) {

		baseReaderChan := make(chan interface{})

		td := syntheticDriver{
			in: baseReaderChan,
		}

		channel, _ := td.GetMessageReaderChannel()
		if channel == nil {
			t.Errorf("syntheticDriver.GetMessageReaderChannel() did not return message writer channel")
		}
	})
}
