package brokerutil

import (
	"reflect"
	"testing"
)

func Test_newSyntheticDriver(t *testing.T) {

	t.Run("should return new syntheticDriver with blocking driver without errors", func(t *testing.T) {

		_, err := newSyntheticDriver(nil, observableTestDriver{executionFlag: RequiresBlockingExecution})
		if err != nil {
			t.Errorf("newSyntheticDriver() error = %v", err)
		}
	})

	t.Run("should return new syntheticDriver with concurrent driver without errors", func(t *testing.T) {

		_, err := newSyntheticDriver(nil, observableTestDriver{executionFlag: RequiresConcurrentExecution})
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
		var encoded = td.encodeMessage(message, od)

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
		var encoded = td.encodeMessage(message, od)

		var compare = syntheticMessageWithSource{
			message: message,
			source:  od,
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

func Test_syntheticDriver_GetMessageWriterChannel(t *testing.T) {

	t.Run("should not return error", func(t *testing.T) {

		baseWriterChan := make(chan interface{})

		td := syntheticDriver{
			transmitChan: baseWriterChan,
		}

		_, err := td.GetMessageWriterChannel()
		if err != nil {
			t.Errorf("syntheticDriver.GetMessageWriterChannel() error = %v", err)
		}
	})

	t.Run("should return message writer channel", func(t *testing.T) {

		baseWriterChan := make(chan interface{})

		td := syntheticDriver{
			transmitChan: baseWriterChan,
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
			receiveChan: baseReaderChan,
		}

		_, err := td.GetMessageReaderChannel()
		if err != nil {
			t.Errorf("syntheticDriver.GetMessageReaderChannel() error = %v", err)
		}
	})

	t.Run("should return message writer channel", func(t *testing.T) {

		baseReaderChan := make(chan interface{})

		td := syntheticDriver{
			receiveChan: baseReaderChan,
		}

		channel, _ := td.GetMessageReaderChannel()
		if channel == nil {
			t.Errorf("syntheticDriver.GetMessageReaderChannel() did not return message writer channel")
		}
	})
}
