package loopback

import (
	"reflect"
	"testing"

	"github.com/jakoblorz/brokerutil"
)

func TestNewLoopbackPubSubDriver(t *testing.T) {
	type args struct {
		executionFlag brokerutil.Flag
	}
	tests := []struct {
		name    string
		args    args
		want    brokerutil.Flag
		wantErr bool
	}{
		{
			name: "should return PubSubDriver with RequiresBlockingExecution flag set",
			args: args{
				executionFlag: brokerutil.RequiresBlockingExecution,
			},
			want:    brokerutil.RequiresBlockingExecution,
			wantErr: false,
		},
		{
			name: "should return PubSubDriver with RequiresConcurrentExecution flag set",
			args: args{
				executionFlag: brokerutil.RequiresConcurrentExecution,
			},
			want:    brokerutil.RequiresConcurrentExecution,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLoopbackPubSubDriver(tt.args.executionFlag)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLoopbackPubSubDriver() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.executionFlag, tt.want) {
				t.Errorf("NewLoopbackPubSubDriver() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewLoopbackBlockingPubSubDriver(t *testing.T) {

	t.Run("should return PubSubDriver with RequiresBlockingExecution flag set", func(t *testing.T) {
		got, err := NewLoopbackBlockingPubSubDriver()
		if err != nil {
			t.Errorf("NewLoopbackBlockingPubSubDriver() error = %v ", err)
		}

		if !reflect.DeepEqual(got.executionFlag, brokerutil.RequiresBlockingExecution) {
			t.Errorf("NewLoopbackBlockingPubSubDriver() = %v, want %v", got.executionFlag, brokerutil.RequiresBlockingExecution)
		}
	})
}

func TestNewLoopbackConcurrentPubSubDriver(t *testing.T) {

	t.Run("should return PubSubDriver with RequiresConcurrentExecution flag set", func(t *testing.T) {
		got, err := NewLoopbackConcurrentPubSubDriver()
		if err != nil {
			t.Errorf("NewLoopbackConcurrentPubSubDriver() error = %v ", err)
		}

		if !reflect.DeepEqual(got.executionFlag, brokerutil.RequiresConcurrentExecution) {
			t.Errorf("NewLoopbackConcurrentPubSubDriver() = %v, want %v", got.executionFlag, brokerutil.RequiresConcurrentExecution)
		}
	})
}

func TestPubSubDriver_GetDriverFlags(t *testing.T) {

	blocking, err := NewLoopbackBlockingPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	concurrent, err := NewLoopbackConcurrentPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	t.Run("should return flags with RequiresConcurrentExecution flag", func(t *testing.T) {

		if !reflect.DeepEqual([]brokerutil.Flag{brokerutil.RequiresConcurrentExecution}, concurrent.GetDriverFlags()) {
			t.Error("PubSubDriver.GetDriverFlags() did not return flags with RequiresConcurrentExecution flag")
		}
	})

	t.Run("should return flags with RequiresBlockingExecution flag", func(t *testing.T) {

		if !reflect.DeepEqual([]brokerutil.Flag{brokerutil.RequiresBlockingExecution}, blocking.GetDriverFlags()) {
			t.Error("PubSubDriver.GetDriverFlags() did not return flags with RequiresBlockingExecution flag")
		}
	})
}

func TestPubSubDriver_OpenStream(t *testing.T) {

	blocking, err := NewLoopbackBlockingPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.PubSubDriverScaffold = blocking

	defer driver.CloseStream()

	t.Run("should not return any errors", func(t *testing.T) {

		err := driver.OpenStream()
		if err != nil {
			t.Error(err)
		}
	})

}

func TestPubSubDriver_CloseStream(t *testing.T) {

	blocking, err := NewLoopbackBlockingPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.PubSubDriverScaffold = blocking

	err = driver.OpenStream()
	if err != nil {
		t.Error(err)
	}

	t.Run("should not return any errors", func(t *testing.T) {

		err := driver.CloseStream()
		if err != nil {
			t.Error(err)
		}
	})
}

func TestPubSubDriver_ReceiveMessage(t *testing.T) {

	blocking, err := NewLoopbackBlockingPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.BlockingPubSubDriverScaffold = blocking

	t.Run("should receive message from channel", func(t *testing.T) {

		var message = "test message"

		blocking.channel <- message

		msg, err := driver.ReceiveMessage()
		if err != nil {
			t.Errorf("PubSubDriver.ReceiveMessage() error = %v", err)
		}

		if !reflect.DeepEqual(msg, message) {
			t.Errorf("PubSubDriver.ReceiveMessage() = %v, want %v", msg, message)
		}
	})
}

func TestPubSubDriver_PublishMessage(t *testing.T) {

	blocking, err := NewLoopbackBlockingPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.BlockingPubSubDriverScaffold = blocking

	t.Run("should publish message to channel", func(t *testing.T) {

		var message = "test message"

		if err := driver.PublishMessage(message); err != nil {
			t.Errorf("PubSubDriver.PublishMessage() error = %v", err)
		}

		msg := <-blocking.channel

		if !reflect.DeepEqual(msg, message) {
			t.Errorf("PubSubDriver.PublishMessage() published %v, want %v", msg, message)
		}
	})
}

func TestPubSubDriver_GetMessageWriterChannel(t *testing.T) {

	concurrent, err := NewLoopbackConcurrentPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.ConcurrentPubSubDriverScaffold = concurrent

	t.Run("should return channel with publishing capabilities", func(t *testing.T) {

		var message = "test message"
		msgChan, err := driver.GetMessageWriterChannel()
		if err != nil {
			t.Errorf("PubSubDriver.GetMessageWriterChannel() error = %v", err)
		}

		msgChan <- message

		msg := <-concurrent.channel

		if !reflect.DeepEqual(msg, message) {
			t.Errorf("PubSubDriver.GetMessageWriterChannel() returned error prone channel, published %v, want %v", msg, message)
		}
	})
}

func TestPubSubDriver_GetMessageReaderChannel(t *testing.T) {

	concurrent, err := NewLoopbackConcurrentPubSubDriver()
	if err != nil {
		t.Error(err)
	}

	var driver brokerutil.ConcurrentPubSubDriverScaffold = concurrent

	t.Run("should return channel with receiving capabilities", func(t *testing.T) {

		var message = "test message"
		msgChan, err := driver.GetMessageReaderChannel()
		if err != nil {
			t.Errorf("PubSubDriver.GetMessageReaderChannel() error = %v", err)
		}

		concurrent.channel <- message

		msg := <-msgChan

		if !reflect.DeepEqual(msg, message) {
			t.Errorf("PubSubDriver.GetMessageReaderChanel() returned error prone channel, received %v, want %v", msg, message)
		}
	})
}
