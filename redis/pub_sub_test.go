package redis

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/jakoblorz/brokerutil"
)

func TestNewRedisPubSub(t *testing.T) {
	type args struct {
		channels []string
		opts     *redis.Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "should not return any errors connecting to localhost redis service",
			args: args{
				channels: []string{"test-channel"},
				opts:     &redis.Options{Addr: ":6379"},
			},
			wantErr: false,
		},
		{
			name: "should return error connecting to refused redis service",
			args: args{
				channels: []string{"test-channel"},
				opts:     &redis.Options{Addr: ":5656"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRedisPubSub(tt.args.channels, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRedisPubSub() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPubSub_GetDriverFlags(t *testing.T) {
	tests := []struct {
		name string
		want []brokerutil.Flag
	}{
		{
			name: "should return flags array containing concurrency driver flag",
			want: []brokerutil.Flag{brokerutil.RequiresConcurrentExecution},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PubSub{}
			if got := p.GetDriverFlags(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PubSub.GetDriverFlags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPubSub_GetMessageWriterChannel(t *testing.T) {

	ps, err := NewRedisPubSub([]string{}, &redis.Options{
		Addr: ":6379",
	})

	if err != nil {
		t.Error(err)
	}

	defer ps.client.Close()

	t.Run("should not return any errors", func(t *testing.T) {

		if _, err := ps.GetMessageWriterChannel(); err != nil {
			t.Errorf("PubSub.GetMessageWriterChannel() did return an error: err = %v", err)
		}
	})

	t.Run("should return interface writer channel", func(t *testing.T) {

		if c, _ := ps.GetMessageWriterChannel(); c == nil {
			t.Error("PubSub.GetMessageWriterChannel() did return nil as interface writer channel")
		}
	})
}

func TestPubSub_GetMessageReaderChannel(t *testing.T) {

	ps, err := NewRedisPubSub([]string{}, &redis.Options{
		Addr: ":6379",
	})

	if err != nil {
		t.Error(err)
	}

	defer ps.client.Close()

	t.Run("should not return any errors", func(t *testing.T) {

		if _, err := ps.GetMessageReaderChannel(); err != nil {
			t.Errorf("PubSub.GetMessageReaderChannel() did return an error: err = %v", err)
		}
	})

	t.Run("should return interface reader channel", func(t *testing.T) {

		if c, _ := ps.GetMessageReaderChannel(); c == nil {
			t.Error("PubSub.GetMessageReaderChannel() did return nil as interface reader channel")
		}
	})
}

func TestPubSub_OpenStream(t *testing.T) {

	ps, err := NewRedisPubSub([]string{}, &redis.Options{
		Addr: ":6379",
	})

	if err != nil {
		t.Error(err)
	}

	defer ps.client.Close()

	t.Run("should not return any errors", func(t *testing.T) {

		if err := ps.OpenStream(); err != nil {
			t.Errorf("PubSub.OpenStream() returned an error: err = %v", err)
		}
	})

	t.Run("should relay received message into rx channel", func(t *testing.T) {

		var txMessage = "test message"

		go func() {

			time.Sleep(time.Millisecond)

			err = ps.client.Publish(ps.channelNames[0], txMessage).Err()
			if err != nil {
				t.Error(err)
			}
		}()

		receiveCh, _ := ps.GetMessageReaderChannel()

		rxMessage := <-receiveCh

		if !reflect.DeepEqual(rxMessage, txMessage) {
			t.Errorf("PubSub.OpenStream() did not relay correct recieved message: expected = %v recieved = %v", txMessage, rxMessage)
		}
	})

	t.Run("should send message from tx channel", func(t *testing.T) {

		var txMessage = "test message"

		receiveCh, _ := ps.GetMessageReaderChannel()
		transmitCh, _ := ps.GetMessageWriterChannel()

		transmitCh <- txMessage

		rxMessage := <-receiveCh

		if !reflect.DeepEqual(rxMessage, txMessage) {
			t.Errorf("PubSub.OpenStream() did not send message from tx channel: expected = %v received = %v", txMessage, rxMessage)
		}

	})
}

func TestPubSub_CloseStream(t *testing.T) {

	ps, err := NewRedisPubSub([]string{}, &redis.Options{
		Addr: ":6379",
	})

	if err != nil {
		t.Error(err)
	}

	defer ps.client.Close()

	err = ps.OpenStream()
	if err != nil {
		t.Error(err)
	}

	t.Run("should not return any errors", func(t *testing.T) {

		if err := ps.CloseStream(); err != nil {
			t.Errorf("PubSub.CloseStream() returned an error: err = %v", err)
		}
	})
}
