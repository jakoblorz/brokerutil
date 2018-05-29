package loopback

import (
	"testing"

	"github.com/jakoblorz/singapoor/stream"
)

func TestSubscriber_GetMessageChannel(t *testing.T) {
	type fields struct {
		channel chan interface{}
		manager *stream.SubscriberManager
	}
	tests := []struct {
		name    string
		fields  fields
		want    <-chan interface{}
		wantErr bool
	}{
		{
			name: "should not throw error",
			fields: fields{
				channel: make(chan interface{}),
				manager: stream.NewSubscriberManager(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Subscriber{
				channel: tt.fields.channel,
				manager: tt.fields.manager,
			}
			_, err := s.GetMessageChannel()
			if (err != nil) != tt.wantErr {
				t.Errorf("Subscriber.GetMessageChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
