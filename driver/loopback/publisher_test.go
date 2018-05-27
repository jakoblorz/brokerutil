package loopback

import (
	"reflect"
	"testing"
)

func TestPublisher_NotifyOnMessagePublish(t *testing.T) {

	type fields struct {
		channel chan interface{}
	}
	type args struct {
		msg interface{}
	}

	arg := args{
		msg: "test",
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "should append message to channel",
			fields: fields{
				channel: make(chan interface{}),
			},
			args:    arg,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Publisher{
				channel: tt.fields.channel,
			}

			go func() {

				msg := <-p.channel

				if eq := !reflect.DeepEqual(arg.msg, msg); eq != tt.wantErr {
					t.Errorf("Publisher.NotifyOnMessagePublish() recieved message not equal to expected value, wantErr %v", tt.wantErr)
				}
			}()

			if err := p.NotifyOnMessagePublish(tt.args.msg); (err != nil) != tt.wantErr {
				t.Errorf("Publisher.NotifyOnMessagePublish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
