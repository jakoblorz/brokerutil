package loopback

import (
	"testing"
)

func TestDriver_Open(t *testing.T) {
	type fields struct {
		channel chan interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "should not throw error",
			fields: fields{
				channel: make(chan interface{}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Driver{
				channel: tt.fields.channel,
			}
			if err := d.Open(); (err != nil) != tt.wantErr {
				t.Errorf("Driver.Open() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDriver_Close(t *testing.T) {
	type fields struct {
		channel chan interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "should not throw error",
			fields: fields{
				channel: make(chan interface{}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Driver{
				channel: tt.fields.channel,
			}
			if err := d.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Driver.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewDriver(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "should create driver with initialized channel",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDriver()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDriver() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.channel == nil {
				t.Errorf("NewDriver() = %v, want channel not nil", got)
			}
		})
	}
}

func TestDriver_Publisher(t *testing.T) {
	type fields struct {
		channel chan interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "should create publisher with initialized channel",
			fields: fields{
				channel: make(chan interface{}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Driver{
				channel: tt.fields.channel,
			}
			got, err := d.Publisher()
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.Publisher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.(Publisher).channel == nil {
				t.Errorf("Driver.Publisher() = %v, want channel not nil", got)
			}
		})
	}
}

func TestDriver_Subscriber(t *testing.T) {
	type fields struct {
		channel chan interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "should create subscriber with initialized channel",
			fields: fields{
				channel: make(chan interface{}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Driver{
				channel: tt.fields.channel,
			}
			got, err := d.Subscriber()
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.Subscriber() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.(Subscriber).channel == nil {
				t.Errorf("Driver.Subscriber() = %v, want channel not nil", got)
			}
		})
	}
}
