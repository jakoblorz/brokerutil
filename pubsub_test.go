package brokerutil

import (
	"testing"

	"github.com/jakoblorz/brokerutil/driver"
	"github.com/jakoblorz/brokerutil/driver/loopback"
)

type dynamicDriverScaffold struct {
	driverType driver.Type
}

func (d dynamicDriverScaffold) GetDriverType() driver.Type {
	return d.driverType
}

func (d dynamicDriverScaffold) NotifyStreamClose() error {
	return nil
}

func (d dynamicDriverScaffold) NotifyStreamOpen() error {
	return nil
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

		_, err = NewPubSubFromDriver(dynamicDriverScaffold{
			driverType: driver.Type(3),
		})

		if err == nil {
			t.Error("NewPubSubFromDriver() did not return error when providing wrong driver type")
		}
	})
}
