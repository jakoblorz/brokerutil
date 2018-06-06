package kafka

import (
	"testing"
)

func TestNewKafkaPubSubDriver(t *testing.T) {
	t.Run("should not return any errors creating new driver", func(t *testing.T) {

		_, err := NewKafkaPubSubDriver()
		if err != nil {
			t.Errorf("NewKafkaPubSubDriver() error = %v", err)
		}

	})
}
