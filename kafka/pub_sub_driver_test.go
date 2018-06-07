package kafka

import (
	"testing"
)

var (
	localTopic  = "test-topic"
	localBroker = []string{"localhost:9092"}
)

func TestNewKafkaPubSubDriver(t *testing.T) {
	t.Run("should not return any errors creating new driver", func(t *testing.T) {

		_, err := NewKafkaPubSubDriver(localTopic, localBroker, nil)
		if err != nil {
			t.Errorf("NewKafkaPubSubDriver() error = %v", err)
		}

	})
}
