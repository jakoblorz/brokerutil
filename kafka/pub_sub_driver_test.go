package kafka

import (
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/jakoblorz/brokerutil"
)

var (
	localTopic  = "test-topic"
	localBroker = []string{"localhost:9092"}
)

func TestNewKafkaPubSubDriver(t *testing.T) {
	t.Run("should not return any errors creating new driver", func(t *testing.T) {

		client, err := sarama.NewClient(localBroker, nil)
		if err != nil {
			t.Errorf("%v", err)
		}

		_, err = NewKafkaPubSubDriverFromClient(localTopic, &client)
		if err != nil {
			t.Errorf("NewKafkaPubSubDriver() error = %v", err)
		}

		defer client.Close()
	})
}

func TestPubSubDriver_getSyncedClient(t *testing.T) {

	t.Run("should return new client", func(t *testing.T) {

		client, err := sarama.NewClient(localBroker, nil)
		if err != nil {
			t.Errorf("%v", err)
		}

		d, _ := NewKafkaPubSubDriverFromClient(localTopic, &client)

		c := d.getSyncedClient()
		if c == nil {
			t.Error("PubSubDriver.getSyncedClient() did not return client")
		}

	})
}

func TestPubSubDriver_GetDriverFlags(t *testing.T) {

	t.Run("should return ConcurrentExecution flag", func(t *testing.T) {

		d := PubSubDriver{}

		for _, f := range d.GetDriverFlags() {

			if reflect.DeepEqual(f, brokerutil.RequiresConcurrentExecution) {
				return
			}
		}

		t.Error("PubSubDriver.GetDriverFlags() did not return ConcurrentExecution flag")
	})
}
