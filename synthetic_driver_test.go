package brokerutil

import (
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
