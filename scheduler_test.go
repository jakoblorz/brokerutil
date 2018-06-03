package brokerutil

import "testing"

func Test_scheduler_newScheduler(t *testing.T) {
	t.Run("should return new scheduler implementation", func(t *testing.T) {
		if newScheduler() == nil {
			t.Error("scheduler.newscheduler() did not return new subscriber instance")
		}
	})
}
