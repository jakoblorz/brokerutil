package singapoor

import (
	"errors"
	"testing"
	"time"

	"github.com/jakoblorz/singapoor/stream"
)

func Test_subscriberController_newSubscriberController(t *testing.T) {
	t.Run("should return new subscriberController with initialized subscribers map", func(t *testing.T) {
		if (newSubscriberController()).subscribers == nil {
			t.Error("subscriberController.newSubscriberController() did not return new subscriber instance with initialized subscribers map")
		}
	})
}

func Test_subscriberController_NotifySubscribers(t *testing.T) {
	t.Run("should notify all subscribers by invoking callback funcs", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		var wasAInvoked bool
		var wasBInvoked bool
		var sig = make(chan error)
		var fna = func(msg stream.Message) error {
			wasAInvoked = true
			return nil
		}

		var fnb = func(msg stream.Message) error {
			wasBInvoked = true
			return nil
		}

		subscriberCtrl.subscribers["subscriber-a"] = subscriberWrapper{
			fn:  fna,
			sig: sig,
		}

		subscriberCtrl.subscribers["subscriber-b"] = subscriberWrapper{
			fn:  fnb,
			sig: sig,
		}

		subscriberCtrl.NotifySubscribers(stream.Message("test message"))

		if !(wasAInvoked && wasBInvoked) {
			t.Error("subscriberController.NotifySubscribers() did not notify all subscribers")
		}
	})

	t.Run("should notify all subscribers and pipe errors back", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		var sig = make(chan error, 2)
		var failErr = errors.New("test error")
		var failFn = func(msg stream.Message) error {
			return failErr
		}

		subscriberCtrl.subscribers["subscriber-a"] = subscriberWrapper{
			fn:  failFn,
			sig: sig,
		}

		subscriberCtrl.subscribers["subscriber-b"] = subscriberWrapper{
			fn:  failFn,
			sig: sig,
		}

		subscriberCtrl.NotifySubscribers(stream.Message("test message"))

		<-sig
		<-sig

		remainingSubCount := len(subscriberCtrl.subscribers)

		if remainingSubCount != 0 {
			t.Errorf("subscriberController.NotifySubscribers() did not remove erroring subscribers: found %d registered subscribers", remainingSubCount)
		}
	})
}

func Test_subscriberController_SubscribeAsync(t *testing.T) {

	subscriberCtrl := newSubscriberController()
	var subscriberCountBefore = len(subscriberCtrl.subscribers)

	e, i := subscriberCtrl.SubscribeAsync(func(msg stream.Message) error {
		return nil
	})

	subscriberCountAfter := len(subscriberCtrl.subscribers)

	t.Run("should have increased subscriber count", func(t *testing.T) {
		if subscriberCountAfter <= subscriberCountBefore {
			t.Errorf("subscriberController.SubscribeAsync() did not increase subscriber count: before = %d after = %d", subscriberCountBefore, subscriberCountAfter)
		}
	})

	t.Run("should have returned initialized channel", func(t *testing.T) {
		if e == nil {
			t.Error("subscriberController.SubscribeAsync() did not return initialized channel: channel is nil")
		}
	})

	t.Run("should have returned initialized subscriber identifier", func(t *testing.T) {
		if i == "" {
			t.Error("subscriberController.SubscribeAsync() did not return initialized subscriber identifier: subscriber identifier is empty")
		}
	})
}

func Test_subscriberController_SubscribeSync(t *testing.T) {

	subscriberCtrl := newSubscriberController()
	var subscriberCountBefore = len(subscriberCtrl.subscribers)
	var subscriberCountAfter int

	go func() {

		// wait some time to then notify of a message which lets
		// the callback function to fail
		time.Sleep(10 * time.Millisecond)

		subscriberCountAfter = len(subscriberCtrl.subscribers)

		subscriberCtrl.NotifySubscribers(stream.Message("test message"))
	}()

	subscriberCtrl.SubscribeSync(func(msg stream.Message) error {
		return errors.New("test error")
	})

	t.Run("should have increased subscriber count", func(t *testing.T) {
		if subscriberCountAfter <= subscriberCountBefore {
			t.Errorf("subscriberController.SubscribeAsync() did not increase subscriber count: before = %d after = %d", subscriberCountBefore, subscriberCountAfter)
		}
	})

}

func Test_subscriberController_Unsubscribe(t *testing.T) {

	t.Run("should remove subscriber from map", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		subscriberID := SubscriberIdentifier("unsubscribe-test")

		var sig = make(chan error, 1)

		subscriberCtrl.subscribers[subscriberID] = subscriberWrapper{
			sig: sig,
		}

		var subscriberBeforeCount = len(subscriberCtrl.subscribers)

		subscriberCtrl.Unsubscribe(subscriberID)

		var subscriberAfterCount = len(subscriberCtrl.subscribers)

		if !(subscriberAfterCount < subscriberBeforeCount) {
			t.Error("subscriberController.Unsubscribe() did not remove subscriber from map")
		}
	})

	t.Run("should send nil on subscribers error channel", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		subscriberID := SubscriberIdentifier("unsubscribe-test")

		var sig = make(chan error, 1)

		subscriberCtrl.subscribers[subscriberID] = subscriberWrapper{
			sig: sig,
		}

		subscriberCtrl.Unsubscribe(subscriberID)

		err := <-sig
		if err != nil {
			t.Errorf("subscriberController.Unsubscribe() did not send nil on error channel: sent value = %v", err)
		}
	})
}

func Test_subscriberController_UnsubscribeAll(t *testing.T) {

	t.Run("should remove all subscribers from map", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		var subSig = make(chan error, 2)
		var subFn = func(msg stream.Message) error {
			return nil
		}

		var subIDA = SubscriberIdentifier("sub-id-a")
		var subIDB = SubscriberIdentifier("sub-id-b")

		subscriberCtrl.subscribers[subIDA] = subscriberWrapper{
			fn:  subFn,
			sig: subSig,
		}

		subscriberCtrl.subscribers[subIDB] = subscriberWrapper{
			fn:  subFn,
			sig: subSig,
		}

		subscriberCtrl.UnsubscribeAll()

		var subscriberAfterCount = len(subscriberCtrl.subscribers)

		if subscriberAfterCount != 0 {
			t.Error("subscriberController.UnsubscribeAll() did not remove all subscribers from map")
		}
	})

	t.Run("should send nil on all subscribers error channels", func(t *testing.T) {

		subscriberCtrl := newSubscriberController()

		var sig = make(chan error, 2)
		var subFn = func(msg stream.Message) error {
			return nil
		}

		var subIDA = SubscriberIdentifier("sub-id-a")
		var subIDB = SubscriberIdentifier("sub-id-b")

		subscriberCtrl.subscribers[subIDA] = subscriberWrapper{
			fn:  subFn,
			sig: sig,
		}

		subscriberCtrl.subscribers[subIDB] = subscriberWrapper{
			fn:  subFn,
			sig: sig,
		}

		subscriberCtrl.UnsubscribeAll()

		errChanMsgA := <-sig
		errChanMsgB := <-sig

		if !(errChanMsgA == nil && errChanMsgB == nil) {
			t.Error("subscriberController.UnsubscribeAll() did not send nil on all subscribers error channels")
		}
	})
}
