package defradb

import "github.com/sourcenetwork/defradb/event"

// CloseSubscription unsubscribes sub from bus and reads sub until the bus closes it. The read
// starts before Unsubscribe: the bus's one handler goroutine delivers events with a blocking send
// and takes the unsubscribe from the same command channel, so while a full sub goes unread the
// handler stalls, the command channel fills, and Unsubscribe blocks.
func CloseSubscription(bus event.Bus, sub event.Subscription) {
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range sub.Message() { //nolint:revive // the loop only drains sub
		}
	}()
	bus.Unsubscribe(sub)
	<-drained
}
