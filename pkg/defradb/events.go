package defradb

import "github.com/sourcenetwork/defradb/event"

// CloseSubscription unsubscribes sub from bus and reads sub until the bus closes it. The bus
// delivers each event with a blocking send, including events queued before the unsubscribe, so
// a full sub that nobody reads blocks every delivery and the bus's Close.
func CloseSubscription(bus event.Bus, sub event.Subscription) {
	bus.Unsubscribe(sub)
	for range sub.Message() { //nolint:revive // the loop only drains sub
	}
}
