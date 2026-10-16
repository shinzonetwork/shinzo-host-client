package defradb

import (
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/event"
	"github.com/stretchr/testify/require"
)

func TestCloseSubscription(t *testing.T) {
	bus := event.NewChannelBus(1, 1)
	sub, err := bus.Subscribe(event.UpdateName)
	require.NoError(t, err)
	// The first event fills sub, the bus's handler blocks delivering the second, and the third
	// fills the one-slot command channel, so Unsubscribe can only send once sub is being read.
	for range 3 {
		bus.Publish(event.NewMessage(event.UpdateName, event.Update{}))
	}

	closed := make(chan struct{})
	go func() {
		CloseSubscription(bus, sub)
		bus.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("the subscription did not close")
	}
}
