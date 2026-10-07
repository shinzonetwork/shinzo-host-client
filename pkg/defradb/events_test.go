package defradb

import (
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/event"
	"github.com/stretchr/testify/require"
)

func TestCloseSubscription(t *testing.T) {
	bus := event.NewChannelBus(10, 1)
	sub, err := bus.Subscribe(event.UpdateName)
	require.NoError(t, err)
	// With room for one event and nobody reading, the second blocks the bus.
	for range 2 {
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
		t.Fatal("the bus did not close")
	}
}
