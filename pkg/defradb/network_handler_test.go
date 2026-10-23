package defradb

import (
	"context"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/event"
	"github.com/stretchr/testify/require"
)

func TestStopNetwork_ClosesNoPeersSubscription(t *testing.T) {
	defraNode, err := StartDefraInstanceWithTestConfig(t, DefaultConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	networkHandler := NewNetworkHandler(defraNode, DefaultConfig)
	require.NoError(t, networkHandler.StartNetwork())
	require.NoError(t, networkHandler.StopNetwork())

	// More events than the bus holds for one subscriber, so a subscription left open fills and
	// blocks the bus.
	for range 1000 {
		defraNode.DB.Events().Publish(event.NewMessage(event.P2PNoPeersName, event.P2PNoPeers{}))
	}
	closed := make(chan error, 1)
	go func() { closed <- defraNode.Close(context.Background()) }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the node did not close")
	}
}
