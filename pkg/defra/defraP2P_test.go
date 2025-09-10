package defra

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestP2PConnect(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0" // These are dynamic ports; a new and available port is chosen each time. This means that both defra instances have different ports. See: https://pkg.go.dev/net#Listen
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	indexerDefra := StartDefraInstance(t, ctx, options)
	defer indexerDefra.Close(ctx)

	go func() {
		err := indexer.StartIndexing("", defraUrl)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()
	defer indexer.StopIndexing()

	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	hostDefra := StartDefraInstance(t, ctx, options)
	defer hostDefra.Close(ctx)

	err := hostDefra.Peer.Connect(ctx, indexerDefra.Peer.PeerInfo())
	require.NoError(t, err)
}
