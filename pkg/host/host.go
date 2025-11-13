package host

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/shinzonetwork/host/pkg/stack"
	"github.com/shinzonetwork/host/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = &config.Config{
	Shinzo: config.ShinzoConfig{
		MinimumAttestations: 1,
	},
	ShinzoAppConfig: defra.DefaultConfig,
	HostConfig: config.HostConfig{
		LensRegistryPath: "./.lens",
	},
}

var requiredPeers []string = []string{"/ip4/136.115.148.56/tcp/9171/p2p/12D3KooWT2wVhxc7ySePpFoomm1SengPYdAa1P6iUiAypN5TRijD"} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process

type Host struct {
	DefraNode              *node.Node
	HostedViews            []view.View // Todo I probably need to add some mutex to this as it is updated within threads
	webhookCleanupFunction func()
	eventSubscription      shinzohub.EventSubscription
	LensRegistryPath       string
	processingCancel       context.CancelFunc // For canceling the block processing goroutine

	// These counters keep track of the block number "time stamp" that we last processed the attestations or view on
	attestationProcessedBlocks *stack.Stack[uint64]
	viewProcessedBlocks        map[string]*stack.Stack[uint64] // The block numbers processed mapped to their respective view name
}

func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg, &shinzohub.RealEventSubscription{})
}

func StartHostingWithEventSubscription(cfg *config.Config, eventSub shinzohub.EventSubscription) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true)

	cfg.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers = append(cfg.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers, requiredPeers...)

	defraNode, err := defra.StartDefraInstance(cfg.ShinzoAppConfig,
		&defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"},
		"Block", "Transaction", "AccessListEntry", "Log")
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %v", err)
	}

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return nil, err
	}

	newHost := &Host{
		DefraNode:                  defraNode,
		HostedViews:                []view.View{},
		webhookCleanupFunction:     func() {},
		eventSubscription:          eventSub,
		LensRegistryPath:           cfg.HostConfig.LensRegistryPath,
		processingCancel:           func() {},
		attestationProcessedBlocks: stack.New[uint64](),
		viewProcessedBlocks:        map[string]*stack.Stack[uint64]{},
	}

	if len(cfg.Shinzo.WebSocketUrl) > 0 {
		cancel, channel, err := eventSub.StartEventSubscription(cfg.Shinzo.WebSocketUrl)

		cancellableContext, cancelEventHandler := context.WithCancel(context.Background())
		go func() { newHost.handleIncomingEvents(cancellableContext, channel) }()

		newHost.webhookCleanupFunction = func() {
			cancel()
			cancelEventHandler()
		}

		if err != nil {
			return nil, fmt.Errorf("error starting event subscription: %v", err)
		}
	}

	// Start the block processing goroutine
	processingCtx, processingCancel := context.WithCancel(context.Background())
	newHost.processingCancel = processingCancel
	go newHost.processAllViews(processingCtx)

	return newHost, nil
}

func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	h.processingCancel() // Stop the block processing goroutine
	return h.DefraNode.Close(ctx)
}

// waitForDefraDB waits for a DefraDB instance to be ready by using app-sdk's QuerySingle
func waitForDefraDB(ctx context.Context, defraNode *node.Node) error {
	fmt.Println("Waiting for defra...")
	maxAttempts := 30

	// Simple query to check if the schema is ready
	query := `{ Block { __typename } }`

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := defra.QuerySingle[map[string]interface{}](ctx, defraNode, query)
		if err == nil {
			fmt.Println("Defra is responsive!")
			return nil
		}

		if attempt < maxAttempts {
			fmt.Printf("Attempt %d failed... Trying again\n", attempt)
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("DefraDB failed to become ready after %d retry attempts", maxAttempts)
}

func (h *Host) handleIncomingEvents(ctx context.Context, channel <-chan shinzohub.ShinzoEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-channel:
			if !ok {
				return // Event channel closed
			}

			if registeredEvent, ok := event.(*shinzohub.ViewRegisteredEvent); ok {
				logger.Sugar.Debugf("Received new view: %s", registeredEvent.View.Name)
				err := h.PrepareView(ctx, registeredEvent.View)
				if err != nil {
					logger.Sugar.Errorf("Failed to prepare view: %v", err)
				} else {
					h.HostedViews = append(h.HostedViews, registeredEvent.View) // Todo we will eventually want to give hosts the option to opt in/out of hosting new views
					h.viewProcessedBlocks[registeredEvent.View.Name] = &stack.Stack[uint64]{}
				}
			} else {
				logger.Sugar.Errorf("Received unknown event: %+v", event)
			}
		}
	}
}

func StartHostingWithTestConfig(t *testing.T) (*Host, error) {
	testConfig := DefaultConfig
	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"
	return StartHosting(testConfig)
}
