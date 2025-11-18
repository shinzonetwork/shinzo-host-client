package host

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	indexerschema "github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/stack"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
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

var requiredPeers []string = []string{} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process

type Host struct {
	DefraNode              *node.Node
	HostedViews            []view.View // Todo I probably need to add some mutex to this as it is updated within threads
	webhookCleanupFunction func()
	eventSubscription      shinzohub.EventSubscription
	LensRegistryPath       string
	processingCancel       context.CancelFunc // For canceling the block processing goroutine
	blockMonitorCancel     context.CancelFunc // For canceling the block monitoring goroutine

	// These counters keep track of the block number "time stamp" that we last processed the attestations or view on
	attestationProcessedBlocks *stack.Stack[uint64]
	viewProcessedBlocks        map[string]*stack.Stack[uint64] // The block numbers processed mapped to their respective view name
	mostRecentBlockReceived    uint64                          // This keeps track of the most recent block number received - useful for debugging and confirming Host is receiving blocks from Indexers
}

func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg, &shinzohub.RealEventSubscription{})
}

func StartHostingWithEventSubscription(cfg *config.Config, eventSub shinzohub.EventSubscription) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true)

	// Get the base schema and append attestation record schemas for all primitives
	baseSchema := indexerschema.GetSchema()
	modifiedSchema, err := appendAttestationRecordSchemas(baseSchema)
	if err != nil {
		return nil, fmt.Errorf("error appending attestation record schemas: %v", err)
	}

	defraNode, err := defra.StartDefraInstance(cfg.ShinzoAppConfig,
		defra.NewSchemaApplierFromProvidedSchema(modifiedSchema),
		"Block", "Transaction", "AccessListEntry", "Log")
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %v", err)
	}

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return nil, err
	}

	// Add attestation record collections as P2P collections
	primitives, err := extractPrimitiveTypes(baseSchema)
	if err != nil {
		return nil, fmt.Errorf("error extracting primitive types: %v", err)
	}
	
	if len(primitives) == 0 {
		return nil, fmt.Errorf("no primitive types found in schema")
	}
	
	attestationCollectionNames := make([]string, 0, len(primitives))
	for _, primitive := range primitives {
		attestationCollectionNames = append(attestationCollectionNames, fmt.Sprintf("AttestationRecord_%s", primitive))
	}
	
	err = defraNode.DB.AddP2PCollections(ctx, attestationCollectionNames...)
	if err != nil {
		return nil, fmt.Errorf("error adding attestation record collections as P2P collections: %v", err)
	}

	newHost := &Host{
		DefraNode:                  defraNode,
		HostedViews:                []view.View{},
		webhookCleanupFunction:     func() {},
		eventSubscription:          eventSub,
		LensRegistryPath:           cfg.HostConfig.LensRegistryPath,
		processingCancel:           func() {},
		blockMonitorCancel:         func() {},
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
	go newHost.processBlocks(processingCtx)

	// Start the block monitoring goroutine
	blockMonitorCtx, blockMonitorCancel := context.WithCancel(context.Background())
	newHost.blockMonitorCancel = blockMonitorCancel
	go newHost.monitorHighestBlockNumber(blockMonitorCtx)

	return newHost, nil
}

func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	h.processingCancel()   // Stop the block processing goroutine
	h.blockMonitorCancel() // Stop the block monitoring goroutine
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

func (h *Host) monitorHighestBlockNumber(ctx context.Context) {
	logger.Sugar.Info("Monitoring for new blocks...")
	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Block monitoring stopped due to context cancellation")
			return
		case <-ticker.C:
			highestBlockNumber, err := h.getCurrentBlockNumber(ctx)
			if err != nil {
				logger.Sugar.Errorf("Error fetching highest block number: %v", err)
				continue
			}

			if highestBlockNumber > h.mostRecentBlockReceived {
				logger.Sugar.Infof("Highest block number in defraNode: %d", highestBlockNumber)
				h.mostRecentBlockReceived = highestBlockNumber
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
