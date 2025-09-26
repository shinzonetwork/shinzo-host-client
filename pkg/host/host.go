package host

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	appDefra "github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/app-sdk/pkg/views"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = &config.Config{
	Shinzo: config.ShinzoConfig{
		MinimumAttestations: 1,
	},
	ShinzoAppConfig: defra.DefaultConfig,
}

var requiredPeers []string = []string{} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process

type Host struct {
	DefraNode              *node.Node
	HostedViews            []views.View // Todo I probably need to add some mutex to this as it is updated within threads
	webhookCleanupFunction func()
}

func StartHosting(cfg *config.Config) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true)

	defraNode, err := defra.StartDefraInstance(cfg.ShinzoAppConfig, &defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"})
	if err != nil {
		return nil, fmt.Errorf("Error starting defra instance: %v", err)
	}

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return nil, err
	}

	newHost := &Host{
		DefraNode:   defraNode,
		HostedViews: []views.View{},
	}

	if len(cfg.Shinzo.WebSocketUrl) > 0 {
		cancel, channel, err := shinzohub.StartEventSubscription(cfg.Shinzo.WebSocketUrl)

		cancellableContext, cancelEventHandler := context.WithCancel(context.Background())
		go func() { newHost.handleIncomingEvents(cancellableContext, channel) }()

		newHost.webhookCleanupFunction = func() {
			cancel()
			cancelEventHandler()
		}

		if err != nil {
			return nil, fmt.Errorf("Error starting event subscription: %v", err)
		}
	}

	// Todo connect to the indexers and sync primitives

	// Todo process dataviews

	return newHost, nil
}

func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	return h.DefraNode.Close(ctx)
}

// waitForDefraDB waits for a DefraDB instance to be ready by using app-sdk's QuerySingle
func waitForDefraDB(ctx context.Context, defraNode *node.Node) error {
	fmt.Println("Waiting for defra...")
	maxAttempts := 30

	// Simple query to check if the schema is ready
	query := `{ Block { __typename } }`

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := appDefra.QuerySingle[map[string]interface{}](ctx, defraNode, query)
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
				logger.Sugar.Debugf("Received new view: %+v", registeredEvent.View)
				err := registeredEvent.View.SubscribeTo(context.Background(), h.DefraNode)
				if err != nil {
					logger.Sugar.Errorf("Failed to subscribe to view %+v: %v", registeredEvent.View, err)
				} else {
					h.HostedViews = append(h.HostedViews, registeredEvent.View) // Todo we will eventually want to give hosts the option to opt in/out of hosting new views
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
