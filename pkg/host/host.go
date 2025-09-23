package host

import (
	"context"
	"fmt"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	appDefra "github.com/shinzonetwork/app-sdk/pkg/defra"
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

func StartHosting(cfg *config.Config) error {
	if cfg == nil {
		cfg = DefaultConfig
	}

	defraNode, err := defra.StartDefraInstance(cfg.ShinzoAppConfig, &defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"})
	if err != nil {
		return fmt.Errorf("Error starting defra instance: %v", err)
	}

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return err
	}

	if len(cfg.Shinzo.WebSocketUrl) > 0 {
		_, _, err = shinzohub.StartEventSubscription(cfg.Shinzo.WebSocketUrl) // Todo replace with below once host is doing something
		// closeWebhookFunction, err := shinzohub.StartEventSubscription(cfg.ShinzoHub.RPCUrl)
		// defer closeWebhookFunction()
		if err != nil {
			return fmt.Errorf("Error starting event subscription: %v", err)
		}
	}

	// Todo connect to the indexers and sync primitives

	// Todo process dataviews

	return nil
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
