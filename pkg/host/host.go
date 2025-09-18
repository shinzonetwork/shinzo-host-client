package host

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/indexer/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = &config.Config{
	DefraDB: config.DefraDBConfig{
		Url:           "http://localhost:9181",
		KeyringSecret: os.Getenv("DEFRA_KEYRING_SECRET"),
		P2P: config.DefraP2PConfig{
			BootstrapPeers: requiredPeers,
			ListenAddr:     defaultListenAddress,
		},
		Store: config.DefraStoreConfig{
			Path: ".defra",
		},
	},
	ShinzoHub: config.ShinzoHubConfig{
		RPCUrl: defaultShinzoHubRpcUrl,
	},
	Logger: config.LoggerConfig{
		Development: false,
	},
}

var requiredPeers []string = []string{} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process

const defaultListenAddress string = "/ip4/127.0.0.1/tcp/9171"
const defaultShinzoHubRpcUrl string = ""

func StartHosting(defraStarted bool, cfg *config.Config) error {
	ctx := context.Background()

	if cfg == nil {
		cfg = DefaultConfig
	}
	cfg.DefraDB.P2P.BootstrapPeers = append(cfg.DefraDB.P2P.BootstrapPeers, requiredPeers...)

	logger.Init(cfg.Logger.Development)

	if !defraStarted {
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(cfg.DefraDB.Store.Path),
			http.WithAddress(strings.Replace(cfg.DefraDB.Url, "http://localhost", "127.0.0.1", 1)),
			netConfig.WithBootstrapPeers(cfg.DefraDB.P2P.BootstrapPeers...),
		}
		listenAddress := cfg.DefraDB.P2P.ListenAddr
		if len(listenAddress) > 0 {
			options = append(options, netConfig.WithListenAddresses(listenAddress))
		}

		defraNode, err := node.New(ctx, options...)
		if err != nil {
			return fmt.Errorf("Failed to create defra node %v: ", err)
		}

		err = defraNode.Start(ctx)
		if err != nil {
			return fmt.Errorf("Failed to start defra node %v: ", err)
		}
		defer defraNode.Close(ctx)

		err = applySchema(ctx, defraNode)
		if err != nil && !strings.HasPrefix(err.Error(), "collection already exists") { // Todo we are swallowing this error for now, but we should investigate how we update the schemas - do we need to not swallow this error?
			return fmt.Errorf("Failed to apply schema to defra node: %v", err)
		}
	}

	err := defra.WaitForDefraDB(cfg.DefraDB.Url)
	if err != nil {
		return err
	}

	// _, _, err = shinzohub.StartEventSubscription(cfg.ShinzoHub.RPCUrl) // Todo replace with below once host is doing something
	// // closeWebhookFunction, err := shinzohub.StartEventSubscription(cfg.ShinzoHub.RPCUrl)
	// // defer closeWebhookFunction()
	// if err != nil {
	// 	return fmt.Errorf("Error starting event subscription: %v", err)
	// }

	// Todo connect to the indexers and sync primitives

	// Todo process dataviews

	return nil
}

// Todo - we'll have to update this to include the policy id (which we should get back from ShinzoHub during registration)
func applySchema(ctx context.Context, defraNode *node.Node) error {
	logger.Sugar.Debug("Applying schema...")

	// Try different possible paths for the schema file
	possiblePaths := []string{
		"schema/schema.graphql",       // From project root
		"../schema/schema.graphql",    // From bin/ directory
		"../../schema/schema.graphql", // From pkg/host/ directory - test context
	}

	var schemaPath string
	var err error
	for _, path := range possiblePaths {
		if _, err = os.Stat(path); err == nil {
			schemaPath = path
			break
		}
	}

	if schemaPath == "" {
		return fmt.Errorf("Failed to find schema file in any of the expected locations: %v", possiblePaths)
	}

	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("Failed to read schema file: %v", err)
	}

	_, err = defraNode.DB.AddSchema(ctx, string(schema))
	return err
}
