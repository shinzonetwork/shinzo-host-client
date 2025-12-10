package host

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	indexerschema "github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/config"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	playgroundserver "github.com/shinzonetwork/shinzo-host-client/pkg/playground"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = func() *config.Config {
	cfg := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		ShinzoAppConfig: defra.DefaultConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: "./.lens",
		},
	}
	// Ensure keyring secret is set for tests and use temp directory to avoid conflicts
	if cfg.ShinzoAppConfig.DefraDB.KeyringSecret == "" {
		cfg.ShinzoAppConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
		// Use temp directory for test data to avoid keyring conflicts
		cfg.ShinzoAppConfig.DefraDB.Store.Path = "/tmp/defra-test-default"
	}
	return cfg
}()

var requiredPeers []string = []string{} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process

type Host struct {
	DefraNode              *node.Node
	HostedViews            []view.View // Todo I probably need to add some mutex to this as it is updated within threads
	webhookCleanupFunction func()
	eventSubscription      shinzohub.EventSubscription
	LensRegistryPath       string
	processingCancel       context.CancelFunc // For canceling the block processing goroutine
	playgroundServer       *http.Server       // Playground HTTP server (if enabled)
	config                 *config.Config     // Host configuration including StartHeight

	// These trackers keep track of processed block ranges for attestations and views
	attestationRangeTracker *BlockRangeTracker
	viewRangeFinder         *ViewRangeFinder              // New intelligent view range finder
	viewRangeTrackers       map[string]*BlockRangeTracker // Legacy block range trackers (deprecated)
	mostRecentBlockReceived uint64                        // This keeps track of the most recent block number received - useful for debugging and confirming Host is receiving blocks from Indexers
}

func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg, &shinzohub.RealEventSubscription{})
}

func StartHostingWithEventSubscription(cfg *config.Config, eventSub shinzohub.EventSubscription) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true)

	defraNode, err := defra.StartDefraInstance(cfg.ShinzoAppConfig,
		defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()),
		"Block", "Transaction", "AccessListEntry", "Log")
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %v", err)
	}

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return nil, err
	}

	// Initialize attestation record schemas for all document types
	err = initializeAttestationSchemas(ctx, defraNode)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize attestation schemas: %w", err)
	}

	// Log API URL
	if defraNode.APIURL != "" {
		fmt.Printf("🚀 Host HTTP API available at %s\n", defraNode.APIURL)
		fmt.Printf("📊 GraphQL endpoint at %s/api/v0/graphql\n", defraNode.APIURL)
	}

	// Start playground server if enabled
	// We start our own HTTP server that serves the playground UI and proxies
	// API requests to defradb's API server
	var playgroundServer *http.Server
	if isPlaygroundEnabled() && defraNode.APIURL != "" {
		playgroundHandler, err := playgroundserver.NewServer(defraNode.APIURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create playground server: %v", err)
		}

		// Start playground server on a different port (defradb port + 1)
		// Parse the defradb URL to get the port and increment it
		playgroundAddr := cfg.ShinzoAppConfig.DefraDB.Url
		if defraNode.APIURL != "" {
			playgroundAddr = defraNode.APIURL
		}
		playgroundAddr, err = incrementPort(playgroundAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve defra url: %w", err)
		}

		playgroundServer = &http.Server{
			Addr:    playgroundAddr,
			Handler: playgroundHandler,
		}

		go func() {
			if err := playgroundServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Sugar.Errorf("Playground server error: %v", err)
			}
		}()

		fmt.Printf("🧪 GraphQL Playground available at http://%s\n", playgroundAddr)
		fmt.Printf("   (Playground proxies API requests to defradb at %s)\n", defraNode.APIURL)
	}

	newHost := &Host{
		DefraNode:               defraNode,
		HostedViews:             []view.View{},
		webhookCleanupFunction:  func() {},
		eventSubscription:       eventSub,
		LensRegistryPath:        cfg.HostConfig.LensRegistryPath,
		processingCancel:        func() {},
		playgroundServer:        playgroundServer,
		config:                  cfg,
		attestationRangeTracker: NewBlockRangeTracker(),
		viewRangeFinder:         NewViewRangeFinder(defraNode, logger.Sugar),
		viewRangeTrackers:       make(map[string]*BlockRangeTracker),
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

	// Start the integrated block processing goroutine with BlockRangeTracker
	processingCtx, processingCancel := context.WithCancel(context.Background())
	newHost.processingCancel = processingCancel
	go newHost.processAllViewsWithSubscription(processingCtx)

	// Block monitoring is now handled by the event-driven processAllViews goroutine
	// No separate monitoring goroutine needed

	return newHost, nil
}

func incrementPort(apiURL string) (string, error) {
	if !strings.HasPrefix(apiURL, "http://") && !strings.HasPrefix(apiURL, "https://") {
		apiURL = "http://" + apiURL
	}
	parsed, err := url.Parse(apiURL)
	if err == nil {
		host := parsed.Host
		if host == "" {
			host = parsed.Path
		}
		// Split host:port
		parts := strings.Split(host, ":")
		if len(parts) == 2 {
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s:%d", parts[0], port+1), nil
		} else if len(parts) == 1 {
			return "", fmt.Errorf("No port found")
		}
	}
	return "", err
}

func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	h.processingCancel() // Stop the block processing goroutine (now includes block monitoring)

	// Shutdown playground server if it exists
	if h.playgroundServer != nil {
		if err := h.playgroundServer.Shutdown(ctx); err != nil {
			logger.Sugar.Errorf("Error shutting down playground server: %v", err)
		}
	}

	// Force shutdown with timeout to prevent hanging
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- h.DefraNode.Close(shutdownCtx)
	}()

	select {
	case err := <-done:
		return err
	case <-shutdownCtx.Done():
		logger.Sugar.Warn("DefraDB shutdown timed out, forcing exit")
		return nil // Don't return error for timeout in tests
	}
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
					h.viewRangeTrackers[registeredEvent.View.Name] = NewBlockRangeTracker()
				}
			} else {
				logger.Sugar.Errorf("Received unknown event: %+v", event)
			}
		}
	}
}

// monitorHighestBlockNumber has been removed - block monitoring is now handled
// by the event-driven processAllViews goroutine for better efficiency

func StartHostingWithTestConfig(t *testing.T) (*Host, error) {
	testConfig := DefaultConfig
	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"
	return StartHosting(testConfig)
}

// initializeAttestationSchemas creates attestation record collections for all document types
func initializeAttestationSchemas(ctx context.Context, defraNode *node.Node) error {
	// Document types that need attestation record collections
	documentTypes := []string{"Block", "Transaction", "Log", "AccessList"}

	for _, docType := range documentTypes {
		collectionName := fmt.Sprintf("Document_%s", docType)

		err := hostAttestation.AddAttestationRecordCollection(ctx, defraNode, collectionName)
		if err != nil && !strings.Contains(err.Error(), "collection already exists") {
			return fmt.Errorf("failed to create attestation collection for %s: %w", docType, err)
		}

		logger.Sugar.Infof("✅ Attestation collection AttestationRecord_%s initialized", collectionName)
	}

	return nil
}

// isPlaygroundEnabled checks if the playground is enabled at build time.
// This function will only return true when the code is built with the hostplayground tag.
func isPlaygroundEnabled() bool {
	// This will be true only when built with -tags hostplayground
	// We use a build tag to conditionally compile this
	return playgroundEnabled
}
