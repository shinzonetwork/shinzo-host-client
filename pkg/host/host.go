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

	// "github.com/shinzonetwork/shinzo-host-client/pkg/view" // COMMENTED: Focusing on event-driven attestations
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
	DefraNode      *node.Node
	NetworkHandler *defra.NetworkHandler // P2P network control
	// COMMENTED: View-related fields - focusing on pure event-driven attestation system
	// HostedViews            []view.View // Todo I probably need to add some mutex to this as it is updated within threads
	webhookCleanupFunction func()
	eventSubscription      shinzohub.EventSubscription
	LensRegistryPath       string
	processingCancel       context.CancelFunc // For canceling the event processing goroutine
	playgroundServer       *http.Server       // Playground HTTP server (if enabled)
	config                 *config.Config     // Host configuration including StartHeight

	// EVENT-DRIVEN ATTESTATION SYSTEM: Only track attestation processing
	attestationRangeTracker *BlockRangeTracker
	processingPipeline      *ProcessingPipeline // Complete message processing pipeline
	// COMMENTED: View processing will be redesigned later for efficiency
	// viewRangeFinder         *ViewRangeFinder              // New intelligent view range finder
	// viewRangeTrackers       map[string]*BlockRangeTracker // Legacy block range trackers (deprecated)
	mostRecentBlockReceived uint64 // This keeps track of the most recent block number received - useful for debugging and confirming Host is receiving blocks from Indexers
}

func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg, &shinzohub.RealEventSubscription{})
}

func StartHostingWithEventSubscription(cfg *config.Config, eventSub shinzohub.EventSubscription) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true)

	defraNode, networkHandler, err := defra.StartDefraInstance(cfg.ShinzoAppConfig,
		defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()),
		"Block", "Transaction", "AccessListEntry", "Log")
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %v", err)
	}

	// Store network handler for P2P control
	// Note: P2P will be started after gap processing completes

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
		DefraNode:      defraNode,
		NetworkHandler: networkHandler,
		// COMMENTED: View initialization - pure event-driven attestation focus
		// HostedViews:             []view.View{},
		webhookCleanupFunction:  func() {},
		eventSubscription:       eventSub,
		LensRegistryPath:        cfg.HostConfig.LensRegistryPath,
		processingCancel:        func() {},
		playgroundServer:        playgroundServer,
		config:                  cfg,
		attestationRangeTracker: NewBlockRangeTracker(),
		// COMMENTED: View processing will be redesigned for efficiency
		// viewRangeFinder:         NewViewRangeFinder(defraNode, logger.Sugar),
		// viewRangeTrackers:       make(map[string]*BlockRangeTracker),
	}

	cacheSize := cfg.Shinzo.CacheSize
	if cacheSize <= 0 {
		cacheSize = 10000 // Default cache size
	}

	queueSize := cfg.Shinzo.CacheQueueSize
	if queueSize <= 0 {
		queueSize = 1000 // Default queue size
	}

	workerCount := cfg.Shinzo.WorkerCount
	if workerCount <= 0 {
		workerCount = 4 // Default worker count
	}

	cacheMaxAge := time.Duration(cfg.Shinzo.CacheMaxAgeSeconds) * time.Second
	if cacheMaxAge <= 0 {
		cacheMaxAge = 5 * time.Minute // Default 5 minutes
	}

	// Create simplified processing pipeline (no cache)
	newHost.processingPipeline = NewProcessingPipeline(
		context.Background(), newHost, 0, cacheMaxAge, cacheSize, queueSize, workerCount,
	)

	logger.Sugar.Infof("🔧 Processing pipeline initialized: cache=%d, queue=%d, workers=%d",
		cacheSize, queueSize, workerCount)

	// START THE PROCESSING PIPELINE (CRITICAL!)
	newHost.processingPipeline.Start()

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

	// Start the event-driven attestation processing system
	processingCtx, processingCancel := context.WithCancel(context.Background())
	newHost.processingCancel = processingCancel
	go newHost.processAttestationEventsWithSubscription(processingCtx)

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

	// Stop the processing pipeline
	if h.processingPipeline != nil {
		h.processingPipeline.Stop()
	}

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

			// COMMENTED: View event handling - focusing on pure attestation events
			// if registeredEvent, ok := event.(*shinzohub.ViewRegisteredEvent); ok {
			// 	logger.Sugar.Debugf("Received new view: %s", registeredEvent.View.Name)
			// 	err := h.PrepareView(ctx, registeredEvent.View)
			// 	if err != nil {
			// 		logger.Sugar.Errorf("Failed to prepare view: %v", err)
			// 	} else {
			// 		h.HostedViews = append(h.HostedViews, registeredEvent.View)
			// 		h.viewRangeTrackers[registeredEvent.View.Name] = NewBlockRangeTracker()
			// 	}
			// } else {
			logger.Sugar.Debugf("Received event (view processing disabled): %+v", event)
			// }
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
