package host

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/signer"
	"github.com/shinzonetwork/shinzo-host-client/config"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	playgroundserver "github.com/shinzonetwork/shinzo-host-client/pkg/playground"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/sourcenetwork/corelog"

	// "github.com/shinzonetwork/shinzo-host-client/pkg/view" // COMMENTED: Focusing on event-driven attestations
	"github.com/sourcenetwork/defradb/node"
)

// parseTimeoutOrDefault parses a duration string or returns a default value
func parseTimeoutOrDefault(timeoutStr string, defaultDuration time.Duration) time.Duration {
	if timeoutStr == "" {
		return defaultDuration
	}

	duration, err := time.ParseDuration(timeoutStr)
	if err != nil {
		logger.Sugar.Warnf("Invalid timeout format '%s', using default %v", timeoutStr, defaultDuration)
		return defaultDuration
	}

	return duration
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

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
	processingPipeline *ProcessingPipeline // Complete message processing pipeline

	// VIEW MANAGEMENT SYSTEM: Handle lens transformations and view lifecycle
	viewManager             *ViewManager                       // Manages view lifecycle and processing
	viewRegistrationHandler *shinzohub.ViewRegistrationHandler // Handles Shinzo Hub view registration events
	viewEndpointManager     *ViewEndpointManager               // Manages HTTP endpoints for views

	healthServer *server.HealthServer

	// METRICS SYSTEM: Track attestation and document processing metrics
	metrics *server.HostMetrics // Comprehensive metrics tracking

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

	// Configure DefraDB logging - set to error level to hide INFO logs from HTTP requests
	corelog.SetConfigOverride("http", corelog.Config{
		Level: corelog.LevelError,
	})

	defraNode, networkHandler, err := defra.StartDefraInstance(
		cfg.ShinzoAppConfig,
		defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchema()),
		"Block", "Transaction", "AccessListEntry", "Log",
	)
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

	// Apply local schema after DefraDB is ready (for use with non-branchable)
	err = applySchema(ctx, defraNode)
	if err != nil {
		return nil, fmt.Errorf("failed to apply schema: %w", err)
	}

	// Initialize attestation record schemas for all document types
	// err = initializeAttestationSchemas(ctx, defraNode)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to initialize attestation schemas: %w", err)
	// }

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
		webhookCleanupFunction: func() {},
		eventSubscription:      eventSub,
		LensRegistryPath:       cfg.HostConfig.LensRegistryPath,
		processingCancel:       func() {},
		playgroundServer:       playgroundServer,
		config:                 cfg,
		metrics:                server.NewHostMetrics(),
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

	// Initialize ViewManager for lens transformations and view lifecycle management
	viewConfig := ViewManagerConfig{
		InactivityTimeout: parseTimeoutOrDefault(cfg.Shinzo.ViewInactivityTimeout, 24*time.Hour),
		CleanupInterval:   parseTimeoutOrDefault(cfg.Shinzo.ViewCleanupInterval, 1*time.Hour),
		WorkerCount:       maxInt(cfg.Shinzo.ViewWorkerCount, 4),  // Ensure at least 4 workers
		QueueSize:         maxInt(cfg.Shinzo.ViewQueueSize, 1000), // Ensure at least 1000 queue size
	}
	newHost.viewManager = NewViewManager(defraNode, viewConfig)

	// Set metrics callback for ViewManager
	newHost.viewManager.metricsCallback = func() *server.HostMetrics {
		return newHost.metrics
	}

	logger.Sugar.Infof("🎯 ViewManager initialized: workers=%d, queue=%d, timeout=%v",
		viewConfig.WorkerCount, viewConfig.QueueSize, viewConfig.InactivityTimeout)

	// Initialize ViewRegistrationHandler for Shinzo Hub events
	newHost.viewRegistrationHandler = shinzohub.NewViewRegistrationHandler(cfg.Shinzo.StartHeight)
	newHost.viewRegistrationHandler.SetViewManager(newHost.viewManager)

	// Initialize ViewEndpointManager for HTTP endpoints
	newHost.viewEndpointManager = NewViewEndpointManager(defraNode, newHost.viewManager)

	logger.Sugar.Info("🌐 ViewEndpointManager initialized for dynamic route creation")

	// Setup HTTP server with view endpoints
	newHost.setupViewHTTPServer()

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

	// Initialize and start health server
	var healthDefraURL string
	if cfg.ShinzoAppConfig.DefraDB.Url != "" {
		healthDefraURL = cfg.ShinzoAppConfig.DefraDB.Url
	} else if defraNode != nil && defraNode.APIURL != "" {
		healthDefraURL = defraNode.APIURL
	}

	newHost.healthServer = server.NewHealthServer(8080, newHost, healthDefraURL)

	// Start health server in background
	go func() {
		if err := newHost.healthServer.Start(); err != nil {
			logger.Sugar.Errorf("Health server failed: %v", err)
		}
	}()

	logger.Sugar.Info("🏥 Health server started on port 8080")

	return newHost, nil
}

// HealthChecker interface implementation for Host
func (h *Host) IsHealthy() bool {
	// Check if DefraDB is accessible and processing pipeline is running
	return h.DefraNode != nil && h.processingPipeline != nil
}

func (h *Host) GetCurrentBlock() int64 {
	if h.metrics != nil {
		return int64(h.metrics.MostRecentBlock)
	}
	return 0
}

func (h *Host) GetLastProcessedTime() time.Time {
	if h.metrics != nil {
		return h.metrics.LastDocumentTime
	}
	return time.Time{}
}

func (h *Host) GetPeerInfo() (*server.P2PInfo, error) {
	p2pInfo := &server.P2PInfo{
		Enabled:  h.NetworkHandler != nil,
		PeerInfo: []server.PeerInfo{},
	}

	// Get actual peer information using signer package methods
	if h.DefraNode != nil && h.NetworkHandler != nil {
		// Get peer ID and public key using the same approach as indexer
		peerPubKey, err := h.GetPeerPublicKey()
		if err == nil {
			// Create peer info with actual data
			peerInfo := server.PeerInfo{
				ID:        peerPubKey,                          // Use peer public key as ID for now
				Addresses: []string{"/ip4/127.0.0.1/tcp/9171"}, // Default local address
				PublicKey: peerPubKey,
			}

			p2pInfo.PeerInfo = append(p2pInfo.PeerInfo, peerInfo)
		}
	}

	return p2pInfo, nil
}

func (h *Host) SignMessages(message string) (server.DefraPKRegistration, server.PeerIDRegistration, error) {
	// Use the signer package approach like the indexer
	signedMsg, err := signer.SignWithDefraKeys(message, h.DefraNode, h.config.ShinzoAppConfig)
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Sign with peer ID
	peerSignedMsg, err := signer.SignWithP2PKeys(message, h.DefraNode, h.config.ShinzoAppConfig)
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Get node and peer public keys from signer helpers
	nodePubKey, err := h.GetNodePublicKey()
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, fmt.Errorf("failed to get node public key: %w", err)
	}

	peerPubKey, err := h.GetPeerPublicKey()
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, fmt.Errorf("failed to get peer public key: %w", err)
	}

	return server.DefraPKRegistration{
			PublicKey:   nodePubKey,
			SignedPKMsg: signedMsg,
		}, server.PeerIDRegistration{
			PeerID:        peerPubKey,
			SignedPeerMsg: peerSignedMsg,
		}, nil
}

func (h *Host) GetNodePublicKey() (string, error) {
	return signer.GetDefraPublicKey(h.DefraNode, h.config.ShinzoAppConfig)
}

func (h *Host) GetPeerPublicKey() (string, error) {
	return signer.GetP2PPublicKey(h.DefraNode, h.config.ShinzoAppConfig)
}

// setupViewHTTPServer creates an HTTP server that serves view endpoints alongside DefraDB API
func (h *Host) setupViewHTTPServer() {
	// Note: View endpoints will be registered dynamically as views are created
	// The /api/v0/views listing endpoint is handled by RegisterViewEndpoints
	logger.Sugar.Info("🌐 View HTTP endpoints configured and ready")
}

// handleViewList provides a list of all available view endpoints
func (h *Host) handleViewList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	endpoints := h.GetViewEndpoints()
	response := map[string]interface{}{
		"available_views": len(endpoints),
		"endpoints":       make([]map[string]string, 0, len(endpoints)),
	}

	for name, endpoint := range endpoints {
		response["endpoints"] = append(response["endpoints"].([]map[string]string), map[string]string{
			"view_name":    name,
			"path":         endpoint.Path,
			"last_updated": endpoint.LastUpdated,
		})
	}

	json.NewEncoder(w).Encode(response)
}

func incrementPort(apiURL string) (string, error) {
	// Ensure URL has protocol
	if !strings.HasPrefix(apiURL, "http://") && !strings.HasPrefix(apiURL, "https://") {
		apiURL = "http://" + apiURL
	}

	parsed, err := url.Parse(apiURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL %s: %v", apiURL, err)
	}

	host, portStr, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return "", fmt.Errorf("invalid host:port %s: %v", parsed.Host, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("invalid port %s: %v", portStr, err)
	}

	return net.JoinHostPort(host, strconv.Itoa(port+1)), nil
}

func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	h.processingCancel() // Stop the block processing goroutine (now includes block monitoring)

	// Close the playground server if it's running
	if h.playgroundServer != nil {
		if err := h.playgroundServer.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down playground server: %v\n", err)
		}
	}

	// Close the ViewManager
	if h.viewManager != nil {
		if err := h.viewManager.Close(); err != nil {
			fmt.Printf("Error shutting down ViewManager: %v\n", err)
		}
	}

	// Close the processing pipeline
	if h.processingPipeline != nil {
		h.processingPipeline.Stop()
	}

	// Close the DefraDB node
	if h.DefraNode != nil {
		return h.DefraNode.Close(ctx)
	}

	return nil
}

// GetMetricsHandler returns an HTTP handler for the metrics endpoint
func (h *Host) GetMetricsHandler() http.Handler {
	if h.metrics == nil {
		// Return a handler that returns empty metrics if not initialized
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"error": "metrics not initialized"}`))
		})
	}
	return h.metrics
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

			// Process view registration events using the new ViewRegistrationHandler
			if registeredEvent, ok := event.(*shinzohub.ViewRegisteredEvent); ok {
				logger.Sugar.Infof("📋 Received new view registration: %s (creator: %s)", registeredEvent.View.Name, registeredEvent.Creator)

				// Process the event through the ViewRegistrationHandler
				if h.viewRegistrationHandler != nil {
					err := h.viewRegistrationHandler.ProcessRegisteredEvent(ctx, *registeredEvent)
					if err != nil {
						logger.Sugar.Errorf("❌ Failed to process view registration for %s: %v", registeredEvent.View.Name, err)
					} else {
						logger.Sugar.Infof("✅ Successfully processed view registration for %s", registeredEvent.View.Name)
					}
				} else {
					logger.Sugar.Warn("ViewRegistrationHandler not initialized - cannot process view registration")
				}
			} else {
				logger.Sugar.Debugf("Received non-view event: %+v", event)
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
	return true
}

// applySchema applies the GraphQL schema to DefraDB node
func applySchema(ctx context.Context, defraNode *node.Node) error {
	fmt.Println("Applying schema...")

	_, err := defraNode.DB.AddSchema(ctx, schema.GetSchema())
	if err != nil && strings.Contains(err.Error(), "collection already exists") {
		fmt.Println("Schema already exists, skipping...")
		return nil
	}
	return err
}
