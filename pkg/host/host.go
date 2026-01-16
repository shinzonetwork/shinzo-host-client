package host

import (
	"context"
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
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	playgroundserver "github.com/shinzonetwork/shinzo-host-client/pkg/playground"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/corelog"
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
		DefraDB: config.DefraDBConfig{
			Url:           "localhost:9181",
			KeyringSecret: "test-keyring-secret-for-testing",
			P2P: config.DefraDBP2PConfig{
				Enabled:             true,
				BootstrapPeers:      []string{},
				ListenAddr:          "/ip4/0.0.0.0/tcp/9171",
				MaxRetries:          5,
				RetryBaseDelayMs:    1000,
				ReconnectIntervalMs: 60000,
				EnableAutoReconnect: true,
			},
			Store: config.DefraDBStoreConfig{
				Path: "/tmp/defra-test-default",
			},
		},
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
		HostConfig: config.HostConfig{
			LensRegistryPath: "./.lens",
		},
	}
	return cfg
}()

type Host struct {
	DefraNode      *node.Node
	NetworkHandler *defra.NetworkHandler // P2P network control

	// signature verifier as a service
	signatureVerifier *attestation.DefraSignatureVerifier // Cached signature verifier for attestation processing

	webhookCleanupFunction func()
	LensRegistryPath       string
	processingCancel       context.CancelFunc // For canceling the event processing goroutine
	playgroundServer       *http.Server       // Playground HTTP server (if enabled)
	config                 *config.Config     // Host configuration including StartHeight

	// EVENT-DRIVEN ATTESTATION SYSTEM: Only track attestation processing
	processingPipeline *ProcessingPipeline // Complete message processing pipeline

	// VIEW MANAGEMENT SYSTEM: Handle lens transformations and view lifecycle
	viewManager *view.ViewManager // Manages view lifecycle and processing

	// METRICS SYSTEM: Track attestation and document processing metrics
	healthServer *server.HealthServer
	metrics      *server.HostMetrics // Comprehensive metrics tracking

	mostRecentBlockReceived uint64 // This keeps track of the most recent block number received - useful for debugging and confirming Host is receiving blocks from Indexers
}

func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg)
}

func StartHostingWithEventSubscription(cfg *config.Config) (*Host, error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(true, "./logs")

	// Configure DefraDB logging - set to error level to hide INFO logs from HTTP requests
	corelog.SetConfigOverride("http", corelog.Config{
		Level: corelog.LevelError,
	})

	defraNode, networkHandler, err := defra.StartDefraInstance(
		cfg.ToAppConfig(),
		defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()),
		constants.AllCollections...,
	)
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %v", err)
	}

	// P2P peers will be added after ViewManager is initialized (see below)

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
		playgroundAddr := cfg.DefraDB.Url
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
		LensRegistryPath:       cfg.HostConfig.LensRegistryPath,
		processingCancel:       func() {},
		playgroundServer:       playgroundServer,
		config:                 cfg,
		metrics:                server.NewHostMetrics(),
	}

	// Initialize ViewManager and load persisted views
	// This handles: 1) Fetch views from ShinzoHub, 2) Load local views, 3) Download WASM files, 4) Register views
	newHost.viewManager = view.NewViewManager(defraNode, cfg.HostConfig.LensRegistryPath)

	// Hook up metrics callback for view tracking
	newHost.viewManager.SetMetricsCallback(func() *server.HostMetrics {
		return newHost.metrics
	})

	// Build RPC and WebSocket URLs from base URL (e.g., "shinzohub-rpc.infra.source.network:26657")
	var rpcURL, wsURL string
	if cfg.Shinzo.HubBaseURL != "" {
		rpcURL = "http://" + cfg.Shinzo.HubBaseURL
		wsURL = "ws://" + cfg.Shinzo.HubBaseURL + "/websocket"
	}

	// Fetch views from ShinzoHub if RPC URL is configured
	var hubViews []view.View
	logger.Sugar.Infof("ShinzoHub base URL: %s", rpcURL)
	if rpcURL != "" {
		logger.Sugar.Infof("🔍 Querying ShinzoHub for registered views: %s", rpcURL)
		rpcClient := shinzohub.NewRPCClient(rpcURL, defraNode)
		fetchedViews, err := rpcClient.FetchAllRegisteredViews(context.Background())
		if err != nil {
			logger.Sugar.Warnf("⚠️ Failed to fetch views from ShinzoHub: %v", err)
		}
		logger.Sugar.Infof("📋 Found %d views from ShinzoHub", len(fetchedViews))
		hubViews = fetchedViews
	} else {
		logger.Sugar.Info("📋 No ShinzoHub base URL configured - skipping remote view fetch")
	}

	logger.Sugar.Debug("Loading %d views from ShinzoHub", len(hubViews))
	if err := newHost.viewManager.LoadAndRegisterViews(context.Background(), hubViews); err != nil {
		logger.Sugar.Warnf("Failed to load views on startup: %v", err)
	}

	logger.Sugar.Info("🎯 ViewManager initialized")

	// Add bootstrap peers and start P2P network now that ViewManager is ready
	// Note: ToAppConfig() passes empty peers to StartDefraInstance, peers are added here after ViewManager init
	if networkHandler != nil {
		logger.Sugar.Info("▶️ Adding P2P peers and starting network...")

		// Add the indexer peer(s) - these were removed from config to delay P2P until ViewManager is ready
		bootstrapPeers := cfg.DefraDB.P2P.BootstrapPeers
		logger.Sugar.Infof("▶️ Adding %d P2P peers and starting network...", len(bootstrapPeers))

		for _, peer := range bootstrapPeers {
			if err := networkHandler.AddPeer(peer); err != nil {
				logger.Sugar.Warnf("Failed to add peer %s: %v", peer, err)
			} else {
				logger.Sugar.Infof("✅ Added P2P peer: %s", peer)
			}
		}

		if err := networkHandler.StartNetwork(); err != nil {
			logger.Sugar.Warnf("Failed to start P2P network: %v", err)
		} else {
			logger.Sugar.Info("✅ P2P network started")
		}
	}

	queueSize := cfg.Shinzo.CacheQueueSize
	if queueSize <= 0 {
		queueSize = 50000
	}

	// Create processing pipeline with batch settings from config
	newHost.processingPipeline = NewProcessingPipeline(
		context.Background(), newHost, queueSize,
		cfg.Shinzo.BatchWriterCount, cfg.Shinzo.BatchSize, cfg.Shinzo.BatchFlushInterval,
	)

	logger.Sugar.Infof("🔧 Processing pipeline initialized: queue=%d, batchWriters=%d, batchSize=%d, flushInterval=%dms",
		queueSize, cfg.Shinzo.BatchWriterCount, cfg.Shinzo.BatchSize, cfg.Shinzo.BatchFlushInterval)

	// Start the process pipeline
	newHost.processingPipeline.Start()

	if wsURL != "" {
		cancel, channel, err := shinzohub.StartEventSubscription(wsURL)

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
	if cfg.DefraDB.Url != "" {
		healthDefraURL = cfg.DefraDB.Url
	} else if defraNode != nil && defraNode.APIURL != "" {
		healthDefraURL = defraNode.APIURL
	}

	if defraNode != nil {
		newHost.signatureVerifier = attestation.NewDefraSignatureVerifier(defraNode)
		logger.Sugar.Info("🔐 Optimized signature verifier initialized")
	}

	port := cfg.HostConfig.HealthServerPort
	if port == 0 {
		port = 8080
	}
	newHost.healthServer = server.NewHealthServer(port, newHost, healthDefraURL, newHost.metrics)

	// Start health server in background
	go func() {
		if err := newHost.healthServer.Start(); err != nil {
			logger.Sugar.Errorf("Health server failed: %v", err)
		}
	}()

	logger.Sugar.Infof("🏥 Health server started on port %d", port)

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

// ProcessViewRegistrationEvent processes a view registration event
func (h *Host) ProcessViewRegistrationEvent(ctx context.Context, event shinzohub.ViewRegisteredEvent) error {
	if h.viewManager == nil {
		return fmt.Errorf("ViewManager not initialized")
	}

	// Validate view has required fields
	if event.View.Query == nil || *event.View.Query == "" {
		return fmt.Errorf("view %s missing query", event.View.Name)
	}
	if event.View.Sdl == nil || *event.View.Sdl == "" {
		return fmt.Errorf("view %s missing SDL", event.View.Name)
	}

	// Write WASM to disk
	if len(event.View.Transform.Lenses) > 0 {
		if err := event.View.PostWasmToFile(ctx, h.LensRegistryPath); err != nil {
			return fmt.Errorf("failed to write WASM for view %s: %w", event.View.Name, err)
		}
	}

	// Register view
	if err := h.viewManager.RegisterView(ctx, event.View); err != nil {
		return fmt.Errorf("failed to register view %s: %w", event.View.Name, err)
	}

	// Persist to registry
	if err := view.SaveViewToRegistry(h.LensRegistryPath, event.View); err != nil {
		logger.Sugar.Warnf("Failed to persist view %s: %v", event.View.Name, err)
	}

	return nil
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
		peerInfoStrings, err := h.DefraNode.DB.PeerInfo()
		if err != nil {
			logger.Sugar.Warnf("Failed to get peer info from DefraDB: %v", err)
			return p2pInfo, nil
		}

		peers, errs := defra.BootstrapIntoPeers(peerInfoStrings)
		if len(errs) > 0 {
			logger.Sugar.Warnf("Errors parsing peer info: %v", errs)
		}

		for _, peer := range peers {
			peerInfo := server.PeerInfo{
				ID:        peer.ID,
				Addresses: peer.Addresses,
				PublicKey: peer.ID,
			}

			p2pInfo.PeerInfo = append(p2pInfo.PeerInfo, peerInfo)
		}
	}

	return p2pInfo, nil
}

func (h *Host) SignMessages(message string) (server.DefraPKRegistration, server.PeerIDRegistration, error) {
	// Use the signer package approach like the indexer
	signedMsg, err := signer.SignWithDefraKeys(message, h.DefraNode, h.config.ToAppConfig())
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Sign with peer ID
	peerSignedMsg, err := signer.SignWithP2PKeys(message, h.DefraNode, h.config.ToAppConfig())
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
	return signer.GetDefraPublicKey(h.DefraNode, h.config.ToAppConfig())
}

func (h *Host) GetPeerPublicKey() (string, error) {
	return signer.GetP2PPublicKey(h.DefraNode, h.config.ToAppConfig())
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

	// ViewManager cleanup (no explicit close needed - just log)
	if h.viewManager != nil {
		logger.Sugar.Infof("🎯 ViewManager shutdown: %d active views", h.viewManager.GetViewCount())
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
	query := `{ ` + constants.CollectionBlock + ` { __typename } }`

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
				// check that request is complete : decodes and lens is not nil
				// call ensure wasm
				// call view.RegisterView

				// Process the event through the ViewRegistrationHandler
				// 1. Check that request is complete: Query, SDL, and Lenses are present
				if registeredEvent.View.Query == nil || *registeredEvent.View.Query == "" {
					logger.Sugar.Errorf("❌ View %s missing query - skipping registration", registeredEvent.View.Name)
					continue
				}
				if registeredEvent.View.Sdl == nil || *registeredEvent.View.Sdl == "" {
					logger.Sugar.Errorf("❌ View %s missing SDL - skipping registration", registeredEvent.View.Name)
					continue
				}

				// 2. Ensure WASM files are written to disk (decodes base64 and writes to lens registry)
				if len(registeredEvent.View.Transform.Lenses) > 0 {
					if err := registeredEvent.View.PostWasmToFile(ctx, h.LensRegistryPath); err != nil {
						logger.Sugar.Errorf("❌ Failed to write WASM files for view %s: %v", registeredEvent.View.Name, err)
						continue
					}
				}

				// 3. Register the view with ViewManager and persist to registry
				if h.viewManager != nil {
					if err := h.viewManager.RegisterView(ctx, registeredEvent.View); err != nil {
						logger.Sugar.Errorf("❌ Failed to register view %s: %v", registeredEvent.View.Name, err)
					} else {
						// 4. Persist view to registry for next startup
						if err := view.SaveViewToRegistry(h.LensRegistryPath, registeredEvent.View); err != nil {
							logger.Sugar.Warnf("⚠️ Failed to persist view %s: %v", registeredEvent.View.Name, err)
						}
						logger.Sugar.Infof("✅ Successfully registered view %s", registeredEvent.View.Name)
					}
				} else {
					logger.Sugar.Warn("ViewManager not initialized - cannot register view")
				}
			} else if entityEvent, ok := event.(*shinzohub.EntityRegisteredEvent); ok {
				// Process EntityRegistered events - add as P2P peers
				entityType := shinzohub.GetEntityType(entityEvent.Entity)
				logger.Sugar.Infof("🎯 Received EntityRegistered event: type=%s, key=%s, owner=%s, pid=%s",
					entityType, entityEvent.Key, entityEvent.Owner, entityEvent.Pid)

				// Add entity as P2P peer for communication
				if h.NetworkHandler != nil {
					err := h.NetworkHandler.AddPeer(entityEvent.Pid)
					if err != nil {
						logger.Sugar.Errorf("❌ Failed to add %s peer %s: %v", entityType, entityEvent.Pid, err)
					} else {
						logger.Sugar.Infof("✅ Successfully added %s as P2P peer: %s", entityType, entityEvent.Pid)
					}
				} else {
					logger.Sugar.Warn("NetworkHandler not initialized - cannot add P2P peer")
				}
			} else {
				logger.Sugar.Debugf("Received unknown event type: %+v", event)
			}
		}
	}
}

// monitorHighestBlockNumber has been removed - block monitoring is now handled
// by the event-driven processAllViews goroutine for better efficiency

func StartHostingWithTestConfig(t *testing.T) (*Host, error) {
	testConfig := DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.Url = "127.0.0.1:0"

	// Set start height to 0 for tests to process all documents
	testConfig.Shinzo.StartHeight = 0

	// Create host with dynamic health server port
	host, err := StartHosting(testConfig)
	if err != nil {
		return nil, err
	}

	// Override health server with dynamic port for tests
	if host.healthServer != nil {
		// Stop the configured health server
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		host.healthServer.Stop(ctx)

		// Find an available port
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, fmt.Errorf("failed to find available port: %w", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		// Create new health server with dynamic port
		healthDefraURL := ""
		if testConfig.DefraDB.Url != "" {
			healthDefraURL = "http://" + testConfig.DefraDB.Url
		}
		host.healthServer = server.NewHealthServer(port, host, healthDefraURL, host.metrics)

		// Start the new health server
		go func() {
			if err := host.healthServer.Start(); err != nil && !strings.Contains(err.Error(), "Server closed") && !strings.Contains(err.Error(), "address already in use") {
				logger.Sugar.Errorf("Health server failed: %v", err)
			}
		}()

		logger.Sugar.Infof("🏥 Health server started on port %d", port)
	}

	return host, nil
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

	_, err := defraNode.DB.AddSchema(ctx, schema.GetSchemaForBuild())
	if err != nil && strings.Contains(err.Error(), "collection already exists") {
		fmt.Println("Schema already exists, trying to add new types individually...")
		// Try adding Config__LastProcessedPage separately in case it's new
		configSchema := `type Config__LastProcessedPage { page: Int @index }`
		_, configErr := defraNode.DB.AddSchema(ctx, configSchema)
		if configErr != nil && !strings.Contains(configErr.Error(), "collection already exists") {
			fmt.Printf("Note: Could not add Config__LastProcessedPage: %v\n", configErr)
		}
		return nil
	}
	return err
}

// RegisterViewWithManager registers a view and manages its lifecycle
func (h *Host) RegisterViewWithManager(ctx context.Context, v view.View) error {
	if h.viewManager == nil {
		return fmt.Errorf("ViewManager not initialized")
	}
	return h.viewManager.RegisterView(ctx, v)
}

// GetActiveViewNames returns names of all active views
func (h *Host) GetActiveViewNames() []string {
	if h.viewManager == nil {
		return []string{}
	}
	return h.viewManager.GetActiveViewNames()
}
