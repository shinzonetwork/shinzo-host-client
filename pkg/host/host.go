package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/accounting"
	"github.com/shinzonetwork/shinzo-host-client/pkg/acp"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	playgroundserver "github.com/shinzonetwork/shinzo-host-client/pkg/playground"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/signer"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/corelog"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	defradbHttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
)

// parseTimeoutOrDefault parses a duration string or returns a default value.
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

// maxInt returns the maximum of two integers.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// DefaultConfig provides a template for host configuration with sensible defaults.
var DefaultConfig *config.Config = func() *config.Config { //nolint:gochecknoglobals
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			URL:           defaultDefraURL,
			KeyringSecret: "test-keyring-secret-for-testing",
			P2P: config.DefraDBP2PConfig{
				Enabled:             true,
				BootstrapPeers:      []string{},
				ListenAddr:          "/ip4/0.0.0.0/tcp/9171",
				MaxRetries:          defaultMaxP2PRetries,
				RetryBaseDelayMs:    defaultRetryBaseDelayMs,
				ReconnectIntervalMs: defaultReconnectIntervalMs,
				EnableAutoReconnect: true,
			},
			Store: config.DefraDBStoreConfig{
				Path: "/tmp/defra-test-default",
			},
		},
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		Schema: config.SchemaConfig{
			IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			HTTPClientTimeoutSecs: config.DefaultSchemaHTTPClientTimeout,
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
		HostConfig: config.HostConfig{
			LensRegistryPath: "./.defra/lens",
		},
	}
	return cfg
}()

// Host represents the main application state and components of the Shinzo host. It manages the DefraDB node, P2P network, view manager, processing pipeline, health server, and other core functionalities of the host application.
type Host struct {
	DefraNode      *node.Node
	NetworkHandler *defradb.NetworkHandler // P2P network control

	// signature verifier as a service
	signatureVerifier      *attestation.DefraSignatureVerifier // Cached signature verifier for attestation processing
	blockSignatureVerifier *attestation.BlockSignatureVerifier // Block signature verifier for block-signed documents
	blockCIDCollector      *attestation.BlockCIDCollector      // Collects CIDs per block for batch verification

	webhookCleanupFunction func()
	LensRegistryPath       string
	processingCancel       context.CancelFunc  // For canceling the event processing goroutine
	playgroundServer       *http.Server        // Playground HTTP server (if enabled)
	acpServer              *defradbHttp.Server // ACP-wrapped GraphQL server (set when the ACP middleware owns the API port)
	acpMiddleware          *acp.Middleware     // Billing gate; drained on Close so in-flight records are not lost
	config                 *config.Config      // Host configuration including StartHeight

	// EVENT-DRIVEN ATTESTATION SYSTEM: Only track attestation processing
	processingPipeline *ProcessingPipeline // Complete message processing pipeline

	// VIEW MANAGEMENT SYSTEM: Handle lens transformations and view lifecycle
	viewManager *view.Manager // Manages view lifecycle and processing

	// Indexers seen attesting, snapshotted into service records. Nil when
	// recording is off.
	attesters *observedAttesters

	healthServer *server.HealthServer
	metrics      *server.HostMetrics

	// mostRecentBlockReceived uint64

	pruner     *pruner.Pruner     // Document pruner for removing old blocks
	pruneQueue *pruner.EventQueue // FIFO queue tracking replicated docIDs
	// pruneGuardStop context.CancelFunc // Stops the prune guard goroutine
}

// StartHosting starts the host application with the provided configuration. It initializes DefraDB, sets up the processing pipeline, view manager, and health server, and optionally subscribes to ShinzoHub events. This is the main entry point for starting the host.
func StartHosting(cfg *config.Config) (*Host, error) {
	return StartHostingWithEventSubscription(cfg)
}

// StartHostingWithEventSubscription starts the host with optional event subscription to ShinzoHub for view registrations and other events. This is the main entry point for starting the host application.
func StartHostingWithEventSubscription(cfg *config.Config) (*Host, error) { //nolint:funlen // TODO: fix funlen
	if cfg == nil {
		cfg = DefaultConfig
	}

	logger.Init(cfg.Logger.Development, "./logs")
	logger.Sugar.Info("🚀 Starting Shinzo host...")

	// Load ACP middleware configuration from the environment before any
	// defradb work so a misconfigured host fails fast instead of starting
	// up ungated.
	acpCfg, err := acp.LoadConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("acp config: %w", err)
	}

	// Configure DefraDB logging - set to error level to hide INFO logs from HTTP requests
	corelog.SetConfigOverride("http", corelog.Config{
		Level: corelog.LevelError,
	})

	var replicationFilter client.ReplicationFilter
	if f := NewEventReplicationFilter(cfg.Shinzo.EventFilter); f != nil {
		replicationFilter = f
	}

	// wazero runs lens transforms in pure Go. wasmtime is the upstream default,
	// but a wasm trap inside wasmtime crosses cgo as a Rust panic and Go cannot
	// recover from it, so the host process dies. wazero returns traps as Go errors.
	nodeOpts := options.Node()
	nodeOpts.DB().SetLensRuntime("wazero")

	schemaCtx, schemaCtxCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Schema.HTTPClientTimeoutSecs)*time.Second)

	logger.Sugar.Infof("Resolving schema (timeout %ds)...", cfg.Schema.HTTPClientTimeoutSecs)
	resolvedSchema := resolveSchema(schemaCtx, cfg)
	schemaCtxCancel()

	// When the ACP middleware is enabled the host owns the GraphQL API port.
	// Defradb still initializes its store, ACP, P2P, and DB on Start; only
	// the auto-start of the HTTP server is suppressed.
	if acpCfg.Enabled {
		nodeOpts.SetDisableAPI(true)
	}

	internalCfg := cfg.ToInternalConfig()
	logger.Sugar.Info("Starting DefraDB node (first run can take ~30-60s)...")
	defraNode, networkHandler, err := defradb.StartDefraInstance(
		internalCfg,
		defradb.NewSchemaApplierFromProvidedSchema(resolvedSchema),
		[]options.Enumerable[options.NodeOptions]{nodeOpts},
		replicationFilter,
		constants.AllCollections...,
	)
	if err != nil {
		return nil, fmt.Errorf("error starting defra instance: %w", err)
	}

	// P2P peers will be added after ViewManager is initialized (see below)

	ctx := context.Background()

	err = waitForDefraDB(ctx, defraNode)
	if err != nil {
		return nil, err
	}

	// Apply local schema after DefraDB is ready (for use with non-branchable)
	err = applySchema(ctx, defraNode, resolvedSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to apply schema: %w", err)
	}

	// Bootstrap from historical snapshots before P2P starts
	if cfg.HostConfig.Snapshot.Enabled && cfg.HostConfig.Snapshot.IndexerURL != "" && len(cfg.HostConfig.Snapshot.HistoricalRanges) > 0 {
		bootstrapFromSnapshots(ctx, defraNode, cfg.HostConfig.Snapshot)
	}

	// View manager has to be built before the ACP server because the
	// middleware's view registry adapts the manager's accessors.
	viewManager := view.NewManager(defraNode, cfg.HostConfig.LensRegistryPath)

	// Recording the served-query attesting set needs the host's observed
	// attesters: the block-signature workers populate it and the serve path
	// snapshots it. Built only when recording is on so the workers skip it
	// otherwise.
	var attesters *observedAttesters
	if acpCfg.Enabled && acpCfg.ASBaseURL != "" {
		window := acpCfg.AttesterWindow
		if window <= 0 {
			window = DefaultAttesterWindow
		}
		attesters = newObservedAttesters(window)
	}

	// When the middleware is enabled the host owns the GraphQL API. The
	// handler is constructed here, wrapped, and served on cfg.DefraDB.URL.
	// defraNode.APIURL is populated inside startACPServer so the playground
	// proxy below uses our address.
	var acpServer *defradbHttp.Server
	var acpMiddleware *acp.Middleware
	if acpCfg.Enabled {
		acpServer, acpMiddleware, err = startACPServer(ctx, acpCfg, internalCfg, cfg.Shinzo.HubBaseURL, defraNode, viewManager, cfg.DefraDB.URL, attesters)
		if err != nil {
			return nil, fmt.Errorf("acp server: %w", err)
		}
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
			return nil, fmt.Errorf("failed to create playground server: %w", err)
		}

		// Start playground server on a different port (defradb port + 1)
		// Parse the defradb URL to get the port and increment it
		playgroundAddr := cfg.DefraDB.URL
		if defraNode.APIURL != "" {
			playgroundAddr = defraNode.APIURL
		}
		playgroundAddr, err = incrementPort(playgroundAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve defra url: %w", err)
		}

		playgroundServer = &http.Server{
			Addr:              playgroundAddr,
			Handler:           playgroundHandler,
			ReadHeaderTimeout: defaultTimeout,
		}

		go func() {
			if err := playgroundServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Sugar.Errorf("Playground server error: %v", err)
			}
		}()

		fmt.Printf("🧪 GraphQL Playground available at http://%s\n", playgroundAddr)
		fmt.Printf("   (Playground proxies API requests to defradb at %s)\n", defraNode.APIURL)
	}

	newHost := &Host{
		DefraNode:              defraNode,
		NetworkHandler:         networkHandler,
		webhookCleanupFunction: func() {},
		LensRegistryPath:       cfg.HostConfig.LensRegistryPath,
		processingCancel:       func() {},
		playgroundServer:       playgroundServer,
		acpServer:              acpServer,
		acpMiddleware:          acpMiddleware,
		config:                 cfg,
		metrics:                server.NewHostMetrics(),
		viewManager:            viewManager,
		attesters:              attesters,
	}

	// Hook up metrics callback for view tracking
	newHost.viewManager.SetMetricsCallback(func() *server.HostMetrics {
		return newHost.metrics
	})

	// Build RPC, WebSocket, and LCD URLs from the hub base URL
	// (e.g., "shinzohub-rpc.infra.source.network:26657"). The Cosmos LCD listens
	// on a separate port (1317 by convention) on the same host.
	var rpcURL, wsURL, lcdURL string
	if cfg.Shinzo.HubBaseURL != "" {
		rpcURL = "http://" + cfg.Shinzo.HubBaseURL
		wsURL = "ws://" + cfg.Shinzo.HubBaseURL + "/websocket"
		lcdURL = "http://" + strings.Replace(cfg.Shinzo.HubBaseURL, ":26657", ":1317", 1)
	}

	// One client serves both the startup backfill and the live-event hydration.
	var rpcClient *shinzohub.RPCClient
	if rpcURL != "" {
		rpcClient = shinzohub.NewRPCClient(lcdURL, defraNode)
	}

	// Fetch views from ShinzoHub if RPC URL is configured
	var hubViews []view.View
	logger.Sugar.Infof("ShinzoHub base URL: %s", rpcURL)
	if rpcClient != nil {
		logger.Sugar.Infof("🔍 Querying ShinzoHub for registered views: %s", rpcURL)
		fetchedViews, totalCount, err := rpcClient.FetchAllRegisteredViews(context.Background())
		if err != nil {
			logger.Sugar.Warnf("⚠️ Failed to fetch views from ShinzoHub: %v", err)
		} else {
			logger.Sugar.Infof("📋 Found %d views from ShinzoHub", totalCount)
			// Debug log the SDL schema for each fetched view
			for i, view := range fetchedViews {
				logger.Sugar.Debugf("🔍 View %d/%d: %s", i+1, len(fetchedViews), view.Name)
				logger.Sugar.Debugf("📄 SDL for %s:\n%s", view.Name, view.Data.Sdl)
				logger.Sugar.Debugf("🎯 Query for %s:\n%s", view.Name, view.Data.Query)
			}
		}
		logger.Sugar.Infof("📋 Found %d views from ShinzoHub", totalCount)
		hubViews = fetchedViews
	} else {
		logger.Sugar.Info("📋 No ShinzoHub base URL configured - skipping remote view fetch")
	}

	logger.Sugar.Debug("Loading %d views from ShinzoHub", len(hubViews))
	if err := newHost.viewManager.LoadAndRegisterViews(context.Background(), hubViews); err != nil {
		logger.Sugar.Warnf("Failed to load views on startup: %v", err)
	}

	logger.Sugar.Info("🎯 ViewManager initialized")

	// Add bootstrap peers and start P2P network (deferred until ViewManager is ready)
	if networkHandler != nil {
		logger.Sugar.Info("▶️ Adding P2P peers and starting network...")

		// Resolve bootstrap peers: auto-discover peer IDs for addresses that don't include them
		discoveryTimeout := time.Duration(cfg.DefraDB.P2P.PeerDiscoveryTimeoutMs) * time.Millisecond
		bootstrapPeers := resolveBootstrapPeers(context.Background(), cfg.DefraDB.P2P.BootstrapPeers, discoveryTimeout)
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
		cfg.Shinzo.UseBlockSignatures,
	)

	logger.Sugar.Infof("🔧 Processing pipeline initialized: queue=%d, batchWriters=%d, batchSize=%d, flushInterval=%dms, useBlockSignatures=%v",
		queueSize, cfg.Shinzo.BatchWriterCount, cfg.Shinzo.BatchSize, cfg.Shinzo.BatchFlushInterval, cfg.Shinzo.UseBlockSignatures)

	// Start the process pipeline
	newHost.processingPipeline.Start()

	if wsURL != "" {
		cancel, channel, err := shinzohub.StartEventSubscription(wsURL, rpcClient)

		cancellableContext, cancelEventHandler := context.WithCancel(context.Background())
		go func() { newHost.handleIncomingEvents(cancellableContext, channel) }()

		newHost.webhookCleanupFunction = func() {
			cancel()
			cancelEventHandler()
		}

		if err != nil {
			return nil, fmt.Errorf("error starting event subscription: %w", err)
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
	if cfg.DefraDB.URL != "" {
		healthDefraURL = cfg.DefraDB.URL
	} else if defraNode != nil && defraNode.APIURL != "" {
		healthDefraURL = defraNode.APIURL
	}

	if defraNode != nil {
		newHost.signatureVerifier = attestation.NewDefraSignatureVerifier(defraNode, newHost.metrics)
		newHost.blockSignatureVerifier = attestation.NewBlockSignatureVerifier(blockSignatureCacheSize)
		newHost.blockCIDCollector = attestation.NewBlockCIDCollector()
		logger.Sugar.Info("🔐 Optimized signature verifier initialized (with block signature support)")
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

	metricsURL := fmt.Sprintf("http://localhost:%d/metrics", port)
	healthURL := fmt.Sprintf("http://localhost:%d/health", port)
	logger.Sugar.Infof("📊 Metrics available at: %s", metricsURL)
	logger.Sugar.Infof("🏥 Health available at: %s", healthURL)

	// Initialize pruner for removing old replicated blocks
	if cfg.Pruner.Enabled && defraNode != nil {
		cfg.Pruner.SetDefaults()
		collections := pruner.DefaultCollectionConfig()

		pruneQueue := pruner.NewEventQueue(collections)
		queuePath := filepath.Join(cfg.DefraDB.Store.Path, "prune_queue.gob")
		if loaded, err := pruneQueue.LoadFromFile(queuePath); err != nil {
			logger.Sugar.Warnf("Failed to load prune queue from disk: %v", err)
		} else if loaded > 0 {
			logger.Sugar.Infof("Restored %d entries from prune queue file", loaded)
		}

		p := pruner.NewPruner(&cfg.Pruner, defraNode)
		p.SetQueue(pruneQueue)

		if err := p.Start(ctx); err != nil {
			logger.Sugar.Warnf("Failed to start pruner: %v", err)
		}

		newHost.pruner = p
		newHost.pruneQueue = pruneQueue
	}

	if cfg.HostConfig.OpenBrowserOnStart {
		go func() {
			time.Sleep(openBrowserDelaySecs * time.Second)
			if err := openBrowser(metricsURL); err != nil {
				logger.Sugar.Debugf("Could not open browser automatically: %v", err)
			} else {
				logger.Sugar.Infof("🌐 Opened metrics page in browser")
			}
			if err := openBrowser(healthURL); err != nil {
				logger.Sugar.Debugf("Could not open browser automatically: %v", err)
				logger.Sugar.Infof("📊 Health available at: %s", healthURL)
			} else {
				logger.Sugar.Infof("🌐 Opened health page in browser: %s", healthURL)
			}
		}()
	}

	return newHost, nil
}

// IsHealthy returns check for defra + processing pipeline health. This is used by the health server to determine if the host is healthy and ready to serve requests.
func (h *Host) IsHealthy() bool {
	// Check if DefraDB is accessible and processing pipeline is running
	return h.DefraNode != nil && h.processingPipeline != nil
}

// GetCurrentBlock returns the most recent block height that the host has processed, based on the host's metrics. This is used for monitoring the progress of block processing and data freshness.
func (h *Host) GetCurrentBlock() int64 {
	if h.metrics != nil {
		return int64(atomic.LoadUint64(&h.metrics.MostRecentBlock)) //nolint:gosec // block numbers won't exceed int64 max
	}
	return 0
}

// ProcessViewRegistrationEvent processes a view registration event.
func (h *Host) ProcessViewRegistrationEvent(ctx context.Context, event shinzohub.ViewRegisteredEvent) error {
	if h.viewManager == nil {
		return ErrViewManagerNil
	}

	// Validate view has required fields
	if event.View.Data.Query == "" {
		return fmt.Errorf("view %s: %w", event.View.Name, ErrViewMissingQuery)
	}
	if event.View.Data.Sdl == "" {
		return fmt.Errorf("view %s: %w", event.View.Name, ErrViewMissingSDL)
	}

	// Write WASM to disk
	if len(event.View.Data.Transform.Lenses) > 0 {
		if err := event.View.PostWasmToFile(h.LensRegistryPath); err != nil {
			return fmt.Errorf("failed to write WASM for view %s: %w", event.View.Name, err)
		}
	}

	// Register view
	if err := h.viewManager.RegisterView(ctx, &event.View); err != nil {
		return fmt.Errorf("failed to register view %s: %w", event.View.Name, err)
	}

	// Persist to registry (with any auto-corrections applied)
	if err := view.SaveViewToRegistry(h.LensRegistryPath, event.View); err != nil {
		logger.Sugar.Warnf("Failed to persist view %s: %v", event.View.Name, err)
	}

	return nil
}

// GetLastProcessedTime returns the timestamp of the most recently processed block, based on the host's metrics. This is used for monitoring the freshness of the data being processed by the host.
func (h *Host) GetLastProcessedTime() time.Time {
	if h.metrics != nil {
		return h.metrics.LastDocumentTime
	}
	return time.Time{}
}

// GetPeerInfo returns information about the host's P2P network status, including whether P2P is enabled, the host's own peer ID and addresses, and a list of currently connected peers. This is used for monitoring and debugging the P2P network status of the host.
func (h *Host) GetPeerInfo() (*server.P2PInfo, error) {
	p2pInfo := &server.P2PInfo{
		Enabled:  h.NetworkHandler != nil,
		PeerInfo: []server.PeerInfo{},
	}

	if h.DefraNode == nil || h.NetworkHandler == nil {
		return p2pInfo, nil
	}

	ctx := context.Background()

	// Get this node's own peer info (listening addresses)
	ownAddresses, err := h.DefraNode.DB.PeerInfo(ctx)
	if err != nil {
		logger.Sugar.Warnf("Failed to get peer info from DefraDB: %v", err)
		return p2pInfo, nil
	}
	ownPeers, _ := defradb.BootstrapIntoPeers(ownAddresses)

	if len(ownPeers) > 0 {
		var addresses []string
		for _, p := range ownPeers {
			addresses = append(addresses, p.Addresses...)
		}
		p2pInfo.Self = &server.PeerInfo{
			ID:        ownPeers[0].ID,
			Addresses: addresses,
		}
	}

	// Get actually connected peers
	activePeerStrings, err := h.DefraNode.DB.ActivePeers(ctx)
	if err != nil {
		activePeerStrings = nil // P2P not ready, treat as no peers
	}
	activePeers, _ := defradb.BootstrapIntoPeers(activePeerStrings)

	// Deduplicate peers by ID and merge addresses
	peerMap := make(map[string]*server.PeerInfo)
	for _, peer := range activePeers {
		if existing, ok := peerMap[peer.ID]; ok {
			existing.Addresses = append(existing.Addresses, peer.Addresses...)
		} else {
			peerMap[peer.ID] = &server.PeerInfo{
				ID:        peer.ID,
				Addresses: peer.Addresses,
			}
		}
	}
	for _, p := range peerMap {
		p2pInfo.PeerInfo = append(p2pInfo.PeerInfo, *p)
	}

	return p2pInfo, nil
}

// SignMessages signs a message using both DefraDB node keys and P2P keys, returning the signed messages for attestation registration and peer ID registration. This is used for proving ownership of the host in the attestation system and P2P network.
func (h *Host) SignMessages(message string) (server.DefraPKRegistration, server.PeerIDRegistration, error) {
	// Use the signer package approach like the indexer
	signedMsg, err := signer.SignWithDefraKeys(message, h.DefraNode, h.config.ToInternalConfig())
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Sign with peer ID
	peerSignedMsg, err := signer.SignWithP2PKeys(message, h.DefraNode, h.config.ToInternalConfig())
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

// GetNodePublicKey retrieves the public key of the DefraDB node using the signer package. This is used for attestation registration and verification.
func (h *Host) GetNodePublicKey() (string, error) {
	return signer.GetDefraPublicKey(h.DefraNode, h.config.ToInternalConfig())
}

// GetPeerPublicKey retrieves the P2P public key of the host using the signer package. This is used for peer ID registration and verification in the P2P network.
func (h *Host) GetPeerPublicKey() (string, error) {
	return signer.GetP2PPublicKey(h.DefraNode, h.config.ToInternalConfig())
}

// incrementPort takes a URL string, parses it, increments the port by 1, and returns the modified URL string. It handles URLs with or without protocols and returns an error if the URL is invalid or the port cannot be parsed.
func incrementPort(apiURL string) (string, error) {
	// Ensure URL has protocol
	if !strings.HasPrefix(apiURL, "http://") && !strings.HasPrefix(apiURL, "https://") {
		apiURL = "http://" + apiURL
	}

	parsed, err := url.Parse(apiURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL %s: %w", apiURL, err)
	}

	host, portStr, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return "", fmt.Errorf("invalid host:port %s: %w", parsed.Host, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("invalid port %s: %w", portStr, err)
	}

	return net.JoinHostPort(host, strconv.Itoa(port+1)), nil
}

// Close gracefully shuts down the host, including the DefraDB node, processing pipeline, health server, playground server, and pruner. It ensures all resources are cleaned up properly.
func (h *Host) Close(ctx context.Context) error {
	h.webhookCleanupFunction()
	h.processingCancel() // Stop the block processing goroutine (now includes block monitoring)

	// Stop pruner and save queue to disk
	if h.pruner != nil {
		h.pruner.Stop()
		h.pruner = nil
	}

	// Close the playground server if it's running
	if h.playgroundServer != nil {
		if err := h.playgroundServer.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down playground server: %v\n", err)
		}
	}

	// Shut down the ACP-wrapped GraphQL server before closing defradb;
	// the server holds a handler that calls into defraNode.DB.
	if h.acpServer != nil {
		if err := h.acpServer.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down ACP server: %v\n", err)
		}
	}

	// The server no longer accepts requests, so drain the service records it
	// already spawned before exit; they post to the accounting service, not defradb.
	if h.acpMiddleware != nil {
		h.acpMiddleware.Drain(ctx)
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

// buildRecording constructs service-record submission when an accounting service
// URL is configured; otherwise the gate serves without recording. attesters
// supplies the observed attesting set snapshotted into each record, and is
// non-nil whenever an accounting service URL is set.
func buildRecording(acpCfg acp.Config, internalCfg *defradb.Config, defraNode *node.Node, attesters *observedAttesters) (*acp.Recording, error) {
	if acpCfg.ASBaseURL == "" {
		return nil, nil
	}
	nodeKey, err := signer.NodeECDSAKey(defraNode, internalCfg)
	if err != nil {
		return nil, fmt.Errorf("load node key for recording: %w", err)
	}
	client := accounting.NewClient(acpCfg.ASBaseURL, acp.DefaultRecordTimeout)
	return &acp.Recording{
		Recorder:  accounting.NewRecorder(client, nodeKey, acpCfg.ChainID),
		Attesters: attesters.Snapshot,
	}, nil
}

// startACPServer constructs the host-owned GraphQL HTTP server: it wraps
// defradb's API handler with the ACP middleware, binds the listener on
// the resolved LAN address, points defraNode.APIURL at the address so
// the playground proxy uses it, and waits for the server to answer a
// health check before returning. Caller shuts the returned server down
// via Host.Close.
func startACPServer(
	ctx context.Context,
	acpCfg acp.Config,
	internalCfg *defradb.Config,
	hubBaseURL string,
	defraNode *node.Node,
	viewManager *view.Manager,
	defraURL string,
	attesters *observedAttesters,
) (*defradbHttp.Server, *acp.Middleware, error) {
	if hubBaseURL == "" {
		return nil, nil, errors.New("billing middleware enabled but the Shinzo hub base URL is not configured")
	}
	// The host reads the hub over its Cosmos LCD (REST) port. HubBaseURL points
	// at the CometBFT RPC port; the LCD is the same host on :1317.
	lcdURL := "http://" + strings.Replace(hubBaseURL, ":26657", ":1317", 1)
	hubClient := shinzohub.NewRPCClient(lcdURL, defraNode)
	epochClock := acp.NewEpochClock(hubClient, acpCfg.EpochLength, epochPollInterval)
	// "shinzo" is the chain's bech32 account prefix: the recovered EVM payer is
	// encoded to it for the x/querybalance lookup.
	authz := acp.NewBalanceAuthorizer(hubClient, epochClock, acpCfg.MinBalance(), "shinzo")

	registry := acp.NewViewRegistry(viewManager)

	recording, err := buildRecording(acpCfg, internalCfg, defraNode, attesters)
	if err != nil {
		return nil, nil, err
	}
	mw := acp.NewMiddleware(authz, registry, acpCfg.ChainID, acp.DefaultRequestMaxAge, recording, logger.Sugar)

	handler, err := defradbHttp.NewHandler(defraNode.DB, defraNode.Options())
	if err != nil {
		return nil, nil, fmt.Errorf("defradb handler: %w", err)
	}

	// Bind on host:port. Loopback hosts (localhost, 127.0.0.1, ::1) are
	// promoted to 0.0.0.0 so the same listener accepts both external
	// connections and in-process loopback calls (e.g. healthcheck probes).
	host, port, splitErr := net.SplitHostPort(defraURL)
	if splitErr != nil {
		return nil, nil, fmt.Errorf("parse defraURL %q: %w", defraURL, splitErr)
	}
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		host = "0.0.0.0"
	}
	listenAddr := net.JoinHostPort(host, port)

	server, err := defradbHttp.NewServer(mw.Wrap(handler), options.NodeHTTP().SetAddress(listenAddr))
	if err != nil {
		return nil, nil, fmt.Errorf("defradb server: %w", err)
	}
	if err := server.SetListener(); err != nil {
		return nil, nil, fmt.Errorf("listener: %w", err)
	}

	go func() {
		if err := server.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Sugar.Errorf("ACP server stopped: %v", err)
		}
	}()

	defraNode.APIURL = server.Address()

	healthCtx, cancel := context.WithTimeout(ctx, acpHealthCheckTimeout)
	defer cancel()

	healthClient, err := defradbHttp.NewClient(server.Address())
	if err != nil {
		_ = server.Shutdown(ctx)
		return nil, nil, fmt.Errorf("health client: %w", err)
	}
	if err := healthClient.HealthCheck(healthCtx); err != nil {
		_ = server.Shutdown(ctx)
		return nil, nil, fmt.Errorf("health check: %w", err)
	}

	return server, mw, nil
}

// acpHealthCheckTimeout bounds the post-listen health check on the
// ACP-wrapped server. The server is local; a slow response indicates
// startup is wedged and the host should fail rather than hang.
const acpHealthCheckTimeout = 5 * time.Second

// epochPollInterval bounds how often the balance gate re-reads the hub height to
// learn the current settlement epoch. Balances only change at epoch close, so a
// coarse interval is enough to invalidate the per-payer cache in time.
const epochPollInterval = 30 * time.Second

// GetMetricsHandler returns an HTTP handler for the metrics endpoint.
func (h *Host) GetMetricsHandler() http.Handler {
	if h.metrics == nil {
		// Return a handler that returns empty metrics if not initialized
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"error": "metrics not initialized"}`))
		})
	}
	return h.metrics
}

// waitForDefraDB polls the embedded DefraDB node until a trivial schema query
// succeeds. Returns an error after maxAttempts seconds if the node never
// responds, e.g. because schema setup failed upstream.
func waitForDefraDB(ctx context.Context, defraNode *node.Node) error {
	fmt.Println("Waiting for defra...")
	maxAttempts := 30

	// Simple query to check if the schema is ready
	query := `{ ` + constants.CollectionBlock + ` { __typename } }`

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := defradb.QuerySingle[map[string]any](ctx, defraNode, query)
		if err == nil {
			fmt.Println("Defra is responsive!")
			return nil
		}

		if attempt < maxAttempts {
			fmt.Printf("Attempt %d failed... Trying again\n", attempt)
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("after %d retry attempts: %w", maxAttempts, ErrDefraDBNotReady)
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
				if registeredEvent.View.Data.Query == "" {
					logger.Sugar.Errorf("❌ View %s missing query - skipping registration", registeredEvent.View.Name)
					continue
				}
				if registeredEvent.View.Data.Sdl == "" {
					logger.Sugar.Errorf("❌ View %s missing SDL - skipping registration", registeredEvent.View.Name)
					continue
				}

				// 2. Ensure WASM files are written to disk (decodes base64 and writes to lens registry)
				if len(registeredEvent.View.Data.Transform.Lenses) > 0 {
					if err := registeredEvent.View.PostWasmToFile(h.LensRegistryPath); err != nil {
						logger.Sugar.Errorf("❌ Failed to write WASM files for view %s: %v", registeredEvent.View.Name, err)
						continue
					}
				}

				// 3. Register the view with ViewManager and persist to registry
				if h.viewManager != nil {
					if err := h.viewManager.RegisterView(ctx, &registeredEvent.View); err != nil {
						logger.Sugar.Errorf("❌ Failed to register view %s: %v", registeredEvent.View.Name, err)
					} else {
						// 4. Persist view to registry for next startup (with any auto-corrections applied)
						if err := view.SaveViewToRegistry(h.LensRegistryPath, registeredEvent.View); err != nil {
							logger.Sugar.Warnf("⚠️ Failed to persist view %s: %v", registeredEvent.View.Name, err)
						}
						logger.Sugar.Infof("✅ Successfully registered view %s", registeredEvent.View.Name)
					}
				} else {
					logger.Sugar.Warn("ViewManager not initialized - cannot register view")
				}
			} else {
				logger.Sugar.Debugf("Received unknown event type: %+v", event)
			}
		}
	}
}

// monitorHighestBlockNumber has been removed - block monitoring is now handled
// by the event-driven processAllViews goroutine for better efficiency

// StartHostingWithTestConfig starts the host with a test configuration, including a temporary DefraDB store path and dynamic health server port. This is used for testing to avoid conflicts with existing instances and to ensure isolation.
func StartHostingWithTestConfig(t *testing.T) (*Host, error) {
	testConfig := DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.URL = "127.0.0.1:0"

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
		ctx, cancel := context.WithTimeout(context.Background(), healthServerShutdownTimeoutSecs*time.Second)
		defer cancel()
		_ = host.healthServer.Stop(ctx)

		// Find an available port
		listener, err := net.Listen("tcp", ":0") //nolint:gosec
		if err != nil {
			return nil, fmt.Errorf("failed to find available port: %w", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()

		// Create new health server with dynamic port
		healthDefraURL := ""
		if testConfig.DefraDB.URL != "" {
			healthDefraURL = "http://" + testConfig.DefraDB.URL
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

// isPlaygroundEnabled checks the environment variable to determine if the GraphQL Playground should be enabled. This allows for dynamic control over the playground feature without changing code, which is useful for testing and different deployment environments.
func isPlaygroundEnabled() bool {
	return playgroundEnabled
}

// applySchema applies the GraphQL schema to DefraDB node.
func applySchema(ctx context.Context, defraNode *node.Node, schemaStr string) error {
	fmt.Println("Applying schema...")

	_, err := defraNode.DB.AddCollection(ctx, schemaStr)
	if err != nil && strings.Contains(err.Error(), "collection already exists") {
		fmt.Println("Schema already exists, trying to add new types individually...")
		// Try adding Config__LastProcessedPage separately in case it's new
		configSchema := `type Config__LastProcessedPage { page: Int, pageSize: Int }`
		_, configErr := defraNode.DB.AddCollection(ctx, configSchema)
		if configErr != nil && !strings.Contains(configErr.Error(), "collection already exists") {
			fmt.Printf("Note: Could not add Config__LastProcessedPage: %v\n", configErr)
		}
		return nil
	}
	return err
}

// RegisterViewWithManager registers a view and manages its lifecycle.
func (h *Host) RegisterViewWithManager(ctx context.Context, v view.View) error {
	if h.viewManager == nil {
		return ErrViewManagerNil
	}
	return h.viewManager.RegisterView(ctx, &v)
}

// GetActiveViewNames returns names of all active views.
func (h *Host) GetActiveViewNames() []string {
	if h.viewManager == nil {
		return []string{}
	}
	return h.viewManager.GetActiveViewNames()
}

// execCommand is the function used to create exec.Cmd instances.
// Overridden in tests to avoid actually opening browsers.
var execCommand = exec.Command //nolint:gochecknoglobals

// openBrowser opens the specified URL in the default browser
// Works on macOS, Linux, and Windows.
func openBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = execCommand("cmd", "/c", "start", url)
	case "darwin":
		cmd = execCommand("open", url)
	case "linux":
		cmd = execCommand("xdg-open", url)
	default:
		return fmt.Errorf("platform %s: %w", runtime.GOOS, ErrUnsupportedPlatform)
	}
	return cmd.Start()
}
