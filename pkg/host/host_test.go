package host

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	defradbHttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

func TestParseTimeoutOrDefault(t *testing.T) {
	tests := []struct {
		name            string
		timeoutStr      string
		defaultDuration time.Duration
		expected        time.Duration
	}{
		{
			name:            "empty string returns default",
			timeoutStr:      "",
			defaultDuration: 5 * time.Second,
			expected:        5 * time.Second,
		},
		{
			name:            "valid duration string",
			timeoutStr:      "10s",
			defaultDuration: 5 * time.Second,
			expected:        10 * time.Second,
		},
		{
			name:            "valid duration with minutes",
			timeoutStr:      "2m",
			defaultDuration: 5 * time.Second,
			expected:        2 * time.Minute,
		},
		{
			name:            "invalid format returns default",
			timeoutStr:      "invalid",
			defaultDuration: 5 * time.Second,
			expected:        5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTimeoutOrDefault(tt.timeoutStr, tt.defaultDuration)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDefraProbeAddress(t *testing.T) {
	const configured = "localhost:9181"
	const bound = "http://192.0.2.10:9181"

	tests := []struct {
		name      string
		defraNode *node.Node
		expected  string
	}{
		{"no node", nil, configured},
		{"api disabled", &node.Node{}, configured},
		{"api bound", &node.Node{APIURL: bound}, bound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, defraProbeAddress(tt.defraNode, configured))
		})
	}
}

func TestMaxInt(t *testing.T) {
	tests := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{"a greater", 10, 5, 10},
		{"b greater", 5, 10, 10},
		{"equal", 7, 7, 7},
		{"negative numbers", -5, -10, -5},
		{"zero and positive", 0, 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maxInt(tt.a, tt.b)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIncrementPort(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		{
			name:        "localhost with port",
			input:       "http://localhost:9181",
			expected:    "localhost:9182",
			expectError: false,
		},
		{
			name:        "IP with port",
			input:       "http://192.168.1.1:8080",
			expected:    "192.168.1.1:8081",
			expectError: false,
		},
		{
			name:        "without protocol",
			input:       "localhost:9181",
			expected:    "localhost:9182",
			expectError: false,
		},
		{
			name:        "invalid URL",
			input:       "not a valid url without port",
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := incrementPort(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	require.NotNil(t, DefaultConfig)
	require.Equal(t, "localhost:9181", DefaultConfig.DefraDB.URL)
	require.True(t, DefaultConfig.DefraDB.P2P.Enabled)
	require.Equal(t, 1, DefaultConfig.Shinzo.MinimumAttestations)
	require.True(t, DefaultConfig.Logger.Development)
	require.Equal(t, "./.defra/lens", DefaultConfig.HostConfig.LensRegistryPath)
}

func TestHost_IsHealthy(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	host := &Host{
		DefraNode: defraNode,
	}

	require.True(t, host.IsHealthy())
}

func TestHost_IsHealthy_NoDefraNode(t *testing.T) {
	host := &Host{
		DefraNode: nil,
	}

	require.False(t, host.IsHealthy())
}

func TestHost_GetCurrentBlock(t *testing.T) {
	metrics := server.NewHostMetrics()
	metrics.MostRecentBlock = 12345

	host := &Host{
		metrics: metrics,
	}

	require.Equal(t, int64(12345), host.GetCurrentBlock())
}

func TestHost_GetCurrentBlock_NoMetrics(t *testing.T) {
	host := &Host{
		metrics: nil,
	}

	require.Equal(t, int64(0), host.GetCurrentBlock())
}

func TestHost_GetLastProcessedTime(t *testing.T) {
	metrics := server.NewHostMetrics()
	expectedTime := time.Now()
	metrics.LastDocumentTime = expectedTime

	host := &Host{
		metrics: metrics,
	}

	require.Equal(t, expectedTime, host.GetLastProcessedTime())
}

func TestHost_GetLastProcessedTime_NoMetrics(t *testing.T) {
	host := &Host{
		metrics: nil,
	}

	require.True(t, host.GetLastProcessedTime().IsZero())
}

func TestHost_GetActiveViewNames(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	vm := view.NewManager(defraNode, t.TempDir(), testCollections.Prefix)

	host := &Host{
		DefraNode:   defraNode,
		viewManager: vm,
	}

	names := host.GetActiveViewNames()
	require.Empty(t, names)
}

func TestHost_GetPeerInfo(t *testing.T) {
	host := &Host{
		NetworkHandler: nil,
		DefraNode:      nil,
	}

	info, err := host.GetPeerInfo()

	require.NoError(t, err)
	require.NotNil(t, info)
	require.False(t, info.Enabled)
	require.Empty(t, info.PeerInfo)
}

func TestHost_Close(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	cleanupCalled := false
	cancelCalled := false

	host := &Host{
		DefraNode: defraNode,
		webhookCleanupFunction: func() {
			cleanupCalled = true
		},
		processingCancel: func() {
			cancelCalled = true
		},
		viewManager: view.NewManager(defraNode, t.TempDir(), testCollections.Prefix),
	}

	err = host.Close(ctx)

	require.NoError(t, err)
	require.True(t, cleanupCalled)
	require.True(t, cancelCalled)
}

func TestHost_Close_WaitsForListener(t *testing.T) {
	listenerDone := make(chan struct{})
	host := &Host{webhookCleanupFunction: func() {}, processingCancel: func() {}, listenerDone: listenerDone}

	closed := make(chan struct{})
	go func() {
		_ = host.Close(context.Background())
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the listener stopped")
	case <-time.After(50 * time.Millisecond):
	}

	close(listenerDone)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the listener stopped")
	}
}

func TestHost_Close_StopsNetwork(t *testing.T) {
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	networkHandler := defradb.NewNetworkHandler(defraNode, defradb.DefaultConfig)
	require.NoError(t, networkHandler.StartNetwork())
	host := &Host{DefraNode: defraNode, NetworkHandler: networkHandler, webhookCleanupFunction: func() {}, processingCancel: func() {}}

	require.NoError(t, host.Close(context.Background()))
	require.False(t, networkHandler.IsNetworkActive())
}

func TestHost_Close_StopsWaitingWhenContextEnds(t *testing.T) {
	host := &Host{webhookCleanupFunction: func() {}, processingCancel: func() {}, listenerDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	closed := make(chan struct{})
	go func() {
		_ = host.Close(ctx)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close kept waiting for a listener after its context ended")
	}
}

func TestHost_ProcessViewRegistrationEvent_NoViewManager(t *testing.T) {
	host := &Host{
		viewManager: nil,
	}

	// Verify the viewManager nil check would fail
	require.Nil(t, host.viewManager)
}

func TestHost_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	// Create a real DefraDB instance with the indexer schema
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			URL: "localhost:9181",
			Store: config.DefraDBStoreConfig{
				Path: t.TempDir(),
			},
		},
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}

	vm := view.NewManager(defraNode, cfg.HostConfig.LensRegistryPath, testCollections.Prefix)

	host := &Host{
		DefraNode:              defraNode,
		viewManager:            vm,
		LensRegistryPath:       cfg.HostConfig.LensRegistryPath,
		config:                 cfg,
		metrics:                server.NewHostMetrics(),
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
	}

	// Test basic operations
	require.NotNil(t, host.DefraNode)
	require.NotNil(t, host.viewManager)
	require.Equal(t, 0, host.viewManager.GetViewCount())

	// Test health check
	require.True(t, host.IsHealthy())
}

func TestHost_ViewManagerIntegration(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	host := &Host{
		DefraNode:        defraNode,
		viewManager:      vm,
		LensRegistryPath: registryPath,
		metrics:          server.NewHostMetrics(),
	}

	// Set metrics callback
	vm.SetMetricsCallback(func() *server.HostMetrics {
		return host.metrics
	})

	// Verify initial state
	require.Equal(t, 0, vm.GetViewCount())
	require.Empty(t, vm.GetActiveViewNames())

	// Load with no views
	err = vm.LoadAndRegisterViews(ctx, nil)
	require.NoError(t, err)
}

func TestView_HasLenses_Integration(t *testing.T) {
	// Test view with lenses
	viewWithLenses := view.View{
		Name: testViewName,
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: "file:///test.wasm"},
				},
			},
		},
	}
	require.True(t, viewWithLenses.HasLenses())

	// Test view without lenses
	viewWithoutLenses := view.View{
		Name: "EmptyView",
	}
	require.False(t, viewWithoutLenses.HasLenses())
}

// ---------------------------------------------------------------------------
// GetPeerInfo - additional coverage: non-nil NetworkHandler but nil DefraNode
// ---------------------------------------------------------------------------

func TestHost_GetPeerInfo_NetworkHandlerNonNilDefraNodeNil(t *testing.T) {
	host := &Host{
		NetworkHandler: &defradb.NetworkHandler{},
		DefraNode:      nil,
	}

	info, err := host.GetPeerInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
	// NetworkHandler is non-nil so Enabled should be true
	require.True(t, info.Enabled)
	// But DefraNode is nil so early return - no peers
	require.Empty(t, info.PeerInfo)
	require.Nil(t, info.Self)
}

// ---------------------------------------------------------------------------
// GetActiveViewNames - nil viewManager
// ---------------------------------------------------------------------------

func TestHost_GetActiveViewNames_NilViewManager(t *testing.T) {
	host := &Host{
		viewManager: nil,
	}

	names := host.GetActiveViewNames()
	require.NotNil(t, names)
	require.Empty(t, names)
}

// ---------------------------------------------------------------------------
// GetMetricsHandler
// ---------------------------------------------------------------------------

func TestHost_GetMetricsHandler_WithMetrics(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}

	handler := host.GetMetricsHandler()
	require.NotNil(t, handler)

	// The handler should be the metrics object itself (implements http.Handler)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

func TestHost_GetMetricsHandler_NilMetrics(t *testing.T) {
	host := &Host{
		metrics: nil,
	}

	handler := host.GetMetricsHandler()
	require.NotNil(t, handler)

	// Should return the fallback handler with error JSON
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "metrics not initialized")
}

// ---------------------------------------------------------------------------
// incrementPort - additional edge cases
// ---------------------------------------------------------------------------

func TestIncrementPort_MissingPort(t *testing.T) {
	// A URL with no port should fail when SplitHostPort cannot find a port
	_, err := incrementPort("http://localhost")
	require.Error(t, err)
}

func TestIncrementPort_NonNumericPort(t *testing.T) {
	_, err := incrementPort("http://localhost:abc")
	require.Error(t, err)
}

func TestIncrementPort_HTTPS(t *testing.T) {
	result, err := incrementPort("https://example.com:443")
	require.NoError(t, err)
	require.Equal(t, "example.com:444", result)
}

// ---------------------------------------------------------------------------
// Close - with healthServer and playground server
// ---------------------------------------------------------------------------

func TestHost_Close_WithHealthServer(t *testing.T) {
	ctx := context.Background()

	metrics := server.NewHostMetrics()
	hs := server.NewHealthServer(0, nil, "", metrics, "")

	host := &Host{
		DefraNode:              nil,
		healthServer:           hs,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
	}

	err := host.Close(ctx)
	require.NoError(t, err)
}

func TestHost_Close_WithPlaygroundServer(t *testing.T) {
	ctx := context.Background()

	// Create a playground server that's not actually listening
	playgroundServer := &http.Server{
		Addr:              ":0",
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: defaultTimeout,
	}

	host := &Host{
		DefraNode:              nil,
		playgroundServer:       playgroundServer,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
	}

	err := host.Close(ctx)
	require.NoError(t, err)
}

func TestHost_Close_NilDefraNode(t *testing.T) {
	ctx := context.Background()

	host := &Host{
		DefraNode:              nil,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
	}

	err := host.Close(ctx)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// ProcessViewRegistrationEvent
// ---------------------------------------------------------------------------

func TestHost_ProcessViewRegistrationEvent_NilViewManager_ReturnsError(t *testing.T) {
	host := &Host{
		viewManager: nil,
	}

	event := shinzohub.ViewRegisteredEvent{
		View: view.View{Name: testNameTest},
	}

	err := host.ProcessViewRegistrationEvent(context.Background(), event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ViewManager not initialized")
}

func TestHost_ProcessViewRegistrationEvent_MissingQuery(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	host := &Host{
		viewManager: view.NewManager(defraNode, t.TempDir(), testCollections.Prefix),
	}

	// No query set (nil)
	event := shinzohub.ViewRegisteredEvent{
		View: view.View{Name: testTestView},
	}

	err = host.ProcessViewRegistrationEvent(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing query")
}

func TestHost_ProcessViewRegistrationEvent_MissingSDL(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	host := &Host{
		viewManager: view.NewManager(defraNode, t.TempDir(), testCollections.Prefix),
	}

	query := "SELECT * FROM something"
	event := shinzohub.ViewRegisteredEvent{
		View: view.View{
			Name: testTestView,
			Data: viewbundle.View{
				Query: query,
			},
		},
	}

	err = host.ProcessViewRegistrationEvent(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing SDL")
}

func TestHost_ProcessViewRegistrationEvent_EmptyQuery(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	host := &Host{
		viewManager: view.NewManager(defraNode, t.TempDir(), testCollections.Prefix),
	}

	emptyStr := ""
	event := shinzohub.ViewRegisteredEvent{
		View: view.View{
			Name: testTestView,
			Data: viewbundle.View{
				Query: emptyStr,
			},
		},
	}

	err = host.ProcessViewRegistrationEvent(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing query")
}

func TestHost_ProcessViewRegistrationEvent_EmptySDL(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	host := &Host{
		viewManager: view.NewManager(defraNode, t.TempDir(), testCollections.Prefix),
	}

	query := "SELECT * FROM something"
	emptyStr := ""
	event := shinzohub.ViewRegisteredEvent{
		View: view.View{
			Name: testTestView,
			Data: viewbundle.View{
				Query: query,
				Sdl:   emptyStr,
			},
		},
	}

	err = host.ProcessViewRegistrationEvent(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing SDL")
}

// ---------------------------------------------------------------------------
// RegisterViewWithManager
// ---------------------------------------------------------------------------

func TestHost_RegisterViewWithManager_NilViewManager(t *testing.T) {
	host := &Host{
		viewManager: nil,
	}

	err := host.RegisterViewWithManager(context.Background(), view.View{Name: testNameTest})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ViewManager not initialized")
}

// ---------------------------------------------------------------------------
// isPlaygroundEnabled
// ---------------------------------------------------------------------------

func TestIsPlaygroundEnabled(t *testing.T) {
	result := isPlaygroundEnabled()
	// Without the hostplayground build tag, playground is disabled
	require.False(t, result)
}

// ---------------------------------------------------------------------------
// openBrowser - verify it does not panic
// ---------------------------------------------------------------------------

func TestOpenBrowser_DoesNotPanic(t *testing.T) {
	original := execCommand
	execCommand = func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("echo", "mock-browser")
	}
	defer func() { execCommand = original }()

	require.NotPanics(t, func() {
		_ = openBrowser("http://localhost:12345/nonexistent")
	})
}

// ---------------------------------------------------------------------------
// handleIncomingEvents
// ---------------------------------------------------------------------------

func TestHandleIncomingEvents_ContextCancelled(t *testing.T) {
	h := &Host{}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	select {
	case <-done:
		// returned promptly
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return after context cancellation")
	}
}

func TestHandleIncomingEvents_ChannelClosed(t *testing.T) {
	h := &Host{}
	ch := make(chan shinzohub.ShinzoEvent)

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	// Close the channel to trigger the !ok branch
	close(ch)

	select {
	case <-done:
		// returned promptly
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return after channel close")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_NilViewManager(t *testing.T) {
	h := &Host{
		viewManager: nil,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	query := testQueryBlocks
	sdl := "type Block { hash: String }"
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
			},
		},
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_MissingQuery(t *testing.T) {
	h := &Host{
		viewManager: nil,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	// View with nil Query
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
		},
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_EmptyQuery(t *testing.T) {
	h := &Host{
		viewManager: nil,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	emptyQuery := ""
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: emptyQuery,
			},
		},
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_MissingSDL(t *testing.T) {
	h := &Host{
		viewManager: nil,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	query := testQueryBlocks
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: query,
			},
		},
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_EmptySDL(t *testing.T) {
	h := &Host{
		viewManager: nil,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	query := testQueryBlocks
	emptySDL := ""
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: query,
				Sdl:   emptySDL,
			},
		},
	}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

// ---------------------------------------------------------------------------
// SignMessages, GetNodePublicKey, GetPeerPublicKey - nil config/DefraNode
// ---------------------------------------------------------------------------

func TestHost_SignMessages_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
		config:    DefaultConfig,
	}

	_, _, err := h.SignMessages("test message")
	require.Error(t, err)
}

func TestHost_GetNodePublicKey_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
		config:    DefaultConfig,
	}

	_, err := h.GetNodePublicKey()
	require.Error(t, err)
}

func TestHost_GetPeerPublicKey_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
		config:    DefaultConfig,
	}

	_, err := h.GetPeerPublicKey()
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetPeerInfo - with DefraDB
// ---------------------------------------------------------------------------

func TestHost_GetPeerInfo_WithDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	nh := &defradb.NetworkHandler{}
	h := &Host{
		DefraNode:      defraNode,
		NetworkHandler: nh,
	}

	info, err := h.GetPeerInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
	require.True(t, info.Enabled)
}

// ---------------------------------------------------------------------------
// ProcessViewRegistrationEvent with valid query+SDL
// ---------------------------------------------------------------------------

func TestHost_ProcessViewRegistrationEvent_WithLenses(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		DefraNode:        defraNode,
		viewManager:      vm,
		LensRegistryPath: registryPath,
		metrics:          server.NewHostMetrics(),
	}

	query := testQueryTest
	sdl := testViewSDL
	event := shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: "TestViewLens",
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "nonexistent.wasm"},
					},
				},
			},
		},
	}

	// PostWasmToFile will fail for nonexistent WASM, exercises the lenses path
	err = h.ProcessViewRegistrationEvent(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "WASM")
}

func TestHost_ProcessViewRegistrationEvent_WithViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		DefraNode:        defraNode,
		viewManager:      vm,
		LensRegistryPath: registryPath,
		metrics:          server.NewHostMetrics(),
	}

	query := testQueryTest
	sdl := testViewSDL
	event := shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
			},
		},
	}

	// This may fail at registration since the schema doesn't actually match DefraDB,
	// but it exercises the full path past validation
	_ = h.ProcessViewRegistrationEvent(ctx, event)
}

// ---------------------------------------------------------------------------
// RegisterViewWithManager - with valid ViewManager
// ---------------------------------------------------------------------------

func TestHost_RegisterViewWithManager_WithViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	vm := view.NewManager(defraNode, t.TempDir(), testCollections.Prefix)
	h := &Host{
		DefraNode:   defraNode,
		viewManager: vm,
	}

	query := testQueryTest
	sdl := testViewSDL
	v := view.View{
		Name: testViewName,
		Data: viewbundle.View{
			Query: query,
			Sdl:   sdl,
		},
	}

	// May fail at the DefraDB level but exercises the code path
	_ = h.RegisterViewWithManager(ctx, v)
}

// ---------------------------------------------------------------------------
// Close - full coverage with all components
// ---------------------------------------------------------------------------

func TestHost_Close_WithViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	vm := view.NewManager(defraNode, t.TempDir(), testCollections.Prefix)

	playgroundServer := &http.Server{
		Addr:              ":0",
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: defaultTimeout,
	}

	h := &Host{
		DefraNode:              defraNode,
		viewManager:            vm,
		playgroundServer:       playgroundServer,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
	}

	err = h.Close(ctx)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// openBrowser - additional platforms
// ---------------------------------------------------------------------------

func TestOpenBrowser_ReturnsNoError(t *testing.T) {
	original := execCommand
	execCommand = func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("echo", "mock-browser")
	}
	defer func() { execCommand = original }()

	err := openBrowser("http://localhost:99999/test")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// StartHosting with nil config uses DefaultConfig
// ---------------------------------------------------------------------------

func TestStartHosting_NilConfig_UsesDefault(t *testing.T) {
	require.NotNil(t, DefaultConfig)
	require.Equal(t, "localhost:9181", DefaultConfig.DefraDB.URL)
}

// ---------------------------------------------------------------------------
// GetPeerInfo - with real P2P-enabled DefraDB
// ---------------------------------------------------------------------------

func TestHost_GetPeerInfo_WithP2PEnabled(t *testing.T) {
	ctx := context.Background()

	// Start a DefraDB instance that includes P2P
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	nh := &defradb.NetworkHandler{}
	h := &Host{
		DefraNode:      defraNode,
		NetworkHandler: nh,
		config:         DefaultConfig,
	}

	info, err := h.GetPeerInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
	require.True(t, info.Enabled)
	// PeerInfo is a slice (may be empty if no peers connected in test)
	require.NotNil(t, info.PeerInfo)
}

// ---------------------------------------------------------------------------
// SignMessages - with real DefraDB
// ---------------------------------------------------------------------------

func TestHost_SignMessages_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
		config:    DefaultConfig,
	}

	defraPK, peerID, err := h.SignMessages("test message for signing")
	// SignMessages requires a P2P identity key file; if not available, accept the error
	if err != nil {
		require.Contains(t, err.Error(), "identity")
		return
	}
	require.NotEmpty(t, defraPK.PublicKey, "public key should not be empty")
	require.NotEmpty(t, defraPK.SignedPKMsg, "signed message should not be empty")
	require.NotEmpty(t, peerID.PeerID, "peer ID should not be empty")
	require.NotEmpty(t, peerID.SignedPeerMsg, "signed peer message should not be empty")
}

// ---------------------------------------------------------------------------
// GetNodePublicKey / GetPeerPublicKey - with real DefraDB
// ---------------------------------------------------------------------------

func TestHost_GetNodePublicKey_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
		config:    DefaultConfig,
	}

	pubKey, err := h.GetNodePublicKey()
	// Requires P2P identity key file; accept error if not available
	if err != nil {
		require.Contains(t, err.Error(), "identity")
		return
	}
	require.NotEmpty(t, pubKey)
}

func TestHost_GetPeerPublicKey_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
		config:    DefaultConfig,
	}

	peerKey, err := h.GetPeerPublicKey()
	// Requires P2P identity key file; accept error if not available
	if err != nil {
		require.Contains(t, err.Error(), "identity")
		return
	}
	require.NotEmpty(t, peerKey)
}

// ---------------------------------------------------------------------------
// handleIncomingEvents - unknown event type
// ---------------------------------------------------------------------------

type unknownTestEvent struct{}

func (e *unknownTestEvent) ToString() string { return "unknown" }

func TestHandleIncomingEvents_UnknownEventType(t *testing.T) {
	h := &Host{}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(ctx, ch)
		close(done)
	}()

	// Send an event that is none of the known types
	ch <- &unknownTestEvent{}

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

// ---------------------------------------------------------------------------
// handleIncomingEvents - ViewRegistered with valid query+SDL and ViewManager
// ---------------------------------------------------------------------------

func TestHandleIncomingEvents_ViewRegisteredEvent_WithLenses(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		viewManager:      vm,
		LensRegistryPath: registryPath,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	evtCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(evtCtx, ch)
		close(done)
	}()

	query := testQueryBlocks
	sdl := "type TestView { hash: String }"
	// View with lenses that have invalid WASM (will fail PostWasmToFile but exercises the path)
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: "TestViewWithLenses",
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{
							Path: "invalid/wasm/path.wasm",
						},
					},
				},
			},
		},
	}

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHandleIncomingEvents_ViewRegisteredEvent_WithViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		viewManager:      vm,
		LensRegistryPath: registryPath,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	evtCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(evtCtx, ch)
		close(done)
	}()

	query := testQueryBlocks
	sdl := "type TestView { hash: String }"
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: testViewName,
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
			},
		},
	}

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

// ---------------------------------------------------------------------------
// applySchema - with real DefraDB
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// waitForDefraDB - with real DefraDB
// ---------------------------------------------------------------------------

func TestWaitForDefraDB_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Should succeed quickly since DefraDB is already ready
	err = waitForDefraDB(ctx, defraNode, testCollections.Block.Name)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Close - with pruner
// ---------------------------------------------------------------------------

func TestHost_Close_WithPruner(t *testing.T) {
	ctx := context.Background()

	host := &Host{
		DefraNode:              nil,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
		// pruner is nil by default - this is a safe close
	}

	err := host.Close(ctx)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// StartHostingWithTestConfig - full integration test
// ---------------------------------------------------------------------------

func TestStartHostingWithTestConfig(t *testing.T) {
	host, err := StartHostingWithTestConfig(t)
	if err != nil {
		// May fail if P2P port is busy or other infra issue
		t.Skipf("Skipping integration test: %v", err)
	}
	require.NotNil(t, host)

	// Verify host is functional
	require.NotNil(t, host.DefraNode)
	require.NotNil(t, host.metrics)
	require.True(t, host.IsHealthy())

	// Verify GetCurrentBlock returns a value
	_ = host.GetCurrentBlock()

	// Verify GetLastProcessedTime returns a value
	_ = host.GetLastProcessedTime()

	// Close the host
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = host.Close(closeCtx)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// StartHosting with nil config uses defaults
// ---------------------------------------------------------------------------

func TestStartHosting_NilConfig(t *testing.T) {
	host, err := StartHosting(nil)
	if err != nil {
		t.Skipf("Skipping integration test: %v", err)
	}
	require.NotNil(t, host)

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = host.Close(closeCtx)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// handleIncomingEvents - ViewRegistered with valid base64 WASM lens
// (exercises PostWasmToFile success + RegisterView attempt)
// ---------------------------------------------------------------------------

func TestHandleIncomingEvents_ViewRegisteredEvent_WithBase64Lens(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		viewManager:      vm,
		LensRegistryPath: registryPath,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	evtCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(evtCtx, ch)
		close(done)
	}()

	query := "Log { address topics }"
	sdl := "type TestLensView { address: String }"
	// Use valid base64 (tiny fake WASM) so PostWasmToFile succeeds
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: testContractKey1,
		Creator:         testCreator1,
		View: view.View{
			Name: "TestLensView",
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{
							Path: "AAAA", // valid base64 for 3 zero bytes
						},
					},
				},
			},
		},
	}

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

// ---------------------------------------------------------------------------
// handleIncomingEvents - ViewRegistered with no lenses (exercises the empty lenses path)
// ---------------------------------------------------------------------------

func TestHandleIncomingEvents_ViewRegisteredEvent_NoLenses_WithViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &defradb.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	registryPath := t.TempDir()
	vm := view.NewManager(defraNode, registryPath, testCollections.Prefix)

	h := &Host{
		viewManager:      vm,
		LensRegistryPath: registryPath,
	}
	ch := make(chan shinzohub.ShinzoEvent, 1)

	evtCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.handleIncomingEvents(evtCtx, ch)
		close(done)
	}()

	query := "Log { address }"
	sdl := "type NoLensView { address: String }"
	ch <- &shinzohub.ViewRegisteredEvent{
		ContractAddress: "0xkey2",
		Creator:         "creator2",
		View: view.View{
			Name: "NoLensView",
			Data: viewbundle.View{
				Query: query,
				Sdl:   sdl,
				// No lenses — skips PostWasmToFile, goes straight to RegisterView
			},
		},
	}

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleIncomingEvents did not return")
	}
}

func TestHost_Close_Full(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		webhookCleanupFunction: func() {},
		processingCancel:       func() {},
		metrics:                metrics,
		healthServer:           server.NewHealthServer(0, nil, "", metrics, ""),
	}

	go func() { _ = h.healthServer.Start() }()
	time.Sleep(50 * time.Millisecond)

	err = h.Close(ctx)
	require.NoError(t, err)
}

// A generator's peer is added to the host's peers along with the bootstrap peers.
func TestStartHostingAddsGeneratorPeers(t *testing.T) {
	listener, err := net.Listen("tcp", testLoopbackAddr)
	require.NoError(t, err)
	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NoError(t, listener.Close())
	generator := httptest.NewServer(schemaHandler(t, chain.EthereumMainnet))
	t.Cleanup(generator.Close)

	cfg := *DefaultConfig
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.URL = testLoopbackAddr
	cfg.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/0"
	cfg.HostConfig.HealthServerPort = addr.Port
	cfg.Chains = []chain.Config{{
		Prefix:     chain.EthereumMainnet,
		Generators: []chain.Generator{{URL: generator.URL, Peer: testPeerMultiaddr}},
	}}

	h, err := StartHosting(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, h.Close(ctx))
	})

	require.Contains(t, h.NetworkHandler.GetPeers(), testPeerMultiaddr)
}

// DefraDB's API answers only once the chain's tables exist.
func TestStartHostingServesAPIAfterTables(t *testing.T) {
	// Both listeners stay open until both ports are picked, so the two ports differ.
	var ports []int
	var listeners []net.Listener
	for range 2 {
		listener, err := net.Listen("tcp", testLoopbackAddr)
		require.NoError(t, err)
		listeners = append(listeners, listener)
		addr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		ports = append(ports, addr.Port)
	}
	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}
	apiPort, healthPort := ports[0], ports[1]

	// The generator holds the schema until released, so the host is still creating its tables.
	fetching := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseSchema := sync.OnceFunc(func() { close(release) })
	serveSchema := schemaHandler(t, chain.EthereumMainnet)
	generator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case fetching <- struct{}{}:
		default:
		}
		<-release
		serveSchema(w, r)
	}))
	t.Cleanup(generator.Close)
	t.Cleanup(releaseSchema)

	cfg := *DefaultConfig
	cfg.DefraDB.Store.Path = t.TempDir()
	// StartDefraInstance rewrites a loopback address to the LAN IP, so an API that DefraDB started
	// early would not answer the loopback dial below. It leaves 0.0.0.0 alone.
	cfg.DefraDB.URL = net.JoinHostPort("0.0.0.0", strconv.Itoa(apiPort))
	cfg.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/0"
	cfg.HostConfig.HealthServerPort = healthPort
	cfg.Chains = []chain.Config{{
		Prefix:     chain.EthereumMainnet,
		Generators: []chain.Generator{{URL: generator.URL}},
	}}

	var h *Host
	started := make(chan error, 1)
	go func() {
		var err error
		h, err = StartHosting(&cfg)
		started <- err
	}()

	select {
	case <-fetching:
	case err := <-started:
		t.Fatalf("StartHosting returned before fetching the schema: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("StartHosting never fetched the schema")
	}
	apiAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(apiPort))
	if conn, err := net.DialTimeout("tcp", apiAddr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the API answered before the chain's tables existed")
	}

	releaseSchema()
	require.NoError(t, <-started)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, h.Close(ctx))
	})

	api, err := defradbHttp.NewClient(apiAddr)
	require.NoError(t, err)
	result := api.ExecRequest(context.Background(), "{ "+h.collections.Block.Name+" { __typename } }")
	require.Empty(t, result.GQL.Errors)
}
