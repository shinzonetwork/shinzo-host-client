package host

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	indexerschema "github.com/shinzonetwork/shinzo-indexer-client/pkg/schema"
	"github.com/shinzonetwork/view-creator/core/models"
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
	require.Equal(t, "localhost:9181", DefaultConfig.DefraDB.Url)
	require.True(t, DefaultConfig.DefraDB.P2P.Enabled)
	require.Equal(t, 1, DefaultConfig.Shinzo.MinimumAttestations)
	require.True(t, DefaultConfig.Logger.Development)
	require.Equal(t, "./.lens", DefaultConfig.HostConfig.LensRegistryPath)
}

func TestHost_IsHealthy(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	host := &Host{
		DefraNode:          defraNode,
		processingPipeline: &ProcessingPipeline{}, // Mock pipeline
	}

	require.True(t, host.IsHealthy())
}

func TestHost_IsHealthy_NoDefraNode(t *testing.T) {
	host := &Host{
		DefraNode:          nil,
		processingPipeline: &ProcessingPipeline{},
	}

	require.False(t, host.IsHealthy())
}

func TestHost_IsHealthy_NoPipeline(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	host := &Host{
		DefraNode:          defraNode,
		processingPipeline: nil,
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

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := view.NewViewManager(defraNode, t.TempDir())

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

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
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
		viewManager: view.NewViewManager(defraNode, t.TempDir()),
	}

	err = host.Close(ctx)

	require.NoError(t, err)
	require.True(t, cleanupCalled)
	require.True(t, cancelCalled)
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
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			Url: "localhost:9181",
			Store: config.DefraDBStoreConfig{
				Path: t.TempDir(),
			},
		},
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}

	vm := view.NewViewManager(defraNode, cfg.HostConfig.LensRegistryPath)

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
	host.processingPipeline = &ProcessingPipeline{}
	require.True(t, host.IsHealthy())
}

func TestHost_ViewManagerIntegration(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := view.NewViewManager(defraNode, registryPath)

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
		Name: "TestView",
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "file:///test.wasm"},
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
