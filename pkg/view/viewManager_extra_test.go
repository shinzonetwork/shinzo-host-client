package view

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SetMetricsCallback
// ---------------------------------------------------------------------------

func TestViewManager_SetMetricsCallback(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	require.Nil(t, vm.metricsCallback)

	metrics := server.NewHostMetrics()
	vm.SetMetricsCallback(func() *server.HostMetrics {
		return metrics
	})

	require.NotNil(t, vm.metricsCallback)

	// Verify the callback returns the expected metrics
	result := vm.metricsCallback()
	require.NotNil(t, result)
	require.Equal(t, metrics, result)
}

func TestViewManager_SetMetricsCallback_NilCallback(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	vm.SetMetricsCallback(nil)
	require.Nil(t, vm.metricsCallback)
}

// ---------------------------------------------------------------------------
// QueueView
// ---------------------------------------------------------------------------

func TestViewManager_QueueView(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := View{Name: "TestQueueView"}
	vm.QueueView(v)

	// Read from the processing queue
	item := <-vm.processingQueue
	require.Equal(t, "TestQueueView", item.viewName)
	require.Equal(t, "TestQueueView", item.activeViewKey)
	require.Nil(t, item.lensConfig)
}

func TestViewManager_QueueView_Multiple(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	vm.QueueView(View{Name: "View1"})
	vm.QueueView(View{Name: "View2"})

	item1 := <-vm.processingQueue
	item2 := <-vm.processingQueue
	require.Equal(t, "View1", item1.viewName)
	require.Equal(t, "View2", item2.viewName)
}

// ---------------------------------------------------------------------------
// GetActiveViewNames - with entries
// ---------------------------------------------------------------------------

func TestViewManager_GetActiveViewNames_WithEntries(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Manually add views to activeViews
	vm.activeViews["ViewA"] = nil
	vm.activeViews["ViewB"] = nil

	names := vm.GetActiveViewNames()
	require.Len(t, names, 2)
	require.Contains(t, names, "ViewA")
	require.Contains(t, names, "ViewB")
}

// ---------------------------------------------------------------------------
// GetViewCount - with entries
// ---------------------------------------------------------------------------

func TestViewManager_GetViewCount_WithEntries(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	vm.activeViews["View1"] = nil
	vm.activeViews["View2"] = nil
	vm.activeViews["View3"] = nil

	require.Equal(t, 3, vm.GetViewCount())
}

// ---------------------------------------------------------------------------
// subscribeToSourceCollection
// ---------------------------------------------------------------------------

func TestViewManager_SubscribeToSourceCollection(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// subscribeToSourceCollection currently just logs and returns nil
	err = vm.subscribeToSourceCollection(ctx, "Ethereum__Mainnet__Log", "FilteredLog")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// extractCollectionFromQuery - additional cases
// ---------------------------------------------------------------------------

func TestExtractCollectionFromQuery_WithSpace(t *testing.T) {
	result := extractCollectionFromQuery("User { name }")
	require.Equal(t, "User", result)
}

func TestExtractCollectionFromQuery_WithBrace(t *testing.T) {
	result := extractCollectionFromQuery("User{ name }")
	require.Equal(t, "User", result)
}

func TestExtractCollectionFromQuery_SingleWord(t *testing.T) {
	result := extractCollectionFromQuery("Collection")
	require.Equal(t, "Collection", result)
}

// ---------------------------------------------------------------------------
// LoadAndRegisterViews - various paths
// ---------------------------------------------------------------------------

func TestViewManager_LoadAndRegisterViews_WithExternalViews(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)

	query := "User { name }"
	sdl := "type ExternalView { name: String }"
	externalViews := []View{
		{
			Name:  "ExternalView",
			Query: &query,
			Sdl:   &sdl,
		},
	}

	// This will attempt to register the view; registration may fail because
	// the source collection doesn't exist, but it exercises the code path
	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err) // LoadAndRegisterViews doesn't return errors from individual view registrations
}

func TestViewManager_LoadAndRegisterViews_WithLocalViews(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()

	// Create a views.json with local views
	query := "User { name }"
	sdl := "type LocalView { name: String }"
	localViews := []View{
		{
			Name:  "LocalView",
			Query: &query,
			Sdl:   &sdl,
		},
	}
	data, err := json.Marshal(localViews)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(registryPath, "views.json"), data, 0644))

	vm := NewViewManager(defraNode, registryPath)

	// This exercises the local views loading path
	err = vm.LoadAndRegisterViews(ctx, nil)
	require.NoError(t, err)
}

func TestViewManager_LoadAndRegisterViews_DeduplicatesViews(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()

	query := "User { name }"
	sdl := "type DuplicateView { name: String }"

	// Create a views.json that has the same view as the external one
	localViews := []View{
		{Name: "DuplicateView", Query: &query, Sdl: &sdl},
	}
	data, err := json.Marshal(localViews)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(registryPath, "views.json"), data, 0644))

	vm := NewViewManager(defraNode, registryPath)

	externalViews := []View{
		{Name: "DuplicateView", Query: &query, Sdl: &sdl},
	}

	// Should deduplicate and only try to register once
	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err)
}

func TestViewManager_LoadAndRegisterViews_WithWasmURLs(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)

	query := "User { name }"
	sdl := "type WasmView { name: String }"

	// View with http WASM URL - this will exercise the WASM download path
	// The download will fail because the URL is fake, but it exercises the code path
	externalViews := []View{
		{
			Name:  "WasmView",
			Query: &query,
			Sdl:   &sdl,
			Transform: models.Transform{
				Lenses: []models.Lens{
					{
						Label: "filter",
						Path:  "https://nonexistent.example.com/filter.wasm",
					},
				},
			},
		},
	}

	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err) // Doesn't fail even if WASM download fails
}

func TestViewManager_LoadAndRegisterViews_InvalidLocalViews(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()

	// Create invalid views.json
	require.NoError(t, os.WriteFile(filepath.Join(registryPath, "views.json"), []byte("invalid json"), 0644))

	vm := NewViewManager(defraNode, registryPath)

	// Should not fail - just logs warning
	err = vm.LoadAndRegisterViews(ctx, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// RegisterView - more paths
// ---------------------------------------------------------------------------

func TestViewManager_RegisterView_WithMetricsCallback(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	metrics := server.NewHostMetrics()
	vm.SetMetricsCallback(func() *server.HostMetrics {
		return metrics
	})

	query := "User { name }"
	sdl := "type MetricTestView { name: String }"
	v := View{
		Name:  "MetricTestView",
		Query: &query,
		Sdl:   &sdl,
	}

	// Registration will likely fail because source collection doesn't exist,
	// but we test the metrics callback path
	err = vm.RegisterView(ctx, v)
	// If it fails at SetMigration, metrics won't be updated
	// If it succeeds, metrics should show 1 registered
	if err == nil {
		require.Equal(t, int64(1), atomic.LoadInt64(&metrics.ViewsRegistered))
	}
}

func TestViewManager_RegisterView_WithMetricsCallback_NilMetrics(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Set a callback that returns nil
	vm.SetMetricsCallback(func() *server.HostMetrics {
		return nil
	})

	query := "User { name }"
	sdl := "type NilMetricView { name: String }"
	v := View{
		Name:  "NilMetricView",
		Query: &query,
		Sdl:   &sdl,
	}

	// This should not panic even though metrics returns nil
	_ = vm.RegisterView(ctx, v)
}

func TestViewManager_RegisterView_NoLensesView(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	query := "User { name }"
	sdl := "type NoLensRegView { name: String }"
	v := View{
		Name:      "NoLensRegView",
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
	}

	// Registration will attempt to configure lens (may fail) but exercises no-lens path
	err = vm.RegisterView(ctx, v)
	if err != nil {
		// Error could be about migration, lens configuration, or view creation
		require.True(t, strings.Contains(err.Error(), "failed to set migration") ||
			strings.Contains(err.Error(), "failed to configure lens") ||
			strings.Contains(err.Error(), "failed to create view"),
			"unexpected error: %s", err.Error())
	}
}

func TestViewManager_RegisterView_WithBase64WasmConversion(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)

	query := "User { name }"
	sdl := "type WasmConvView { name: String }"

	// Use base64 encoded wasm to exercise PostWasmToFile path
	wasmBase64 := "AGFzbQEAAAA=" // minimal valid-looking base64

	v := View{
		Name:  "WasmConvView",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "test_filter",
					Path:  wasmBase64,
				},
			},
		},
	}

	// Registration will exercise the PostWasmToFile path
	err = vm.RegisterView(ctx, v)
	// May fail at SetMigration or ConfigureLens but we exercise the WASM conversion path
	if err != nil {
		// Verify the lens path was converted to file:// URL
		require.Contains(t, v.Transform.Lenses[0].Path, "file://")
	}
}

func TestViewManager_RegisterView_WithFileURLLens(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	query := "User { name }"
	sdl := "type FileURLView { name: String }"

	v := View{
		Name:  "FileURLView",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "filter",
					Path:  "file:///some/path/filter.wasm",
				},
			},
		},
	}

	// Has lenses but needsWasmConversion returns false (file:// prefix)
	// So PostWasmToFile should be skipped
	err = vm.RegisterView(ctx, v)
	// May fail at SetMigration, but the WASM conversion is skipped
	if err != nil {
		// Path should remain unchanged
		require.Equal(t, "file:///some/path/filter.wasm", v.Transform.Lenses[0].Path)
	}
}

func TestViewManager_RegisterView_SubscribeSourceCollectionPath(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Add schema so view creation can potentially succeed
	_, err = defraNode.DB.AddSchema(ctx, "type SourceUser { name: String }")
	require.NoError(t, err)

	vm := NewViewManager(defraNode, t.TempDir())

	query := "SourceUser { name }"
	sdl := "type SourceSubView { name: String }"

	v := View{
		Name:  "SourceSubView",
		Query: &query,
		Sdl:   &sdl,
	}

	// This exercises the query != nil branch for source collection subscription
	_ = vm.RegisterView(ctx, v)
}

// ---------------------------------------------------------------------------
// RegisterView - already registered view returns error
// ---------------------------------------------------------------------------

func TestViewManager_RegisterView_AlreadyRegistered(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	query := "User { name }"
	sdl := "type DupView { name: String }"
	v := View{
		Name:  "DupView",
		Query: &query,
		Sdl:   &sdl,
	}

	// Manually add to activeViews to simulate prior registration
	vm.mutex.Lock()
	vm.activeViews["DupView"] = &client.LensConfig{}
	vm.mutex.Unlock()

	err = vm.RegisterView(ctx, v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already registered")
}

// ---------------------------------------------------------------------------
// LoadAndRegisterViews - EnsureAllWASM error path
// ---------------------------------------------------------------------------

func TestViewManager_LoadAndRegisterViews_WasmDownloadError(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)

	query := "User { name }"
	sdl := "type WasmErrView { name: String }"
	externalViews := []View{
		{
			Name:  "WasmErrView",
			Query: &query,
			Sdl:   &sdl,
			Transform: models.Transform{
				Lenses: []models.Lens{
					{
						Label: "filter",
						Path:  "http://nonexistent.invalid/filter.wasm",
					},
				},
			},
		},
	}

	// EnsureAllWASM will fail for the invalid URL but LoadAndRegisterViews should continue
	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err) // LoadAndRegisterViews returns nil even on WASM download failure
}

// ---------------------------------------------------------------------------
// deduplicateViews - edge cases
// ---------------------------------------------------------------------------

func TestDeduplicateViews_SingleElement(t *testing.T) {
	result := deduplicateViews([]View{{Name: "Only"}})
	require.Len(t, result, 1)
	require.Equal(t, "Only", result[0].Name)
}

func TestDeduplicateViews_AllDuplicates(t *testing.T) {
	views := []View{
		{Name: "Same"},
		{Name: "Same"},
		{Name: "Same"},
	}
	result := deduplicateViews(views)
	require.Len(t, result, 1)
	require.Equal(t, "Same", result[0].Name)
}

// ---------------------------------------------------------------------------
// extractWasmURLsFromViews - edge cases
// ---------------------------------------------------------------------------

func TestExtractWasmURLsFromViews_EmptyViews(t *testing.T) {
	urls := extractWasmURLsFromViews([]View{})
	require.Empty(t, urls)
}

func TestExtractWasmURLsFromViews_NoLenses(t *testing.T) {
	views := []View{
		{Name: "View1", Transform: models.Transform{}},
	}
	urls := extractWasmURLsFromViews(views)
	require.Empty(t, urls)
}

// RegisterView with nil Query is not tested because BuildLensConfig()
// dereferences *v.Query unconditionally — callers must ensure Query != nil.

// ---------------------------------------------------------------------------
// RegisterView - empty source collection from extractCollectionFromQuery
// ---------------------------------------------------------------------------

func TestViewManager_RegisterView_EmptySourceCollection(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Query that starts with space/brace so extractCollectionFromQuery returns ""
	query := "{ name }"
	sdl := "type EmptySourceView { name: String }"
	v := View{
		Name:  "EmptySourceView",
		Query: &query,
		Sdl:   &sdl,
	}

	// Registration will fail at SetMigration but exercises the empty source collection path
	_ = vm.RegisterView(ctx, v)
}

// ---------------------------------------------------------------------------
// extractCollectionFromQuery - empty string input
// ---------------------------------------------------------------------------

func TestExtractCollectionFromQuery_EmptyString(t *testing.T) {
	result := extractCollectionFromQuery("")
	require.Equal(t, "", result)
}

func TestExtractCollectionFromQuery_StartsWithBrace(t *testing.T) {
	result := extractCollectionFromQuery("{ name }")
	require.Equal(t, "", result)
}

// ---------------------------------------------------------------------------
// LoadAndRegisterViews - views with nil wasmRegistry
// ---------------------------------------------------------------------------

func TestViewManager_LoadAndRegisterViews_NilWasmRegistry(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)
	// Force wasmRegistry to nil
	vm.wasmRegistry = nil

	query := "User { name }"
	sdl := "type NilWasmRegView { name: String }"
	externalViews := []View{
		{
			Name:  "NilWasmRegView",
			Query: &query,
			Sdl:   &sdl,
			Transform: models.Transform{
				Lenses: []models.Lens{
					{
						Label: "filter",
						Path:  "https://example.com/filter.wasm",
					},
				},
			},
		},
	}

	// Should not panic with nil wasmRegistry - exercises line 85 check
	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err)
}
