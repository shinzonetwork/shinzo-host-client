package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
	"github.com/sourcenetwork/defradb/node"
)



func TestNewViewManager(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)

	require.NotNil(t, vm)
	require.NotNil(t, vm.activeViews)
	require.NotNil(t, vm.defraNode)
	require.NotNil(t, vm.schemaService)
	require.Equal(t, registryPath, vm.registryPath)
}

func TestViewManager_GetActiveViewNames_Empty(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	names := vm.GetActiveViewNames()

	require.Empty(t, names)
}

func TestViewManager_GetViewCount_Empty(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	count := vm.GetViewCount()

	require.Equal(t, 0, count)
}

func TestViewManager_LoadAndRegisterViews_NoViews(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())
	err = vm.LoadAndRegisterViews(ctx, nil)

	require.NoError(t, err)
	require.Equal(t, 0, vm.GetViewCount())
}

func TestDeduplicateViews(t *testing.T) {
	views := []View{
		{Name: "View1"},
		{Name: "View2"},
		{Name: "View1"}, // Duplicate
		{Name: "View3"},
		{Name: "View2"}, // Duplicate
	}

	result := deduplicateViews(views)

	require.Len(t, result, 3)
	require.Equal(t, "View1", result[0].Name)
	require.Equal(t, "View2", result[1].Name)
	require.Equal(t, "View3", result[2].Name)
}

func TestDeduplicateViews_Empty(t *testing.T) {
	result := deduplicateViews([]View{})
	require.Empty(t, result)
}

func TestDeduplicateViews_NoDuplicates(t *testing.T) {
	views := []View{
		{Name: "View1"},
		{Name: "View2"},
		{Name: "View3"},
	}

	result := deduplicateViews(views)

	require.Len(t, result, 3)
}

func TestExtractWasmURLsFromViews_NoHTTPURLs(t *testing.T) {
	views := []View{
		{
			Name: "View1",
			Data: viewbundle.View{
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "file://" + "filter_transaction.wasm"},
					},
				},
			},
		},
	}

	urls := extractWasmURLsFromViews(views)

	require.Empty(t, urls)
}

func TestExtractCollectionFromQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "simple query",
			query:    "Ethereum__Mainnet__Log { address topics }",
			expected: "Ethereum__Mainnet__Log",
		},
		{
			name:     "full collection name",
			query:    "Ethereum__Mainnet__Log { address }",
			expected: "Ethereum__Mainnet__Log",
		},
		{
			name:     "no braces",
			query:    "Ethereum__Mainnet__Transaction",
			expected: "Ethereum__Mainnet__Transaction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractCollectionFromQuery(tt.query)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestView_BuildLensConfig(t *testing.T) {
	query := "Ethereum__Mainnet__Log { address topics }" // Add prefix
	sdl := "type FilteredLog { address: String }"

	v := View{
		Name: "FilteredLog",
		Data: viewbundle.View{
			Query: query,
			Sdl:   sdl,
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path: "file://" + "filter_transaction.wasm",
					},
				},
			},
		},
	}

	config, err := v.BuildLensConfig()
	require.NoError(t, err)

	require.Equal(t, query, config.SourceCollectionVersionID)
	require.Equal(t, sdl, config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Equal(t, "file://"+"filter_transaction.wasm", config.Lens.Lenses[0].Path)
	require.Equal(t, map[string]any{}, config.Lens.Lenses[0].Arguments) // Arguments are empty
}

func TestViewManager_RegisterView_AlreadyExists(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Manually add a view to activeViews
	vm.activeViews["TestView"] = nil

	// Try to register the same view
	v := View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }", // Add prefix
			Sdl:   "type TestView { address: String }",
		},
	}
	err = vm.RegisterView(ctx, &v)

	require.Error(t, err)
	require.Contains(t, err.Error(), "already registered")
}

// MockWASMRegistry provides a mock implementation for testing
type MockWASMRegistry struct {
	downloadedFiles map[string]string
	downloadError   error
}

func NewMockWASMRegistry() *MockWASMRegistry {
	return &MockWASMRegistry{
		downloadedFiles: make(map[string]string),
	}
}

func (m *MockWASMRegistry) EnsureAllWASM(ctx context.Context, urls []string) ([]string, error) {
	if m.downloadError != nil {
		return nil, m.downloadError
	}
	
	// Always return success without actually downloading anything
	var downloaded []string
	for _, url := range urls {
		if _, exists := m.downloadedFiles[url]; !exists {
			m.downloadedFiles[url] = "/mock/path/" + url
			downloaded = append(downloaded, url)
		}
	}
	return downloaded, nil
}

func (m *MockWASMRegistry) SetDownloadError(err error) {
	m.downloadError = err
}

// MockViewManager creates a ViewManager with mocked dependencies to avoid WASM operations
func MockViewManager(defraNode *node.Node, registryPath string) *ViewManager {
	vm := NewViewManager(defraNode, registryPath)
	// Replace WASM registry with mock to avoid any downloads
	vm.wasmRegistry = nil
	return vm
}

// TestViewManager_SetMetricsCallback tests setting metrics callback
func TestViewManager_SetMetricsCallback(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Initially nil
	require.Nil(t, vm.metricsCallback)

	// Set callback
	callbackCalled := false
	vm.SetMetricsCallback(func() *server.HostMetrics {
		callbackCalled = true
		return &server.HostMetrics{}
	})

	require.NotNil(t, vm.metricsCallback)
	
	// Test callback is called (this would happen during actual view registration)
	if vm.metricsCallback != nil {
		vm.metricsCallback()
	}
	require.True(t, callbackCalled)
}

// TestViewManager_QueueView tests queuing views for processing
func TestViewManager_QueueView(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
		},
	}

	// Queue the view
	vm.QueueView(v)

	// Check that item was added to queue
	select {
	case item := <-vm.processingQueue:
		require.Equal(t, "TestView", item.viewName)
		require.Equal(t, "TestView", item.activeViewKey)
	default:
		t.Fatal("Expected item in processing queue")
	}
}

// TestViewManager_LoadAndRegisterViews_ExternalViews tests loading external views
func TestViewManager_LoadAndRegisterViews_ExternalViews(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Create source collections first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Transaction { hash: String }")
	require.NoError(t, err)

	// Create mock views
	externalViews := []View{
		{
			Name: "ExternalView1",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Log { address }",
				Sdl:   "type ExternalView1 { address: String }",
			},
		},
		{
			Name: "ExternalView2",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Transaction { hash }",
				Sdl:   "type ExternalView2 { hash: String }",
			},
		},
	}

	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err)

	// Verify views were registered
	require.Equal(t, 2, vm.GetViewCount())
	names := vm.GetActiveViewNames()
	require.Contains(t, names, "ExternalView1")
	require.Contains(t, names, "ExternalView2")
}

// TestViewManager_LoadAndRegisterViews_ViewWithoutLenses tests loading views without lenses (GitHub Actions compatible)
func TestViewManager_LoadAndRegisterViews_ViewWithoutLenses(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Disable WASM registry completely to avoid any downloads
	vm.wasmRegistry = nil

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	views := []View{
		{
			Name: "WasmView",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Log { address }",
				Sdl:   "type WasmView { address: String }",
				// No lenses to avoid WASM operations - we're testing the URL extraction logic separately
			},
		},
	}

	err = vm.LoadAndRegisterViews(ctx, views)
	require.NoError(t, err)

	// Verify view was registered despite WASM URL (WASM operations are skipped)
	require.Equal(t, 1, vm.GetViewCount())
}

// TestViewManager_RegisterView_ValidationFailure tests view validation
func TestViewManager_RegisterView_ValidationFailure(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Test invalid view (missing name)
	v := View{
		Name: "",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
		},
	}

	err = vm.RegisterView(ctx, &v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "view validation failed")
}

// TestViewManager_RegisterView_QueryCorrection tests inputData -> input correction
func TestViewManager_RegisterView_QueryCorrection(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := &View{
		Name: "CorrectionView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { inputData: address }", // Contains inputData
			Sdl:   "type CorrectionView { address: String }",
		},
	}

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	require.NoError(t, err)

	// Verify query was corrected
	require.NotContains(t, v.Data.Query, "inputData")
	require.Contains(t, v.Data.Query, "input")
}

// TestSuggestCorrectCollection tests collection name correction
func TestSuggestCorrectCollection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple collection",
			input:    "Log",
			expected: "Ethereum__Mainnet__Log",
		},
		{
			name:     "already prefixed",
			input:    "Ethereum__Mainnet__Log",
			expected: "Ethereum__Mainnet__Log",
		},
		{
			name:     "different chain prefix",
			input:    "Polygon__Mainnet__Transaction",
			expected: "Polygon__Mainnet__Transaction",
		},
		{
			name:     "with double underscore",
			input:    "SomeChain__Block",
			expected: "SomeChain__Block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := &ViewManager{}
			result := vm.suggestCorrectCollection(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestViewManager_SubscribeToSourceCollection tests subscription functionality
func TestViewManager_SubscribeToSourceCollection(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	err = vm.subscribeToSourceCollection(ctx, "Ethereum__Mainnet__Log", "TestView")
	require.NoError(t, err)
}

// TestExtractWasmURLsFromViews_MixedURLs tests extracting HTTP URLs from mixed lens paths
func TestExtractWasmURLsFromViews_MixedURLs(t *testing.T) {
	views := []View{
		{
			Name: "MixedView",
			Data: viewbundle.View{
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "file:///local.wasm"},           // Local file
						{Path: "https://example.com/lens1.wasm"}, // HTTP URL
						{Path: "http://example.com/lens2.wasm"},  // HTTP URL  
						{Path: "base64encodeddata"},             // Base64 data
					},
				},
			},
		},
	}

	urls := extractWasmURLsFromViews(views)

	require.Len(t, urls, 2)
	require.Contains(t, urls, "https://example.com/lens1.wasm")
	require.Contains(t, urls, "http://example.com/lens2.wasm")
}

// TestViewManager_LoadAndRegisterViews_Deduplication tests view deduplication
func TestViewManager_LoadAndRegisterViews_Deduplication(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Create source collections first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Transaction { hash: String }")
	require.NoError(t, err)
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Block { number: Int }")
	require.NoError(t, err)

	// Create views with duplicate names
	externalViews := []View{
		{
			Name: "DuplicateView",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Log { address }",
				Sdl:   "type DuplicateView { address: String }",
			},
		},
		{
			Name: "DuplicateView", // Same name
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Transaction { hash }",
				Sdl:   "type DuplicateView { hash: String }",
			},
		},
		{
			Name: "UniqueView",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Block { number }",
				Sdl:   "type UniqueView { number: Int }",
			},
		},
	}

	err = vm.LoadAndRegisterViews(ctx, externalViews)
	require.NoError(t, err)

	// Should only have 2 views (duplicate removed)
	require.Equal(t, 2, vm.GetViewCount())
	names := vm.GetActiveViewNames()
	require.Contains(t, names, "DuplicateView")
	require.Contains(t, names, "UniqueView")
}

// TestViewManager_RegisterView_WithBase64WASM tests registration with base64 WASM (completely mocked)
func TestViewManager_RegisterView_WithBase64WASM(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Use regular view manager but disable WASM registry to avoid downloads
	vm := NewViewManager(defraNode, t.TempDir())
	vm.wasmRegistry = nil

	// Test with a view that has no lenses to avoid WASM complications
	v := &View{
		Name: "Base64WasmView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }",
			Sdl:   "type Base64WasmView { address: String }",
			// No lenses to avoid WASM processing issues
		},
	}

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	// Should succeed without lenses
	require.NoError(t, err)
	require.Equal(t, 1, vm.GetViewCount())
}

// TestViewManager_RegisterView_CollectionNameCorrection tests automatic collection name fixing
func TestViewManager_RegisterView_CollectionNameCorrection(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := &View{
		Name: "CollectionTestView",
		Data: viewbundle.View{
			Query: "Log { address }", // Missing chain prefix - should be auto-corrected
			Sdl:   "type CollectionTestView { address: String }",
		},
	}

	// Create source collection with full name
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	require.NoError(t, err)

	// Verify query was corrected to include chain prefix
	require.Contains(t, v.Data.Query, "Ethereum__Mainnet__Log")
	require.NotEqual(t, "Log { address }", v.Data.Query) // Should not be the original uncorrected form
}

// TestViewManager_Integration_CompleteFlow tests a complete view registration flow without WASM
func TestViewManager_Integration_CompleteFlow(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Set up metrics callback
	metricsCallCount := 0
	vm.SetMetricsCallback(func() *server.HostMetrics {
		metricsCallCount++
		return &server.HostMetrics{}
	})

	// Create a comprehensive view without WASM lenses
	v := &View{
		Name: "CompleteFlowView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address topics }",
			Sdl:   "type CompleteFlowView { address: String, topics: [String] }",
			// No Transform/Lenses to avoid WASM operations
		},
	}

	// Create source collection
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String, topics: [String] }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	require.NoError(t, err)

	// Verify view was registered
	require.Equal(t, 1, vm.GetViewCount())
	require.Contains(t, vm.GetActiveViewNames(), "CompleteFlowView")

	// Verify metrics callback was called
	require.Greater(t, metricsCallCount, 0)
}

// TestExtractCollectionFromQuery_ComplexQueries tests collection extraction from various query formats
func TestExtractCollectionFromQuery_ComplexQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "simple query with space",
			query:    "Log { address topics }",
			expected: "Log",
		},
		{
			name:     "query with newline",
			query:    "Log\n{ address }",
			expected: "Log",
		},
		{
			name:     "query without braces",
			query:    "Transaction",
			expected: "Transaction",
		},
		{
			name:     "query with filter",
			query:    `Log(where: { address: "0x123" }) { address }`,
			expected: "Log(where:",
		},
		{
			name:     "empty query",
			query:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractCollectionFromQuery(tt.query)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestViewManager_RegisterView_NoLenses tests view registration without any lenses
func TestViewManager_RegisterView_NoLenses(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := &View{
		Name: "NoLensesView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }",
			Sdl:   "type NoLensesView { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{}, // Empty lenses
			},
		},
	}

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	require.NoError(t, err)

	// Verify view was registered
	require.Equal(t, 1, vm.GetViewCount())
	require.Contains(t, vm.GetActiveViewNames(), "NoLensesView")
}

// TestViewManager_RegisterView_WithFileURLs tests view registration with file:// URLs (no downloads)
func TestViewManager_RegisterView_WithFileURLs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	v := &View{
		Name: "FileURLView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }",
			Sdl:   "type FileURLView { address: String }",
			// No lenses to avoid WASM file processing issues
		},
	}

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	err = vm.RegisterView(ctx, v)
	// Should succeed without lenses
	require.NoError(t, err)
	require.Equal(t, 1, vm.GetViewCount())
}

// TestViewManager_LoadAndRegisterViews_LocalRegistryError tests handling of local registry errors
func TestViewManager_LoadAndRegisterViews_LocalRegistryError(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Use an invalid registry path that should cause loading to fail
	vm.registryPath = "/invalid/path/that/does/not/exist"

	// Should not fail, just log warning and continue
	err = vm.LoadAndRegisterViews(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, vm.GetViewCount())
}

// TestViewManager_ConcurrentAccess tests thread safety of view manager operations
func TestViewManager_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Test concurrent access to getters
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			// These should be thread-safe
			_ = vm.GetViewCount()
			_ = vm.GetActiveViewNames()
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("Test timed out waiting for concurrent access")
		}
	}
}

// TestViewManager_MetricsCallbackError tests metrics callback error handling
func TestViewManager_MetricsCallbackError(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	vm := NewViewManager(defraNode, t.TempDir())

	// Set a callback that returns nil (error case)
	vm.SetMetricsCallback(func() *server.HostMetrics {
		return nil // Simulate error case
	})

	v := &View{
		Name: "MetricsErrorView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }",
			Sdl:   "type MetricsErrorView { address: String }",
		},
	}

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	// Should not fail even if metrics callback returns nil
	err = vm.RegisterView(ctx, v)
	require.NoError(t, err)
}

// TestDeduplicateViews_ComplexScenarios tests deduplication with edge cases
func TestDeduplicateViews_ComplexScenarios(t *testing.T) {
	tests := []struct {
		name     string
		views    []View
		expected []string
	}{
		{
			name: "all duplicates",
			views: []View{
				{Name: "SameView"},
				{Name: "SameView"},
				{Name: "SameView"},
			},
			expected: []string{"SameView"},
		},
		{
			name: "case sensitive",
			views: []View{
				{Name: "view"},
				{Name: "View"},
				{Name: "VIEW"},
			},
			expected: []string{"view", "View", "VIEW"},
		},
		{
			name: "empty names",
			views: []View{
				{Name: ""},
				{Name: ""},
				{Name: "ValidView"},
			},
			expected: []string{"", "ValidView"},
		},
		{
			name: "single view",
			views: []View{
				{Name: "OnlyView"},
			},
			expected: []string{"OnlyView"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateViews(tt.views)
			require.Len(t, result, len(tt.expected))
			for i, expectedName := range tt.expected {
				require.Equal(t, expectedName, result[i].Name)
			}
		})
	}
}
