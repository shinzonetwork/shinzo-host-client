package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
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
	require.NotNil(t, vm.lensService)
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

func TestExtractWasmURLsFromViews(t *testing.T) {
	views := []View{
		{
			Name: "View1",
			Transform: models.Transform{
				Lenses: []models.Lens{
					{Path: "https://example.com/filter.wasm"},
					{Path: "http://example.com/transform.wasm"},
				},
			},
		},
		{
			Name: "View2",
			Transform: models.Transform{
				Lenses: []models.Lens{
					{Path: "file:///local/path.wasm"}, // Should be excluded
					{Path: "base64encodeddata"},       // Should be excluded
				},
			},
		},
		{
			Name: "View3",
			Transform: models.Transform{
				Lenses: []models.Lens{
					{Path: "https://another.com/lens.wasm"},
				},
			},
		},
	}

	urls := extractWasmURLsFromViews(views)

	require.Len(t, urls, 3)
	require.Contains(t, urls, "https://example.com/filter.wasm")
	require.Contains(t, urls, "http://example.com/transform.wasm")
	require.Contains(t, urls, "https://another.com/lens.wasm")
}

func TestExtractWasmURLsFromViews_NoHTTPURLs(t *testing.T) {
	views := []View{
		{
			Name: "View1",
			Transform: models.Transform{
				Lenses: []models.Lens{
					{Path: "file:///local/path.wasm"},
					{Path: "base64encodeddata"},
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
			query:    "Log { address topics }",
			expected: "Log",
		},
		{
			name:     "full collection name",
			query:    "Ethereum__Mainnet__Log { address }",
			expected: "Ethereum__Mainnet__Log",
		},
		{
			name:     "no braces",
			query:    "Transaction",
			expected: "Transaction",
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
	query := "Log { address topics }"
	sdl := "type FilteredLog { address: String }"

	v := View{
		Name:  "FilteredLog",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "filter",
					Path:  "file:///path/to/filter.wasm",
					Arguments: map[string]any{
						"field": "address",
						"value": "0x123",
					},
				},
			},
		},
	}

	config := v.BuildLensConfig()

	require.Equal(t, query, config.SourceCollectionVersionID)
	require.Equal(t, sdl, config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Equal(t, "file:///path/to/filter.wasm", config.Lens.Lenses[0].Path)
	require.Equal(t, "address", config.Lens.Lenses[0].Arguments["field"])
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
	v := View{Name: "TestView"}
	err = vm.RegisterView(ctx, v)

	require.Error(t, err)
	require.Contains(t, err.Error(), "already registered")
}
