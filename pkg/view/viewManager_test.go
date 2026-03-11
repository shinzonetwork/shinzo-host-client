package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/viewbundle-go"
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
			Data: viewbundle.View{
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "file://" + "filter_transaction.wasm"},
					},
				},
			},
		},
		{
			Name: "View2",
			Data: viewbundle.View{
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "file://" + "filter_transaction.wasm"},

					},
				},
			},
		},
		{
			Name: "View3",
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

	require.Len(t, urls, 1) // Only HTTP URLs should be extracted
	require.Contains(t, urls, "https://another.com/lens.wasm")
}

func TestExtractWasmURLsFromViews_NoHTTPURLs(t *testing.T) {
	views := []View{
		{
			Name: "View1",
			Data: viewbundle.View{
				Transform: viewbundle.Transform{
					Lenses: []viewbundle.Lens{
						{Path: "file://" + "filter_transaction.wasm"},
						{Path: "file://" + "decode_event.wasm", Arguments: `[{"type":"event","name":"AdminChanged","inputs":[]}]`},
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
	err = vm.RegisterView(ctx, v)

	require.Error(t, err)
	require.Contains(t, err.Error(), "already registered") // This should be the actual error
}
