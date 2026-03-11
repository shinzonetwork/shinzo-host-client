package shinzohub

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
	"github.com/sourcenetwork/defradb/node"
)

// TestViewProcessor_ProcessViewFromWire tests wire format processing
func TestViewProcessor_ProcessViewFromWire(t *testing.T) {
	processor := NewViewProcessor(nil)

	// Create test viewbundle
	bundler := viewbundle.NewBundler()
	testView := viewbundle.View{
		Query: "Log { address topics }",
		Sdl:   "type LogView { address: String }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{Path: "test.wasm"},
			},
		},
	}

	// Bundle to wire format
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)

	// Encode to base64 (as it comes from ShinzoHub)
	wireBase64 := bundler.EncodeBase64(wire)

	// Test processing
	resultView, err := processor.ProcessViewFromWire(context.Background(), wireBase64)
	require.NoError(t, err)
	require.NotNil(t, resultView)

	// Verify view properties
	require.Equal(t, "LogView", resultView.Name)
	require.Equal(t, testView.Query, resultView.Data.Query)
	require.Equal(t, testView.Sdl, resultView.Data.Sdl)
	require.Len(t, resultView.Data.Transform.Lenses, 1)
}

// TestViewProcessor_ProcessViewFromWire_InvalidBase64 tests error handling
func TestViewProcessor_ProcessViewFromWire_InvalidBase64(t *testing.T) {
	processor := NewViewProcessor(nil)

	// Test with invalid base64
	_, err := processor.ProcessViewFromWire(context.Background(), "invalid-base64!")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode base64 wire")
}

// TestViewProcessor_ProcessViewFromWire_InvalidWire tests error handling
func TestViewProcessor_ProcessViewFromWire_InvalidWire(t *testing.T) {
	processor := NewViewProcessor(nil)

	// Test with valid base64 but invalid wire data
	invalidBase64 := "dGVzdA==" // "test" in base64
	_, err := processor.ProcessViewFromWire(context.Background(), invalidBase64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create view from wire")
}

// TestViewProcessor_ProcessViewFromWire_InvalidView tests validation
func TestViewProcessor_ProcessViewFromWire_InvalidView(t *testing.T) {
	processor := NewViewProcessor(nil)

	// Create invalid view (missing query)
	bundler := viewbundle.NewBundler()
	invalidView := viewbundle.View{
		Query: "", // Missing query
		Sdl:   "type TestView { field: String }",
	}

	// Bundle and encode
	wire, err := bundler.BundleView(invalidView)
	require.NoError(t, err)
	wireBase64 := bundler.EncodeBase64(wire)

	// Test processing should fail validation
	_, err = processor.ProcessViewFromWire(context.Background(), wireBase64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid view")
	require.Contains(t, err.Error(), "view query is required")
}

// TestViewProcessor_SetupViewInDefraDB tests complete DefraDB setup
func TestViewProcessor_SetupViewInDefraDB(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := node.NewNode(ctx, node.WithInMemory())
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create test view
	testView := &view.View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
		},
	}

	// Test complete setup
	err = processor.SetupViewInDefraDB(ctx, testView)
	require.NoError(t, err)

	// Verify view was created in DefraDB
	collection, err := defraNode.DB.GetCollectionByName(ctx, testView.Name)
	require.NoError(t, err)
	require.Equal(t, testView.Name, collection.Name())
}

// TestViewProcessor_SetupViewInDefraDB_WithLenses tests lens setup
func TestViewProcessor_SetupViewInDefraDB_WithLenses(t *testing.T) {
	// Create temporary directory for WASM
	tempDir := t.TempDir()

	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := node.NewNode(ctx, node.WithInMemory())
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create view with lenses
	testView := &view.View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address topics }",
			Sdl:   "type TestView @materialized(if: false) { address: String, topics: [String] }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///test/lens.wasm", // File path (no conversion needed)
						Arguments: `{"filter": "address"}`,
					},
				},
			},
		},
	}

	// Test setup with lenses
	err = processor.SetupViewInDefraDB(ctx, testView)
	require.NoError(t, err)

	// Verify view was created
	collection, err := defraNode.DB.GetCollectionByName(ctx, testView.Name)
	require.NoError(t, err)
	require.Equal(t, testView.Name, collection.Name())
}

// TestViewProcessor_SetupViewInDefraDB_InvalidView tests error handling
func TestViewProcessor_SetupViewInDefraDB_InvalidView(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := node.NewNode(ctx, node.WithInMemory())
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create invalid view (missing SDL)
	invalidView := &view.View{
		Name: "InvalidView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "", // Missing SDL
		},
	}

	// Test setup should fail
	err = processor.SetupViewInDefraDB(ctx, invalidView)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to configure view")
}

// TestViewProcessor_Integration tests complete integration
func TestViewProcessor_Integration(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := node.NewNode(ctx, node.WithInMemory())
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create complete viewbundle
	bundler := viewbundle.NewBundler()
	testView := viewbundle.View{
		Query: "Log { address topics data }",
		Sdl:   "type ProcessedLog @materialized(if: false) { address: String, topics: [String], processed: Boolean }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{
					Path:      "https://example.com/lens.wasm", // HTTP URL (no conversion needed)
					Arguments: `{"process": true}`,
				},
			},
		},
	}

	// Bundle and encode
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)
	wireBase64 := bundler.EncodeBase64(wire)

	// Process from wire
	view, err := processor.ProcessViewFromWire(ctx, wireBase64)
	require.NoError(t, err)

	// Setup in DefraDB
	err = processor.SetupViewInDefraDB(ctx, view)
	require.NoError(t, err)

	// Verify complete setup
	require.Equal(t, "ProcessedLog", view.Name)
	require.True(t, view.HasLenses())

	// Verify view exists in DefraDB
	collection, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	require.NoError(t, err)
	require.Equal(t, view.Name, collection.Name())
}
