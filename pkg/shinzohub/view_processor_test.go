package shinzohub

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestViewProcessor_ProcessViewFromWire tests wire format processing
func TestViewProcessor_ProcessViewFromWire(t *testing.T) {
	// Create test viewbundle
	bundler := viewbundle.NewBundler()
	
	// Create valid WASM magic number + some data
	wasmData := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD}
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmData)
	
	testView := viewbundle.View{
		Query: "Log { address topics }",
		Sdl:   "type LogView { address: String }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{Path: wasmBase64},
			},
		},
	}

	// Bundle to wire format
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)

	// Encode to base64 (as it comes from ShinzoHub)
	wireBase64 := base64.StdEncoding.EncodeToString(wire)

	// Test processing
	resultView, err := ProcessViewFromWireFormat(wireBase64)
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
	// Test with invalid base64
	_, err := ProcessViewFromWireFormat("invalid-base64!")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode base64 wire")
}

// TestViewProcessor_ProcessViewFromWire_InvalidWire tests error handling
func TestViewProcessor_ProcessViewFromWire_InvalidWire(t *testing.T) {
	// Test with valid base64 but invalid wire data
	invalidBase64 := "dGVzdA==" // "test" in base64
	_, err := ProcessViewFromWireFormat(invalidBase64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create view from wire")
}

// TestViewProcessor_ProcessViewFromWire_InvalidView tests validation
func TestViewProcessor_ProcessViewFromWire_InvalidView(t *testing.T) {
	// Create invalid view (missing query)
	bundler := viewbundle.NewBundler()
	invalidView := viewbundle.View{
		Query: "", // Missing query
		Sdl:   "type TestView { field: String }",
	}

	// Bundle and encode
	wire, err := bundler.BundleView(invalidView)
	require.NoError(t, err)
	wireBase64 := base64.StdEncoding.EncodeToString(wire)

	// Test processing should fail validation
	_, err = ProcessViewFromWireFormat(wireBase64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid view")
	require.Contains(t, err.Error(), "view query is required")
}

// TestViewProcessor_SetupViewInDefraDB tests complete DefraDB setup
func TestViewProcessor_SetupViewInDefraDB(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Log { address: String, topics: [String] }")
	require.NoError(t, err)

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

// TestViewProcessor_SetupViewInDefraDB_WithoutLenses tests basic view setup without lenses (GitHub Actions compatible)
func TestViewProcessor_SetupViewInDefraDB_WithoutLenses(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	processor := NewViewProcessor(defraNode)

	// Create source collection first
	_, err = defraNode.DB.AddSchema(ctx, "type Log { address: String, topics: [String] }")
	require.NoError(t, err)

	// Create view without lenses (GitHub Actions compatible)
	testView := &view.View{
		Name: "TestView1",
		Data: viewbundle.View{
			Query: "Log { address topics }",
			Sdl:   "type TestView1 { address: String, topics: [String] }",
			// No lenses to avoid WASM file operations
		},
	}

	// Setup in DefraDB
	err = processor.SetupViewInDefraDB(ctx, testView)
	require.NoError(t, err)

	// Verify view was created in DefraDB
	collection, err := defraNode.DB.GetCollectionByName(ctx, testView.Name)
	require.NoError(t, err)
	require.Equal(t, testView.Name, collection.Name())
}

// TestViewProcessor_SetupViewInDefraDB_InvalidView tests error handling
func TestViewProcessor_SetupViewInDefraDB_InvalidView(t *testing.T) {
	// Create temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
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
