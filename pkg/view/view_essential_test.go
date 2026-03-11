package view

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/viewbundle-go"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

// Constants for test files
const (
	FilterWasmFile      = "filter_transaction.wasm"
	EventDecoderWasmFile = "decode_event.wasm"
)

// TestNewViewFromWire tests the core viewbundle processing
func TestNewViewFromWire(t *testing.T) {
	// Create a simple viewbundle with file-based WASM (no base64 needed)
	bundler := viewbundle.NewBundler()
	testView := viewbundle.View{
		Query: "Ethereum__Mainnet__Log { address topics }",
		Sdl:   "type LogView @materialized(if: false) { address: String, topics: [String] }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{
					Path:      "file://" + FilterWasmFile, // Use constant
					Arguments: `{"filter":"address"}`,
				},
			},
		},
	}

	// Bundle the view
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)
	require.NotEmpty(t, wire)

	// Test unbundle
	view, err := NewViewFromWire(wire)
	require.NoError(t, err)
	require.NotNil(t, view)

	// Verify name extraction
	require.Equal(t, "LogView", view.Name)
	require.Equal(t, testView.Query, view.Data.Query)
	require.Equal(t, testView.Sdl, view.Data.Sdl)
	require.Len(t, view.Data.Transform.Lenses, 1)
}

// TestValidate tests view validation
func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		view        View
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid view",
			view: View{
				Name: "TestView",
				Data: viewbundle.View{
					Query: "Log { address }",
					Sdl:   "type TestView { address: String }",
				},
			},
			expectError: false,
		},
		{
			name: "missing name",
			view: View{
				Name: "",
				Data: viewbundle.View{
					Query: "Log { address }",
					Sdl:   "type TestView { address: String }",
				},
			},
			expectError: true,
			errorMsg:    "view name is required",
		},
		{
			name: "missing query",
			view: View{
				Name: "TestView",
				Data: viewbundle.View{
					Query: "",
					Sdl:   "type TestView { address: String }",
				},
			},
			expectError: true,
			errorMsg:    "view query is required",
		},
		{
			name: "missing SDL",
			view: View{
				Name: "TestView",
				Data: viewbundle.View{
					Query: "Log { address }",
					Sdl:   "",
				},
			},
			expectError: true,
			errorMsg:    "view SDL is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.view.Validate()
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestPostWasmToFile tests WASM file processing
func TestPostWasmToFile(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "wasm_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create view with base64 WASM
	view := &View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file://" + FilterWasmFile, // Use actual WASM file
						Arguments: `{"test": true}`,
					},
					{
						Path:      "file:///already/exists.wasm", // Should be skipped
						Arguments: `{"test": true}`,
					},
				},
			},
		},
	}

	// Test WASM file creation
	err = view.PostWasmToFile(context.Background(), tempDir)
	require.NoError(t, err)

	// Verify WASM file was created
	require.Len(t, view.Data.Transform.Lenses, 2)
	
	// First lens should be converted to file path
	require.True(t, filepath.IsAbs(view.Data.Transform.Lenses[0].Path))
	require.True(t, len(view.Data.Transform.Lenses[0].Path) < 100) // Short filename
	
	// Second lens should remain unchanged (already a file)
	require.Equal(t, "file:///already/exists.wasm", view.Data.Transform.Lenses[1].Path)

	// Verify file actually exists
	wasmPath := view.Data.Transform.Lenses[0].Path[7:] // Remove "file://" prefix
	_, err = os.Stat(wasmPath)
	require.NoError(t, err)
}

// TestConfigureLens tests DefraDB view configuration
func TestConfigureLens(t *testing.T) {
	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create test view
	// Create the source collection schema first
	logSchema := `type Ethereum__Mainnet__Log {
	address: String
	topics: [String]
	data: String
	transactionHash: String
	blockNumber: Int
}`
	_, err = defraNode.DB.AddSchema(ctx, logSchema)
	require.NoError(t, err)

	view := &View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address topics }", // Add prefix
			Sdl:   "type TestView @materialized(if: false) { address: String, topics: [String] }",
		},
	}

	// Create schema service
	schemaService := &SchemaService{}

	// Test view configuration without lenses
	err = view.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)

	// Verify view was created
	collection, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	require.NoError(t, err)
	require.Equal(t, view.Name, collection.Name())
}

// TestConfigureLensWithLenses tests lens configuration
func TestConfigureLensWithLenses(t *testing.T) {
	// Create temporary directory for WASM
	tempDir, err := os.MkdirTemp("", "lens_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create view with lenses
	view := &View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address topics }",
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

	// Create schema service
	schemaService := &SchemaService{}

	// Test view configuration with lenses
	err = view.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)

	// Verify view was created
	collection, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	require.NoError(t, err)
	require.Equal(t, view.Name, collection.Name())
}

// TestSubscribeTo tests P2P subscription
func TestSubscribeTo(t *testing.T) {
	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create test view
	// Create the source collection schema first
	logSchema := `type Ethereum__Mainnet__Log {
	address: String
	topics: [String]
	data: String
}`
	_, err = defraNode.DB.AddSchema(ctx, logSchema)
	require.NoError(t, err)

	view := &View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
		},
	}

	// First, create the view collection
	schemaService := &SchemaService{}
	err = view.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)

	// Now test subscription
	err = view.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)

	// Verify subscription worked (no error means success)
	// In a real scenario, this would enable P2P replication
}

// TestSubscribeToNonExistentView tests subscription failure
func TestSubscribeToNonExistentView(t *testing.T) {
	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create view without creating the collection first
	view := &View{
		Name: "NonExistentView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type NonExistentView { address: String }",
		},
	}

	// Test subscription should fail
	err = view.SubscribeTo(ctx, defraNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "collection does not exist")
}

// TestHasLenses tests lens detection
func TestHasLenses(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "view with lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{
							{Path: "test.wasm"},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "view without lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{},
					},
				},
			},
			expected: false,
		},
		{
			name: "view with nil lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: nil,
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.view.HasLenses()
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestNeedsWasmConversion tests WASM conversion detection
func TestNeedsWasmConversion(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "view with base64 WASM",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{
							{Path: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "view with file:// path",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{
							{Path: "file:///path/to/lens.wasm"},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "view with http URL",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{
							{Path: "https://example.com/lens.wasm"},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "mixed lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{
							{Path: "base64-data-here"},
							{Path: "file:///already/file.wasm"},
						},
					},
				},
			},
			expected: true, // Should return true if any lens needs conversion
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.view.needsWasmConversion()
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildLensConfig tests lens configuration building
func TestBuildLensConfig(t *testing.T) {
	view := &View{
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address topics }",
			Sdl:   "type LogView { address: String, topics: [String] }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file://" + FilterWasmFile, // Use actual WASM file
						Arguments: `{"filter": "address"}`,
					},
					{
						Path:      "https://example.com/lens2.wasm",
						Arguments: `{"transform": "uppercase"}`,
					},
				},
			},
		},
	}

	// Test lens config building
	lensConfig, err := view.BuildLensConfig()
	require.NoError(t, err)

	// Verify basic configuration
	require.Equal(t, view.Data.Query, lensConfig.SourceCollectionVersionID)
	require.Equal(t, view.Data.Sdl, lensConfig.DestinationCollectionVersionID)
	require.Len(t, lensConfig.Lens.Lenses, 2) // Should have 2 lenses
	
	// Verify lenses have correct paths (basic check)
	require.Equal(t, "file://"+FilterWasmFile, lensConfig.Lens.Lenses[0].Path)
	require.Equal(t, "https://example.com/lens2.wasm", lensConfig.Lens.Lenses[1].Path)
	
	// Verify lenses have parsed arguments
	require.Equal(t, map[string]any{"filter": "address"}, lensConfig.Lens.Lenses[0].Arguments)
	require.Equal(t, map[string]any{"transform": "uppercase"}, lensConfig.Lens.Lenses[1].Arguments)
}

// TestEventDecoderWithUSDC tests the event decoder with USDC contract events
func TestEventDecoderWithUSDC(t *testing.T) {
	// Read USDC ABI
	usdcABIBytes, err := os.ReadFile("usdc-abi.json")
	require.NoError(t, err)
	
	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create the source collection schema first
	logSchema := `type Ethereum__Mainnet__Log {
	address: String
	topics: [String]
	data: String
	transactionHash: String
	blockNumber: Int
}`
	_, err = defraNode.DB.AddSchema(ctx, logSchema)
	require.NoError(t, err)

	// Create a view with the event decoder WASM and USDC ABI
	bundler := viewbundle.NewBundler()
	testView := viewbundle.View{
		Query: "Ethereum__Mainnet__Log { address topics data transactionHash blockNumber }",
		Sdl:   "type USDCContractEvents @materialized(if: false) { address: String, topics: [String], data: String, transactionHash: String, blockNumber: String }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{
					Path:      "file://" + EventDecoderWasmFile,
					Arguments: string(usdcABIBytes), // USDC ABI as parameter
				},
			},
		},
	}

	// Bundle the view
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)
	require.NotEmpty(t, wire)

	// Create view from wire
	view, err := NewViewFromWire(wire)
	require.NoError(t, err)

	// Validate view
	err = view.Validate()
	require.NoError(t, err)

	// Create the view in DefraDB
	schemaService := &SchemaService{}
	err = view.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)

	// Test that the view has the correct configuration
	require.Equal(t, "USDCContractEvents", view.Name)
	require.True(t, view.HasLenses())
	require.False(t, view.needsWasmConversion()) // File path, no conversion needed

	// Verify the collection was created
	collection, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	require.NoError(t, err)
	require.Equal(t, view.Name, collection.Name())
}
func TestExtractNameFromSDL(t *testing.T) {
	tests := []struct {
		name     string
		sdl      string
		expected string
	}{
		{
			name:     "SDL with @materialized",
			sdl:      "type LogView @materialized(if: false) { address: String }",
			expected: "LogView",
		},
		{
			name:     "SDL with @index",
			sdl:      "type TestView @index(unique: [\"address\"]) { address: String }",
			expected: "TestView",
		},
		{
			name:     "SDL without directive",
			sdl:      "type SimpleView { name: String, age: Int }",
			expected: "SimpleView",
		},
		{
			name:     "empty SDL",
			sdl:      "",
			expected: "",
		},
		{
			name:     "complex SDL",
			sdl:      "type ComplexView @materialized(if: false) @index(unique: [\"id\"]) { id: ID!, name: String, data: JSON }",
			expected: "ComplexView",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := &View{
				Data: viewbundle.View{
					Sdl: tt.sdl,
				},
			}

			view.ExtractNameFromSDL()
			require.Equal(t, tt.expected, view.Name)
		})
	}
}

// Integration test for the complete view processing pipeline
func TestViewProcessingPipeline(t *testing.T) {
	// Create temporary directory for WASM
	tempDir, err := os.MkdirTemp("", "pipeline_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a temporary DefraDB instance
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create the source collection schema first
	logSchema := `type Ethereum__Mainnet__Log {
	address: String
	topics: [String]
	data: String
}`
	_, err = defraNode.DB.AddSchema(ctx, logSchema)
	require.NoError(t, err)

	// Create a complete view with file-based WASM
	bundler := viewbundle.NewBundler()
	testView := viewbundle.View{
		Query: "Log { address topics data }",
		Sdl:   "type ProcessedLog @materialized(if: false) { address: String, topics: [String], processed: Boolean }",
		Transform: viewbundle.Transform{
			Lenses: []viewbundle.Lens{
				{
					Path:      "file://" + FilterWasmFile, // Use actual WASM file
					Arguments: `{"process": true}`,
				},
			},
		},
	}

	// Bundle the view
	wire, err := bundler.BundleView(testView)
	require.NoError(t, err)

	// Step 1: Create view from wire
	view, err := NewViewFromWire(wire)
	require.NoError(t, err)

	// Step 2: Validate view
	err = view.Validate()
	require.NoError(t, err)

	// Step 3: Process WASM files
	err = view.PostWasmToFile(ctx, tempDir)
	require.NoError(t, err)

	// Step 4: Configure in DefraDB
	schemaService := &SchemaService{}
	err = view.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)

	// Step 5: Subscribe to view
	err = view.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)

	// Verify final state
	require.Equal(t, "ProcessedLog", view.Name)
	require.True(t, view.HasLenses())
	require.False(t, view.needsWasmConversion()) // Should be false after PostWasmToFile

	// Verify view exists in DefraDB
	collection, err := defraNode.DB.GetCollectionByName(ctx, view.Name)
	require.NoError(t, err)
	require.Equal(t, view.Name, collection.Name())
}
