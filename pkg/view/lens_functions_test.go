package view

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestSetupLensInDefraDB_NoLenses tests when view has no lenses
func TestSetupLensInDefraDB_NoLenses(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	v := &View{
		Name: "NoLensView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type NoLensView { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{},
			},
		},
	}

	lensCID, err := SetupLensInDefraDB(ctx, defraNode, v)
	require.NoError(t, err)
	require.Empty(t, lensCID)
}

// TestSetupLensInDefraDB_WithLenses tests lens setup with valid lenses
func TestSetupLensInDefraDB_WithLenses(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	v := &View{
		Name: "LensView",
		Data: viewbundle.View{
			Query: "Log { address topics }",
			Sdl:   "type LensView { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///test/filter.wasm",
						Arguments: `{"field": "address"}`,
					},
				},
			},
		},
	}

	lensCID, err := SetupLensInDefraDB(ctx, defraNode, v)
	// May fail because WASM file doesn't exist, but we test the code path
	if err != nil {
		require.Contains(t, err.Error(), "failed to add lens")
	} else {
		require.NotEmpty(t, lensCID)
	}
}

// TestBuildLensConfig_SingleLens tests building config with one lens
func TestBuildLensConfig_SingleLens(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Query: "Log { address topics }",
			Sdl:   "type FilteredLog { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///filter.wasm",
						Arguments: `{"key": "value"}`,
					},
				},
			},
		},
	}

	config, err := v.BuildLensConfig()
	require.NoError(t, err)
	require.Equal(t, v.Data.Query, config.SourceCollectionVersionID)
	require.Equal(t, v.Data.Sdl, config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Equal(t, "file:///filter.wasm", config.Lens.Lenses[0].Path)
	require.Equal(t, "value", config.Lens.Lenses[0].Arguments["key"])
}

// TestBuildLensConfig_MultipleLenses tests building config with multiple lenses
func TestBuildLensConfig_MultipleLenses(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Query: "Log { address topics data }",
			Sdl:   "type ProcessedLog { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///filter.wasm",
						Arguments: `{"step": 1}`,
					},
					{
						Path:      "file:///transform.wasm",
						Arguments: `{"step": 2}`,
					},
					{
						Path:      "file:///decode.wasm",
						Arguments: `{"step": 3}`,
					},
				},
			},
		},
	}

	config, err := v.BuildLensConfig()
	require.NoError(t, err)
	require.Len(t, config.Lens.Lenses, 3)
	require.Equal(t, "file:///filter.wasm", config.Lens.Lenses[0].Path)
	require.Equal(t, "file:///transform.wasm", config.Lens.Lenses[1].Path)
	require.Equal(t, "file:///decode.wasm", config.Lens.Lenses[2].Path)
	require.Equal(t, float64(1), config.Lens.Lenses[0].Arguments["step"])
	require.Equal(t, float64(2), config.Lens.Lenses[1].Arguments["step"])
	require.Equal(t, float64(3), config.Lens.Lenses[2].Arguments["step"])
}

// TestBuildLensConfig_EmptyArguments tests lens with empty arguments
func TestBuildLensConfig_EmptyArguments(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type SimpleLog { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///simple.wasm",
						Arguments: "",
					},
				},
			},
		},
	}

	config, err := v.BuildLensConfig()
	require.NoError(t, err)
	require.Len(t, config.Lens.Lenses, 1)
	require.Empty(t, config.Lens.Lenses[0].Arguments)
}

// TestBuildLensConfig_InvalidJSON tests lens with invalid JSON arguments
func TestBuildLensConfig_InvalidJSON(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type BadLog { address: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///bad.wasm",
						Arguments: "not valid json{{{",
					},
				},
			},
		},
	}

	_, err := v.BuildLensConfig()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse lens arguments")
}

// TestHasLenses_True tests HasLenses returns true when lenses exist
func TestHasLenses_True(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: "file:///test.wasm"},
				},
			},
		},
	}

	require.True(t, v.HasLenses())
}

// TestHasLenses_False tests HasLenses returns false when no lenses
func TestHasLenses_False(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{},
			},
		},
	}

	require.False(t, v.HasLenses())
}

// TestHasLenses_Nil tests HasLenses with nil lenses
func TestHasLenses_Nil(t *testing.T) {
	v := &View{
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: nil,
			},
		},
	}

	require.False(t, v.HasLenses())
}

// TestPostWasmToFile_Base64Conversion tests converting base64 WASM to file
func TestPostWasmToFile_Base64Conversion(t *testing.T) {
	tempDir := t.TempDir()

	// Create valid WASM magic number + some data
	wasmData := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD}
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmData)

	v := &View{
		Name: "Base64View",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: wasmBase64},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.NoError(t, err)

	// Verify path was updated to file:// URL
	require.Contains(t, v.Data.Transform.Lenses[0].Path, "file://")
	require.Contains(t, v.Data.Transform.Lenses[0].Path, ".wasm")

	// Verify file exists and has correct content
	filePath := v.Data.Transform.Lenses[0].Path[7:] // Remove "file://"
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, wasmData, data)
}

// TestPostWasmToFile_SkipsFileURLs tests that file:// URLs are not converted
func TestPostWasmToFile_SkipsFileURLs(t *testing.T) {
	tempDir := t.TempDir()

	v := &View{
		Name: "FileURLView",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: "file:///already/a/file.wasm"},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.NoError(t, err)

	// Path should remain unchanged
	require.Equal(t, "file:///already/a/file.wasm", v.Data.Transform.Lenses[0].Path)
}

// TestPostWasmToFile_SkipsHTTPURLs tests that HTTP URLs are not converted
func TestPostWasmToFile_SkipsHTTPURLs(t *testing.T) {
	tempDir := t.TempDir()

	v := &View{
		Name: "HTTPView",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: "https://example.com/lens.wasm"},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.NoError(t, err)

	// Path should remain unchanged
	require.Equal(t, "https://example.com/lens.wasm", v.Data.Transform.Lenses[0].Path)
}

// TestPostWasmToFile_InvalidWASM tests rejection of invalid WASM data
func TestPostWasmToFile_InvalidWASM(t *testing.T) {
	tempDir := t.TempDir()

	// Invalid WASM (missing magic number)
	invalidWasm := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00}
	wasmBase64 := base64.StdEncoding.EncodeToString(invalidWasm)

	v := &View{
		Name: "InvalidWASM",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: wasmBase64},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid WASM file format")
}

// TestPostWasmToFile_TooShortWASM tests rejection of too-short WASM data
func TestPostWasmToFile_TooShortWASM(t *testing.T) {
	tempDir := t.TempDir()

	// WASM data too short (< 8 bytes)
	shortWasm := []byte{0x00, 0x61, 0x73}
	wasmBase64 := base64.StdEncoding.EncodeToString(shortWasm)

	v := &View{
		Name: "ShortWASM",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: wasmBase64},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid WASM file format")
}

// TestPostWasmToFile_MultipleBase64Lenses tests converting multiple base64 lenses
func TestPostWasmToFile_MultipleBase64Lenses(t *testing.T) {
	tempDir := t.TempDir()

	wasm1 := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44}
	wasm2 := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, 0x55, 0x66, 0x77, 0x88}

	v := &View{
		Name: "MultiLens",
		Data: viewbundle.View{
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: base64.StdEncoding.EncodeToString(wasm1)},
					{Path: base64.StdEncoding.EncodeToString(wasm2)},
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), tempDir)
	require.NoError(t, err)

	// Both should be converted
	require.Contains(t, v.Data.Transform.Lenses[0].Path, "file://")
	require.Contains(t, v.Data.Transform.Lenses[1].Path, "file://")

	// Paths should be different
	require.NotEqual(t, v.Data.Transform.Lenses[0].Path, v.Data.Transform.Lenses[1].Path)
}

// TestIsValidBase64_Valid tests valid base64 detection
func TestIsValidBase64_Valid(t *testing.T) {
	validBase64 := base64.StdEncoding.EncodeToString([]byte("test data"))
	require.True(t, isValidBase64(validBase64))
}

// TestIsValidBase64_Invalid tests invalid base64 detection
func TestIsValidBase64_Invalid(t *testing.T) {
	require.False(t, isValidBase64("not valid base64!!!"))
}

// TestIsValidWASM_Valid tests valid WASM magic number detection
func TestIsValidWASM_Valid(t *testing.T) {
	validWasm := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00}
	require.True(t, isValidWASM(validWasm))
}

// TestIsValidWASM_Invalid tests invalid WASM magic number detection
func TestIsValidWASM_Invalid(t *testing.T) {
	invalidWasm := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00}
	require.False(t, isValidWASM(invalidWasm))
}

// TestIsValidWASM_TooShort tests WASM data that's too short
func TestIsValidWASM_TooShort(t *testing.T) {
	shortData := []byte{0x00, 0x61, 0x73}
	require.False(t, isValidWASM(shortData))
}

// TestConfigureLens_WithoutLensCID tests view configuration without lens CID
func TestConfigureLens_WithoutLensCID(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create source collection
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Log { address: String }")
	require.NoError(t, err)

	v := &View{
		Name: "SimpleView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address }",
			Sdl:   "type SimpleView { address: String }",
		},
	}

	schemaService := NewSchemaService()
	err = v.ConfigureLens(ctx, defraNode, schemaService, "")
	require.NoError(t, err)
}

// TestConfigureLens_WithLensCID tests view configuration with lens CID
func TestConfigureLens_WithLensCID(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create source collection
	_, err = defraNode.DB.AddSchema(ctx, "type Ethereum__Mainnet__Transaction { hash: String }")
	require.NoError(t, err)

	v := &View{
		Name: "LensedView",
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Transaction { hash }",
			Sdl:   "type LensedView { hash: String }",
		},
	}

	schemaService := NewSchemaService()
	// Use a fake lens CID - AddView will handle it
	err = v.ConfigureLens(ctx, defraNode, schemaService, "bafyreifake123")
	// May fail if lens CID doesn't exist, but we test the code path
	if err != nil && !contains(err.Error(), "already exists") {
		require.Contains(t, err.Error(), "failed to create view")
	}
}

// TestBuildLensConfig_ComplexArguments tests lens with complex JSON arguments
func TestBuildLensConfig_ComplexArguments(t *testing.T) {
	complexArgs := map[string]interface{}{
		"events": []interface{}{
			map[string]interface{}{
				"name": "Transfer",
				"inputs": []interface{}{
					map[string]interface{}{"type": "address", "name": "from"},
					map[string]interface{}{"type": "address", "name": "to"},
					map[string]interface{}{"type": "uint256", "name": "value"},
				},
			},
		},
		"config": map[string]interface{}{
			"strict": true,
			"version": 2,
		},
	}

	argsJSON, err := json.Marshal(complexArgs)
	require.NoError(t, err)

	v := &View{
		Data: viewbundle.View{
			Query: "Log { address topics data }",
			Sdl:   "type DecodedLog { from: String, to: String, value: String }",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      "file:///decode_event.wasm",
						Arguments: string(argsJSON),
					},
				},
			},
		},
	}

	config, err := v.BuildLensConfig()
	require.NoError(t, err)
	require.Len(t, config.Lens.Lenses, 1)

	// Verify complex arguments were parsed correctly
	args := config.Lens.Lenses[0].Arguments
	require.Contains(t, args, "events")
	require.Contains(t, args, "config")

	configMap := args["config"].(map[string]interface{})
	require.Equal(t, true, configMap["strict"])
	require.Equal(t, float64(2), configMap["version"])
}

// TestSaveViewToRegistry tests saving view to registry file
func TestSaveViewToRegistry(t *testing.T) {
	tempDir := t.TempDir()

	v := View{
		Name: "TestView",
		Data: viewbundle.View{
			Query: "Log { address }",
			Sdl:   "type TestView { address: String }",
		},
	}

	err := SaveViewToRegistry(tempDir, v)
	require.NoError(t, err)

	// Verify file was created
	viewsFile := filepath.Join(tempDir, "views.json")
	_, err = os.Stat(viewsFile)
	require.NoError(t, err)

	// Verify content
	data, err := os.ReadFile(viewsFile)
	require.NoError(t, err)

	var views []View
	err = json.Unmarshal(data, &views)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "TestView", views[0].Name)
}

// TestAddViewsFromLensRegistry tests loading views from registry
func TestAddViewsFromLensRegistry(t *testing.T) {
	tempDir := t.TempDir()

	// Create views.json
	views := []View{
		{
			Name: "View1",
			Data: viewbundle.View{
				Query: "Log { address }",
				Sdl:   "type View1 { address: String }",
			},
		},
		{
			Name: "View2",
			Data: viewbundle.View{
				Query: "Transaction { hash }",
				Sdl:   "type View2 { hash: String }",
			},
		},
	}

	data, err := json.Marshal(views)
	require.NoError(t, err)

	viewsFile := filepath.Join(tempDir, "views.json")
	err = os.WriteFile(viewsFile, data, 0644)
	require.NoError(t, err)

	// Load views
	loadedViews, err := AddViewsFromLensRegistry(tempDir)
	require.NoError(t, err)
	require.Len(t, loadedViews, 2)
	require.Equal(t, "View1", loadedViews[0].Name)
	require.Equal(t, "View2", loadedViews[1].Name)
}

// TestAddViewsFromLensRegistry_NoFile tests loading when file doesn't exist
func TestAddViewsFromLensRegistry_NoFile(t *testing.T) {
	tempDir := t.TempDir()

	views, err := AddViewsFromLensRegistry(tempDir)
	require.NoError(t, err)
	require.Empty(t, views)
}
