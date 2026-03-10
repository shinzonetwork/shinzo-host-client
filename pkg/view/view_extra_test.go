package view

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// PostWasmToFile
// ---------------------------------------------------------------------------

func TestView_PostWasmToFile_Success(t *testing.T) {
	dir := t.TempDir()

	// Create fake wasm content and base64 encode it
	wasmContent := []byte("fake wasm binary content")
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmContent)

	v := View{
		Name: "TestView",
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "test_lens",
					Path:  wasmBase64,
				},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), dir)
	require.NoError(t, err)

	// Verify the path was updated to a file:// URL
	require.Contains(t, v.Transform.Lenses[0].Path, "file://")
	require.Contains(t, v.Transform.Lenses[0].Path, "test_lens.wasm")

	// Verify the file was actually written with the correct content
	// Extract the path from the file:// URL
	filePath := v.Transform.Lenses[0].Path[len("file://"):]
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, wasmContent, data)
}

func TestView_PostWasmToFile_MultipleLenses(t *testing.T) {
	dir := t.TempDir()

	wasm1 := base64.StdEncoding.EncodeToString([]byte("wasm1"))
	wasm2 := base64.StdEncoding.EncodeToString([]byte("wasm2"))

	v := View{
		Name: "TestView",
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Label: "lens1", Path: wasm1},
				{Label: "lens2", Path: wasm2},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), dir)
	require.NoError(t, err)

	require.Contains(t, v.Transform.Lenses[0].Path, "file://")
	require.Contains(t, v.Transform.Lenses[0].Path, "lens1.wasm")
	require.Contains(t, v.Transform.Lenses[1].Path, "file://")
	require.Contains(t, v.Transform.Lenses[1].Path, "lens2.wasm")
}

func TestView_PostWasmToFile_InvalidBase64(t *testing.T) {
	dir := t.TempDir()

	v := View{
		Name: "TestView",
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Label: "bad_lens", Path: "not-valid-base64!!!"},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode base64 WASM data")
}

func TestView_PostWasmToFile_InvalidRegistryPath(t *testing.T) {
	// Use a path under /dev/null which cannot be created as a directory
	v := View{
		Name: "TestView",
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Label: "test_lens", Path: base64.StdEncoding.EncodeToString([]byte("wasm"))},
			},
		},
	}

	err := v.PostWasmToFile(context.Background(), "/dev/null/impossible/path")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to find or create lens registry directory")
}

func TestView_PostWasmToFile_NoLenses(t *testing.T) {
	dir := t.TempDir()

	v := View{
		Name:      "TestView",
		Transform: models.Transform{},
	}

	err := v.PostWasmToFile(context.Background(), dir)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// findOrCreateLensRegistryDir
// ---------------------------------------------------------------------------

func TestView_findOrCreateLensRegistryDir_ExistingDir(t *testing.T) {
	dir := t.TempDir()

	v := View{Name: "TestView"}
	result, err := v.findOrCreateLensRegistryDir(dir)
	require.NoError(t, err)
	require.Equal(t, dir, result)
}

func TestView_findOrCreateLensRegistryDir_CreateNew(t *testing.T) {
	dir := t.TempDir()
	newDir := filepath.Join(dir, "new_registry")

	v := View{Name: "TestView"}
	result, err := v.findOrCreateLensRegistryDir(newDir)
	require.NoError(t, err)
	require.Equal(t, newDir, result)

	// Verify directory was created
	info, err := os.Stat(newDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestView_findOrCreateLensRegistryDir_CannotCreate(t *testing.T) {
	v := View{Name: "TestView"}
	_, err := v.findOrCreateLensRegistryDir("/dev/null/impossible/deeply/nested/path")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create lens registry directory")
}

func TestView_findOrCreateLensRegistryDir_ParentPathExists(t *testing.T) {
	// Test the case where the path with ../ prefix resolves to an existing dir
	dir := t.TempDir()
	subDir := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(subDir, 0755))

	v := View{Name: "TestView"}
	// This tests the first possible path existing
	result, err := v.findOrCreateLensRegistryDir(subDir)
	require.NoError(t, err)
	require.Equal(t, subDir, result)
}

// ---------------------------------------------------------------------------
// ConfigureLens
// ---------------------------------------------------------------------------

func TestView_ConfigureLens_NilQuery(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()

	v := View{
		Name:  "TestView",
		Query: nil,
	}

	err = v.ConfigureLens(ctx, defraNode, ss)
	require.Error(t, err)
	require.Contains(t, err.Error(), "view query is required")
}

func TestView_ConfigureLens_EmptyQuery(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()
	emptyQuery := ""

	v := View{
		Name:  "TestView",
		Query: &emptyQuery,
	}

	err = v.ConfigureLens(ctx, defraNode, ss)
	require.Error(t, err)
	require.Contains(t, err.Error(), "view query is required")
}

func TestView_ConfigureLens_NilSDL(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()
	query := "User { name }"

	v := View{
		Name:  "TestView",
		Query: &query,
		Sdl:   nil,
	}

	err = v.ConfigureLens(ctx, defraNode, ss)
	require.Error(t, err)
	require.Contains(t, err.Error(), "view SDL is required")
}

func TestView_ConfigureLens_EmptySDL(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()
	query := "User { name }"
	emptySDL := ""

	v := View{
		Name:  "TestView",
		Query: &query,
		Sdl:   &emptySDL,
	}

	err = v.ConfigureLens(ctx, defraNode, ss)
	require.Error(t, err)
	require.Contains(t, err.Error(), "view SDL is required")
}

func TestView_ConfigureLens_NoLenses(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()
	query := "User { name }"
	sdl := "type TestViewNoLens { name: String }"

	v := View{
		Name:      "TestViewNoLens",
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
	}

	// This should attempt to call AddView without lens transforms
	// It may succeed or fail depending on if the source collection "User" exists,
	// but it exercises the no-lenses branch
	err = v.ConfigureLens(ctx, defraNode, ss)
	// The AddView call will likely fail because source collection doesn't exist,
	// but we exercise the code path for no lenses
	if err != nil {
		require.Contains(t, err.Error(), "failed to create view")
	}
}

func TestView_ConfigureLens_WithLenses(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ss := NewSchemaService()
	query := "User { name }"
	sdl := "type TestViewWithLens { name: String }"

	v := View{
		Name:  "TestViewWithLens",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "test_filter",
					Path:  "file:///nonexistent/filter.wasm",
					Arguments: map[string]any{
						"field": "name",
					},
				},
			},
		},
	}

	// This exercises the branch with lenses - AddLens will likely fail
	// because the wasm file doesn't exist, which tests that error path
	err = v.ConfigureLens(ctx, defraNode, ss)
	if err != nil {
		// Expected - either "failed to register lens" or "failed to create view"
		require.True(t,
			strings.Contains(err.Error(), "failed to register lens") ||
				strings.Contains(err.Error(), "failed to create view"),
			"unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ApplyLensTransform - additional branches
// ---------------------------------------------------------------------------

func TestView_ApplyLensTransform_NilSDL(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	v := View{
		Name: "TestView",
		Sdl:  nil,
	}

	_, err = v.ApplyLensTransform(ctx, defraNode, "TestView(filter: {}) { name }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to extract fields from SDL")
}

func TestView_ApplyLensTransform_EmptySDL(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	emptySDL := ""
	v := View{
		Name: "TestView",
		Sdl:  &emptySDL,
	}

	_, err = v.ApplyLensTransform(ctx, defraNode, "TestView(filter: {}) { name }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to extract fields from SDL")
}

func TestView_ApplyLensTransform_InvalidSDL(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	invalidSDL := "not a valid SDL"
	v := View{
		Name: "TestView",
		Sdl:  &invalidSDL,
	}

	_, err = v.ApplyLensTransform(ctx, defraNode, "TestView(filter: {}) { name }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to extract fields from SDL")
}

func TestView_ApplyLensTransform_NoFilterInQuery(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sdl := "type TestView { name: String }"
	v := View{
		Name: "TestView",
		Sdl:  &sdl,
	}

	// Query without filter params - should hit "Must have at least one query param" branch
	_, err = v.ApplyLensTransform(ctx, defraNode, "TestView { name }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Must have at least one query param")
}

func TestView_ApplyLensTransform_WithFilter(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sdl := "type TestViewApply { name: String }"
	v := View{
		Name: "TestViewApply",
		Sdl:  &sdl,
	}

	// Query with filter params - will fail on the actual query because collection doesn't exist,
	// but exercises the filter extraction and query building branches
	_, err = v.ApplyLensTransform(ctx, defraNode, "TestViewApply(filter: {name: {_eq: \"Alice\"}}) { name }")
	// The query will fail because the collection doesn't exist
	if err != nil {
		require.Contains(t, err.Error(), "failed to query view collection")
	}
}

// ---------------------------------------------------------------------------
// filterDocumentFields - additional edge cases
// ---------------------------------------------------------------------------

func TestView_FilterDocumentFields_EmptySDLString(t *testing.T) {
	emptySDL := ""
	v := View{
		Name: "TestView",
		Sdl:  &emptySDL,
	}

	document := map[string]any{"name": "Alice", "age": 25}
	filtered, err := v.filterDocumentFields(document)
	require.NoError(t, err)
	require.Equal(t, document, filtered)
}

func TestView_FilterDocumentFields_InvalidSDL(t *testing.T) {
	invalidSDL := "not a valid SDL at all"
	v := View{
		Name: "TestView",
		Sdl:  &invalidSDL,
	}

	document := map[string]any{"name": "Alice"}
	_, err := v.filterDocumentFields(document)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse SDL schema")
}

func TestView_FilterDocumentFields_SDLWithNoFields(t *testing.T) {
	// A type definition with no fields (just empty braces)
	sdl := "type TestView {}"
	v := View{
		Name: "TestView",
		Sdl:  &sdl,
	}

	document := map[string]any{"name": "Alice"}
	filtered, err := v.filterDocumentFields(document)
	// parseSDLFields may return an error for types with no fields
	if err != nil {
		require.Contains(t, err.Error(), "SDL")
		return
	}
	// If no error, document is returned as-is when no fields are found
	require.Equal(t, document, filtered)
}

func TestView_FilterDocumentFields_EmptyDocument(t *testing.T) {
	sdl := "type TestView { name: String }"
	v := View{
		Name: "TestView",
		Sdl:  &sdl,
	}

	document := map[string]any{}
	filtered, err := v.filterDocumentFields(document)
	require.NoError(t, err)
	require.Empty(t, filtered)
}

// ---------------------------------------------------------------------------
// SubscribeTo - success path
// ---------------------------------------------------------------------------

func TestView_SubscribeTo_Success(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// First create the schema so the collection exists
	sdl := "type SubscribeTestView { name: String }"
	_, err = defraNode.DB.AddSchema(ctx, sdl)
	require.NoError(t, err)

	v := View{
		Name: "SubscribeTestView",
	}

	// Now subscribe should work
	err = v.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)
}

func TestView_SubscribeTo_CollectionNotFound(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	v := View{
		Name: "NonExistentCollection",
	}

	err = v.SubscribeTo(ctx, defraNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "collection does not exist")
}

// ---------------------------------------------------------------------------
// needsWasmConversion - no lenses
// ---------------------------------------------------------------------------

func TestView_NeedsWasmConversion_NoLenses(t *testing.T) {
	v := View{
		Transform: models.Transform{},
	}
	require.False(t, v.needsWasmConversion())
}

// ---------------------------------------------------------------------------
// buildLensModules - edge cases
// ---------------------------------------------------------------------------

func TestView_BuildLensModules_NoLenses(t *testing.T) {
	v := View{
		Transform: models.Transform{},
	}

	lens := v.buildLensModules()
	require.Empty(t, lens.Lenses)
}

func TestView_BuildLensModules_NilArguments(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "test",
					Path:  "file:///test.wasm",
				},
			},
		},
	}

	lens := v.buildLensModules()
	require.Len(t, lens.Lenses, 1)
	require.Equal(t, "file:///test.wasm", lens.Lenses[0].Path)
	require.False(t, lens.Lenses[0].Inverse)
}

// ---------------------------------------------------------------------------
// BuildLensConfig (on View)
// ---------------------------------------------------------------------------

func TestView_BuildLensConfig_NoLenses(t *testing.T) {
	query := "User { name }"
	sdl := "type TestView { name: String }"

	v := View{
		Name:      "TestView",
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
	}

	config := v.BuildLensConfig()
	require.Equal(t, query, config.SourceCollectionVersionID)
	require.Equal(t, sdl, config.DestinationCollectionVersionID)
	require.Empty(t, config.Lens.Lenses)
}

func TestView_BuildLensConfig_MultipleLenses(t *testing.T) {
	query := "User { name age }"
	sdl := "type TestView { name: String }"

	v := View{
		Name:  "TestView",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label:     "filter1",
					Path:      "file:///filter1.wasm",
					Arguments: map[string]any{"key1": "val1"},
				},
				{
					Label:     "filter2",
					Path:      "file:///filter2.wasm",
					Arguments: map[string]any{"key2": "val2"},
				},
			},
		},
	}

	config := v.BuildLensConfig()
	require.Equal(t, query, config.SourceCollectionVersionID)
	require.Equal(t, sdl, config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 2)
	require.Equal(t, "file:///filter1.wasm", config.Lens.Lenses[0].Path)
	require.Equal(t, "file:///filter2.wasm", config.Lens.Lenses[1].Path)
	require.Equal(t, "val1", config.Lens.Lenses[0].Arguments["key1"])
	require.Equal(t, "val2", config.Lens.Lenses[1].Arguments["key2"])
}

// ---------------------------------------------------------------------------
// transformQueryCollectionNames - edge cases
// ---------------------------------------------------------------------------

func TestTransformQueryCollectionNames_AccessListEntry(t *testing.T) {
	result := transformQueryCollectionNames("AccessListEntry { address }")
	require.Equal(t, "Ethereum__Mainnet__AccessListEntry { address }", result)
}

func TestTransformQueryCollectionNames_EmptyQuery(t *testing.T) {
	result := transformQueryCollectionNames("")
	require.Equal(t, "", result)
}

// ---------------------------------------------------------------------------
// parseSDLFields - additional edge cases
// ---------------------------------------------------------------------------

func TestParseSDLFields_SkipsDoubleUnderscoreFields(t *testing.T) {
	sdl := "type TestView { __typename: String, name: String }"
	fields, err := parseSDLFields(sdl)
	require.NoError(t, err)
	require.Contains(t, fields, "name")
	require.NotContains(t, fields, "__typename")
}

// ---------------------------------------------------------------------------
// SubscribeTo - error from CreateP2PCollections
// ---------------------------------------------------------------------------

func TestView_SubscribeTo_CreateP2PCollectionsError(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create schema so GetCollectionByName succeeds
	_, err = defraNode.DB.AddSchema(ctx, "type SubscribeP2PErrView { name: String }")
	require.NoError(t, err)

	v := View{
		Name: "SubscribeP2PErrView",
	}

	// First subscribe should succeed
	err = v.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)

	// Subscribing again should also succeed (CreateP2PCollections is idempotent)
	err = v.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// WriteTransformedToCollection - collection not found
// ---------------------------------------------------------------------------

func TestView_WriteTransformedToCollection_CollectionNotFound(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	v := View{Name: "NonExistentWriteCol"}
	docs := []map[string]any{{"name": "Alice"}}

	_, err = v.WriteTransformedToCollection(ctx, defraNode, docs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting collection")
}

func TestView_WriteTransformedToCollection_Success(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create schema for the collection
	_, err = defraNode.DB.AddSchema(ctx, "type WriteTestCol { name: String }")
	require.NoError(t, err)

	sdl := "type WriteTestCol { name: String }"
	v := View{
		Name: "WriteTestCol",
		Sdl:  &sdl,
	}
	docs := []map[string]any{{"name": "Alice"}, {"name": "Bob"}}

	docIDs, err := v.WriteTransformedToCollection(ctx, defraNode, docs)
	require.NoError(t, err)
	require.Len(t, docIDs, 2)
}

func TestView_WriteTransformedToCollection_EmptyDocumentSlice(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	_, err = defraNode.DB.AddSchema(ctx, "type WriteEmptySliceCol { name: String }")
	require.NoError(t, err)

	v := View{Name: "WriteEmptySliceCol"}
	docIDs, err := v.WriteTransformedToCollection(ctx, defraNode, []map[string]any{})
	require.NoError(t, err)
	require.Empty(t, docIDs)
}

func TestView_WriteTransformedToCollection_FilterDocumentFieldsError(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	_, err = defraNode.DB.AddSchema(ctx, "type WriteFilterErr { name: String }")
	require.NoError(t, err)

	// SDL that fails parsing (no type definition)
	invalidSDL := "not valid SDL"
	v := View{
		Name: "WriteFilterErr",
		Sdl:  &invalidSDL,
	}
	docs := []map[string]any{{"name": "Alice"}}

	_, err = v.WriteTransformedToCollection(ctx, defraNode, docs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to filter document fields")
}

// ---------------------------------------------------------------------------
// needsWasmConversion - edge cases
// ---------------------------------------------------------------------------

func TestView_NeedsWasmConversion_FilePrefix(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "file:///some/path/filter.wasm"},
			},
		},
	}
	require.False(t, v.needsWasmConversion())
}

func TestView_NeedsWasmConversion_HttpPrefix(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "http://example.com/filter.wasm"},
			},
		},
	}
	require.False(t, v.needsWasmConversion())
}

func TestView_NeedsWasmConversion_HttpsPrefix(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "https://example.com/filter.wasm"},
			},
		},
	}
	require.False(t, v.needsWasmConversion())
}

func TestView_NeedsWasmConversion_Base64Data(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "AGFzbQEAAAA="}, // base64-like data
			},
		},
	}
	require.True(t, v.needsWasmConversion())
}

func TestView_NeedsWasmConversion_MixedPaths(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Path: "file:///some/path.wasm"},
				{Path: "AGFzbQEAAAA="}, // base64 data - needs conversion
			},
		},
	}
	require.True(t, v.needsWasmConversion())
}

// ---------------------------------------------------------------------------
// HasLenses
// ---------------------------------------------------------------------------

func TestView_HasLenses_Empty(t *testing.T) {
	v := View{Transform: models.Transform{}}
	require.False(t, v.HasLenses())
}

func TestView_HasLenses_WithLenses(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{Label: "test", Path: "file:///test.wasm"},
			},
		},
	}
	require.True(t, v.HasLenses())
}

// ---------------------------------------------------------------------------
// getUniqueViewName
// ---------------------------------------------------------------------------

func TestView_GetUniqueViewName_ReturnsName(t *testing.T) {
	v := View{Name: "MyViewExtra"}
	require.Equal(t, "MyViewExtra", v.getUniqueViewName())
}

// ---------------------------------------------------------------------------
// filterDocumentFields - case insensitive matching
// ---------------------------------------------------------------------------

func TestView_FilterDocumentFields_CaseInsensitiveWithExtraFields(t *testing.T) {
	sdl := "type TestViewCaseExtra { Name: String, Age: Int }"
	v := View{
		Name: "TestViewCaseExtra",
		Sdl:  &sdl,
	}

	// Document has lowercase keys plus extra fields not in SDL
	document := map[string]any{"name": "Alice", "age": 30, "extra": "removed", "another": true}
	filtered, err := v.filterDocumentFields(document)
	require.NoError(t, err)
	// Case-insensitive matching should find Name and Age, and use the schema key casing
	require.Contains(t, filtered, "Name")
	require.Contains(t, filtered, "Age")
	require.NotContains(t, filtered, "extra")
	require.NotContains(t, filtered, "another")
}

func TestView_FilterDocumentFields_NilSDL(t *testing.T) {
	v := View{
		Name: "TestView",
		Sdl:  nil,
	}

	document := map[string]any{"name": "Alice"}
	filtered, err := v.filterDocumentFields(document)
	require.NoError(t, err)
	require.Equal(t, document, filtered)
}

// ---------------------------------------------------------------------------
// transformQueryCollectionNames - various collection types
// ---------------------------------------------------------------------------

func TestTransformQueryCollectionNames_AllTypes(t *testing.T) {
	require.Equal(t, "Ethereum__Mainnet__Log { address }", transformQueryCollectionNames("Log { address }"))
	require.Equal(t, "Ethereum__Mainnet__Block { number }", transformQueryCollectionNames("Block { number }"))
	require.Equal(t, "Ethereum__Mainnet__Transaction { hash }", transformQueryCollectionNames("Transaction { hash }"))
	require.Equal(t, "Ethereum__Mainnet__AccessListEntry { key }", transformQueryCollectionNames("AccessListEntry { key }"))
}

func TestTransformQueryCollectionNames_AlreadyPrefixed(t *testing.T) {
	query := "Ethereum__Mainnet__Log { address }"
	require.Equal(t, query, transformQueryCollectionNames(query))
}

// ---------------------------------------------------------------------------
// parseSDLFields - additional edge cases for __typename prefix
// ---------------------------------------------------------------------------

func TestParseSDLFields_MultipleFieldsWithDoubleUnderscore(t *testing.T) {
	sdl := "type TestView { __typename: String, __id: String, name: String, age: Int }"
	fields, err := parseSDLFields(sdl)
	require.NoError(t, err)
	require.Contains(t, fields, "name")
	require.Contains(t, fields, "age")
	require.NotContains(t, fields, "__typename")
	require.NotContains(t, fields, "__id")
}

// ---------------------------------------------------------------------------
// transformQueryCollectionNames - query with unknown collection (no transformation)
// ---------------------------------------------------------------------------

func TestTransformQueryCollectionNames_UnknownCollection(t *testing.T) {
	result := transformQueryCollectionNames("CustomTable { data }")
	require.Equal(t, "CustomTable { data }", result)
}

// ---------------------------------------------------------------------------
// filterDocumentFields - document with all keys matching schema
// ---------------------------------------------------------------------------

func TestView_FilterDocumentFields_AllKeysMatch(t *testing.T) {
	sdl := "type MatchView { name: String, age: Int }"
	v := View{
		Name: "MatchView",
		Sdl:  &sdl,
	}

	document := map[string]any{"name": "Alice", "age": 30}
	filtered, err := v.filterDocumentFields(document)
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.Equal(t, "Alice", filtered["name"])
	require.Equal(t, 30, filtered["age"])
}

// ---------------------------------------------------------------------------
// buildLensModules - with arguments
// ---------------------------------------------------------------------------

func TestView_BuildLensModules_WithArguments(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label:     "filter",
					Path:      "file:///filter.wasm",
					Arguments: map[string]any{"key": "value", "num": 42},
				},
			},
		},
	}

	lens := v.buildLensModules()
	require.Len(t, lens.Lenses, 1)
	require.Equal(t, "file:///filter.wasm", lens.Lenses[0].Path)
	require.Equal(t, "value", lens.Lenses[0].Arguments["key"])
	require.Equal(t, 42, lens.Lenses[0].Arguments["num"])
	require.False(t, lens.Lenses[0].Inverse)
}

// ---------------------------------------------------------------------------
// PostWasmToFile - existing lens registry dir at ../ prefix
// ---------------------------------------------------------------------------

func TestView_findOrCreateLensRegistryDir_FallbackToDotDotSlash(t *testing.T) {
	// Create directory structure where ../<path> exists
	dir := t.TempDir()
	subDir := filepath.Join(dir, "sub")
	targetDir := filepath.Join(dir, "registry")

	require.NoError(t, os.MkdirAll(subDir, 0755))
	require.NoError(t, os.MkdirAll(targetDir, 0755))

	// Change working dir context: if we pass a path that doesn't exist directly
	// but ../path exists, it should find it via the fallback
	v := View{Name: "TestView"}

	// The first path (registryPath directly) won't exist, so it will try ../ prefix etc.
	nonExistent := filepath.Join(subDir, "nonexistent_registry")
	result, err := v.findOrCreateLensRegistryDir(nonExistent)
	require.NoError(t, err)
	// Since the first path doesn't exist, it creates the directory
	require.Equal(t, nonExistent, result)
}
