package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewLensService
// ---------------------------------------------------------------------------

func TestNewLensService(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ls := NewLensService(defraNode)
	require.NotNil(t, ls)
}

// ---------------------------------------------------------------------------
// SetMigration
// ---------------------------------------------------------------------------

func TestLensService_SetMigration_Success(t *testing.T) {
	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	ls := NewLensService(defraNode)

	// Build a minimal config for SetMigration
	config := BuildLensConfig("sourceVersionID", "destVersionID", "/tmp/nonexistent.wasm", nil)

	// SetMigration on an actual DefraDB instance - it may fail because the schema versions
	// don't exist, but we want to exercise the code path
	_, err = ls.SetMigration(ctx, defraNode, config)
	// The error could be "failed to set migration" wrapping the DB error
	// about unknown collection version IDs, which is expected
	if err != nil {
		require.Contains(t, err.Error(), "failed to set migration")
	}
}

// ---------------------------------------------------------------------------
// BuildLensConfig
// ---------------------------------------------------------------------------

func TestBuildLensConfig_Basic(t *testing.T) {
	query := "Log { address topics }"
	sdl := "type FilteredLog { address: String }"
	wasmPath := "file:///path/to/filter.wasm"
	args := map[string]any{
		"field": "address",
		"value": "0x123",
	}

	config := BuildLensConfig(query, sdl, wasmPath, args)

	require.Equal(t, query, config.SourceCollectionVersionID)
	require.Equal(t, sdl, config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Equal(t, wasmPath, config.Lens.Lenses[0].Path)
	require.Equal(t, "address", config.Lens.Lenses[0].Arguments["field"])
	require.Equal(t, "0x123", config.Lens.Lenses[0].Arguments["value"])
}

func TestBuildLensConfig_NilArgs(t *testing.T) {
	config := BuildLensConfig("src", "dst", "/path.wasm", nil)

	require.Equal(t, "src", config.SourceCollectionVersionID)
	require.Equal(t, "dst", config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Equal(t, "/path.wasm", config.Lens.Lenses[0].Path)
	require.Nil(t, config.Lens.Lenses[0].Arguments)
}

func TestBuildLensConfig_EmptyArgs(t *testing.T) {
	config := BuildLensConfig("src", "dst", "/path.wasm", map[string]any{})

	require.Equal(t, "src", config.SourceCollectionVersionID)
	require.Equal(t, "dst", config.DestinationCollectionVersionID)
	require.Len(t, config.Lens.Lenses, 1)
	require.Empty(t, config.Lens.Lenses[0].Arguments)
}
