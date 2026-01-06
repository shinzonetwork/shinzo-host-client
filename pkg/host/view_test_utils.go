package host

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

// createSimpleView creates a simple view with identity lens transform for testing
func createSimpleView() view.View {
	query := constants.CollectionTransaction + " {" +
		"hash\n" +
		"blockNumber\n" +
		"from\n" +
		"to\n" +
		"value\n" +
		"}"

	sdl := `type SimpleView {
		hash: String
		blockNumber: Int
		from: String
		to: String
		value: String
	}`

	// Create an identity lens transform that passes through all documents
	// This prevents the "no lens transformations configured" error
	return view.View{
		Name:  "SimpleView",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "identity",
					Path:  "", // Empty path means identity transformation
					Arguments: map[string]any{
						"passthrough": true,
					},
				},
			},
		},
	}
}

// createWASMLensView creates a view with WASM lens
func createWASMLensView(targetHash string) view.View {
	// Use a broader query that gets all Transaction documents
	// The WASM lens will then filter them by hash
	query := constants.CollectionTransaction + " {" +
		"hash\n" +
		"blockNumber\n" +
		"from\n" +
		"to\n" +
		"value\n" +
		"}"

	sdl := `type WASMView {
		hash: String
		blockNumber: Int
		from: String
		to: String
		value: String
	}`

	// Read WASM file
	wasmBytes, err := os.ReadFile("/tmp/filter_transaction.wasm")
	if err != nil {
		// Return view without WASM if file not available
		return view.View{
			Name:  "WASMView",
			Query: &query,
			Sdl:   &sdl,
		}
	}

	wasmBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	return view.View{
		Name:  "WASMView",
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "filter_transaction",
					Path:  wasmBase64,
					Arguments: map[string]any{
						"src":   "hash",     // Filter by hash field
						"value": targetHash, // Target hash to match
					},
				},
			},
		},
	}
}

// downloadRealWasmFileForViewTest downloads the WASM file if not present
func downloadRealWasmFileForViewTest(t *testing.T) {
	wasmPath := "/tmp/filter_transaction.wasm"

	// Check if file exists and is valid
	if stat, err := os.Stat(wasmPath); err == nil {
		if stat.Size() > 0 {
			t.Logf("✅ WASM file already exists: %d bytes", stat.Size())
			return
		}
		// File exists but is empty, redownload
		t.Logf("⚠️ WASM file exists but is empty, redownloading...")
	}

	// Download the WASM file
	t.Logf("📥 Downloading WASM file from GitHub...")
	resp, err := http.Get("https://github.com/shinzonetwork/wasm-bucket/raw/refs/heads/main/bucket/filter_transaction/filter_transaction.wasm")
	require.NoError(t, err, "Failed to download WASM file")
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "WASM file download should return 200")

	wasmBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Failed to read WASM file")
	require.Greater(t, len(wasmBytes), 0, "WASM file should not be empty")

	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err, "Failed to write WASM file")

	t.Logf("✅ Downloaded real WASM file: %d bytes", len(wasmBytes))

	// Verify the file was written correctly
	if stat, err := os.Stat(wasmPath); err == nil {
		t.Logf("✅ WASM file verified: %d bytes at %s", stat.Size(), wasmPath)
	}
}

// createTestDocument creates a test document in DefraDB
func createTestDocument(t *testing.T, host *Host, hash string) string {
	createMutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {
				hash: "%s"
				blockNumber: 12345
				from: "0xabc123"
				to: "0xdef456"
				value: "1000000000000000000000"
			}) {
				_docID
				hash
			}
		}
	`, constants.CollectionTransaction, hash)

	type CreateResult struct {
		DocID string `json:"_docID"`
		Hash  string `json:"hash"`
	}

	ctx := context.Background()
	result, err := defra.PostMutation[CreateResult](ctx, host.DefraNode, createMutation)
	require.NoError(t, err, "Should create test document")
	require.NotEmpty(t, result.DocID, "Should have document ID")

	t.Logf("📄 Created transaction document: hash=%s, id=%s", hash, result.DocID)
	return result.DocID
}
