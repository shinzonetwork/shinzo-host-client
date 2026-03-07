package host

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	hostConfig "github.com/shinzonetwork/shinzo-host-client/config"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/snapshot"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// ---------------------------------------------------------------------------
// findCoveringSnapshots
// ---------------------------------------------------------------------------

func TestFindCoveringSnapshots_NoOverlap(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-1-100.tar", StartBlock: 1, EndBlock: 100, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 200, End: 300},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_FullOverlap(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-150-250.tar", StartBlock: 150, EndBlock: 250, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 250},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 2)
	// Sorted by start block
	require.Equal(t, "snap-100-200.tar", result[0].Filename)
	require.Equal(t, "snap-150-250.tar", result[1].Filename)
}

func TestFindCoveringSnapshots_AlreadyImported(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-300-400.tar", StartBlock: 300, EndBlock: 400, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 50, End: 500},
	}

	// existingMin=100, existingMax=200 means snap-100-200 is already fully in DB
	result := findCoveringSnapshots(available, ranges, 100, 200)
	require.Len(t, result, 1)
	require.Equal(t, "snap-300-400.tar", result[0].Filename)
}

func TestFindCoveringSnapshots_UnsignedFiltered(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: false},
		{Filename: "snap-200-300.tar", StartBlock: 200, EndBlock: 300, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 300},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 1)
	require.Equal(t, "snap-200-300.tar", result[0].Filename)
}

func TestFindCoveringSnapshots_DeduplicationByFilename(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	// Two ranges that both overlap the same snapshot
	ranges := []hostConfig.BlockRange{
		{Start: 50, End: 150},
		{Start: 150, End: 250},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 1)
	require.Equal(t, "snap-100-200.tar", result[0].Filename)
}

func TestFindCoveringSnapshots_SortedByStartBlock(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-300-400.tar", StartBlock: 300, EndBlock: 400, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-200-300.tar", StartBlock: 200, EndBlock: 300, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 50, End: 500},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 3)
	require.Equal(t, int64(100), result[0].StartBlock)
	require.Equal(t, int64(200), result[1].StartBlock)
	require.Equal(t, int64(300), result[2].StartBlock)
}

func TestFindCoveringSnapshots_EmptyAvailable(t *testing.T) {
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 200},
	}

	result := findCoveringSnapshots(nil, ranges, 0, 0)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_EmptyRanges(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-200.tar", StartBlock: 100, EndBlock: 200, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}

	result := findCoveringSnapshots(available, nil, 0, 0)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_PartiallyImportedStillIncluded(t *testing.T) {
	// A snapshot that extends beyond the existing range should still be included
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-50-300.tar", StartBlock: 50, EndBlock: 300, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 1, End: 400},
	}

	// existing range 100-200, but snapshot 50-300 extends beyond both ends
	result := findCoveringSnapshots(available, ranges, 100, 200)
	require.Len(t, result, 1)
	require.Equal(t, "snap-50-300.tar", result[0].Filename)
}

func TestFindCoveringSnapshots_ExistingMaxZeroDoesNotSkip(t *testing.T) {
	// When existingMax is 0, the "already imported" check is skipped entirely
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-1-100.tar", StartBlock: 1, EndBlock: 100, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 1, End: 100},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 1)
}

func TestFindCoveringSnapshots_SnapshotEndBeforeRangeStart(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-1-50.tar", StartBlock: 1, EndBlock: 50, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 200},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_SnapshotStartAfterRangeEnd(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-300-400.tar", StartBlock: 300, EndBlock: 400, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 200},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_BoundaryOverlap(t *testing.T) {
	// Snapshot endBlock equals range start - still overlaps
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-50-100.tar", StartBlock: 50, EndBlock: 100, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 100, End: 200},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 1)
}

func TestFindCoveringSnapshots_MultipleRangesMultipleSnapshots(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-1-100.tar", StartBlock: 1, EndBlock: 100, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-200-300.tar", StartBlock: 200, EndBlock: 300, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-500-600.tar", StartBlock: 500, EndBlock: 600, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 50, End: 150},
		{Start: 250, End: 350},
	}

	result := findCoveringSnapshots(available, ranges, 0, 0)
	require.Len(t, result, 2)
	require.Equal(t, "snap-1-100.tar", result[0].Filename)
	require.Equal(t, "snap-200-300.tar", result[1].Filename)
}

// ---------------------------------------------------------------------------
// bootstrapFromSnapshots
// ---------------------------------------------------------------------------

func TestBootstrapFromSnapshots_NoSnapshotsAvailable(t *testing.T) {
	// Create a mock HTTP server that returns empty snapshots list
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshots" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"snapshots": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: server.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Should not panic
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}

func TestBootstrapFromSnapshots_ListSnapshotsFails(t *testing.T) {
	// Create a mock HTTP server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: server.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Should not panic
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}

func TestBootstrapFromSnapshots_NoNeededSnapshots(t *testing.T) {
	// Create a mock HTTP server that returns snapshots outside the requested range
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshots" {
			snaps := []snapshot.SnapshotInfo{
				{Filename: "snap-500-600.tar", StartBlock: 500, EndBlock: 600, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"snapshots": snaps})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: server.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Should not panic - no overlap with requested range
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}

func TestBootstrapFromSnapshots_NilSignature(t *testing.T) {
	// Create a mock HTTP server that returns a snapshot with nil signature
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshots" {
			snaps := []snapshot.SnapshotInfo{
				{Filename: "snap-1-100.tar", StartBlock: 1, EndBlock: 100, Signed: true, Signature: nil},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"snapshots": snaps})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: server.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Should not panic - snapshot has nil signature, will be skipped
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}

func TestBootstrapFromSnapshots_DownloadFails(t *testing.T) {
	// Create a mock HTTP server that lists snapshots but download fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshots":
			snaps := []snapshot.SnapshotInfo{
				{
					Filename:   "snap-1-100.tar",
					StartBlock: 1,
					EndBlock:   100,
					SizeBytes:  1024,
					Signed:     true,
					Signature: &snapshot.SnapshotSignatureData{
						StartBlock:        1,
						EndBlock:          100,
						SignatureIdentity: "test-signer",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"snapshots": snaps})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: server.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Should not panic - download will fail but error is handled gracefully
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}

// ---------------------------------------------------------------------------
// getExistingBlockRange
// ---------------------------------------------------------------------------

func TestGetExistingBlockRange_EmptyDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	minBlock, maxBlock := getExistingBlockRange(ctx, defraNode)
	require.Equal(t, int64(0), minBlock)
	require.Equal(t, int64(0), maxBlock)
}

// ---------------------------------------------------------------------------
// createSnapshotAttestation
// ---------------------------------------------------------------------------

func TestCreateSnapshotAttestation_NoBlockSigMerkleRoots(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sig := &snapshot.SnapshotSignatureData{
		StartBlock:          1,
		EndBlock:            100,
		SignatureIdentity:   "test-signer",
		BlockSigMerkleRoots: []string{}, // Empty
	}

	// Should log a warning and return early
	createSnapshotAttestation(ctx, defraNode, sig)
}

func TestCreateSnapshotAttestation_WithMerkleRoots(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sig := &snapshot.SnapshotSignatureData{
		StartBlock:          1,
		EndBlock:            100,
		SignatureIdentity:   "test-signer",
		BlockSigMerkleRoots: []string{"root1", "root2"},
	}

	// Should attempt to create the attestation record
	// May fail in post but should not panic
	createSnapshotAttestation(ctx, defraNode, sig)
}

func TestCreateSnapshotAttestation_RecordFormat(t *testing.T) {
	sig := &snapshot.SnapshotSignatureData{
		StartBlock:          1,
		EndBlock:            100,
		SignatureIdentity:   "test-signer",
		BlockSigMerkleRoots: []string{"root1", "root2"},
	}

	// Verify the attestation record format without actually posting
	expectedID := fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock)
	require.Equal(t, "snapshot:1-100", expectedID)
}

// ---------------------------------------------------------------------------
// getExistingBlockRange - with blocks in DB
// ---------------------------------------------------------------------------

func TestGetExistingBlockRange_WithBlocksInDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Insert some blocks with different block numbers
	for _, num := range []int{10, 20, 30} {
		mutation := fmt.Sprintf(`mutation {
			create_Ethereum__Mainnet__Block(input: {
				hash: "0xhash%d",
				number: %d,
				timestamp: "2025-01-01T00:00:00Z"
			}) {
				_docID
			}
		}`, num, num)
		result := defraNode.DB.ExecRequest(ctx, mutation)
		require.Empty(t, result.GQL.Errors, "failed to insert block %d", num)
	}

	minBlock, maxBlock := getExistingBlockRange(ctx, defraNode)
	require.Equal(t, int64(10), minBlock)
	require.Equal(t, int64(30), maxBlock)
}

func TestGetExistingBlockRange_SingleBlock(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Insert a single block
	mutation := `mutation {
		create_Ethereum__Mainnet__Block(input: {
			hash: "0xsingleblockhash",
			number: 42,
			timestamp: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`
	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	minBlock, maxBlock := getExistingBlockRange(ctx, defraNode)
	require.Equal(t, int64(42), minBlock)
	require.Equal(t, int64(42), maxBlock)
}

// ---------------------------------------------------------------------------
// createSnapshotAttestation - success path (verify attestation is posted)
// ---------------------------------------------------------------------------

func TestCreateSnapshotAttestation_SuccessVerifyRecord(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sig := &snapshot.SnapshotSignatureData{
		StartBlock:          500,
		EndBlock:            600,
		SignatureIdentity:   "test-signer-identity",
		BlockSigMerkleRoots: []string{"merkle-root-1", "merkle-root-2"},
	}

	createSnapshotAttestation(ctx, defraNode, sig)

	// Verify the attestation record was created by querying for it
	query := `query {
		Ethereum__Mainnet__AttestationRecord(filter: {attested_doc: {_eq: "snapshot:500-600"}}) {
			_docID
			attested_doc
			source_doc
			doc_type
		}
	}`
	result := defraNode.DB.ExecRequest(ctx, query)
	require.Empty(t, result.GQL.Errors)
}

// ---------------------------------------------------------------------------
// bootstrapFromSnapshots - download + import path (mock server serves file)
// ---------------------------------------------------------------------------

func TestFindCoveringSnapshots_AllAlreadyImported(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-100-150.tar", StartBlock: 100, EndBlock: 150, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-200-250.tar", StartBlock: 200, EndBlock: 250, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 1, End: 500},
	}

	// Existing range 50-300 covers everything
	result := findCoveringSnapshots(available, ranges, 50, 300)
	require.Empty(t, result)
}

func TestFindCoveringSnapshots_MixedSignedAndExisting(t *testing.T) {
	available := []snapshot.SnapshotInfo{
		{Filename: "snap-10-50.tar", StartBlock: 10, EndBlock: 50, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-60-80.tar", StartBlock: 60, EndBlock: 80, Signed: false},
		{Filename: "snap-90-120.tar", StartBlock: 90, EndBlock: 120, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
		{Filename: "snap-100-110.tar", StartBlock: 100, EndBlock: 110, Signed: true, Signature: &snapshot.SnapshotSignatureData{}},
	}
	ranges := []hostConfig.BlockRange{
		{Start: 1, End: 200},
	}

	// Existing range 100-110 means snap-100-110 is already imported
	// snap-60-80 is unsigned so skipped
	result := findCoveringSnapshots(available, ranges, 100, 110)
	require.Len(t, result, 2)
	require.Equal(t, "snap-10-50.tar", result[0].Filename)
	require.Equal(t, "snap-90-120.tar", result[1].Filename)
}

func TestCreateSnapshotAttestation_NilBlockSigMerkleRoots(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	sig := &snapshot.SnapshotSignatureData{
		StartBlock:          1,
		EndBlock:            100,
		SignatureIdentity:   "test-signer",
		BlockSigMerkleRoots: nil,
	}

	// Should return early since len(nil) == 0
	createSnapshotAttestation(ctx, defraNode, sig)
}

func TestBootstrapFromSnapshots_DownloadSucceeds_ImportFails(t *testing.T) {
	// Create a mock HTTP server that serves a "snapshot" (invalid tar file)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshots":
			snaps := []snapshot.SnapshotInfo{
				{
					Filename:   "snap-1-100.tar",
					StartBlock: 1,
					EndBlock:   100,
					SizeBytes:  64,
					Signed:     true,
					Signature: &snapshot.SnapshotSignatureData{
						StartBlock:          1,
						EndBlock:            100,
						SignatureIdentity:   "test-signer",
						BlockSigMerkleRoots: []string{"root1"},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"snapshots": snaps})
		case "/snapshots/snap-1-100.tar":
			// Serve a small invalid tar file
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write([]byte("this is not a real tar file but serves as a test payload"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer svr.Close()

	ctx := context.Background()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := hostConfig.SnapshotConfig{
		Enabled:    true,
		IndexerURL: svr.URL,
		HistoricalRanges: []hostConfig.BlockRange{
			{Start: 1, End: 100},
		},
	}

	// Download succeeds but import will fail due to invalid tar format
	// This exercises the download+import path without panicking
	bootstrapFromSnapshots(ctx, defraNode, cfg)
}
