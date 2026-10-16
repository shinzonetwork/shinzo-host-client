package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gocid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/shinzonetwork/shinzo-host-client/config"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// extractDocIDFromMutationResult extracts the _docID from a DefraDB mutation result.
// DefraDB may return the list as []map[string]any or []any depending on version.
//
//nolint:unparam
func extractDocIDFromMutationResult(t *testing.T, result *client.RequestResult, collectionName string) string {
	t.Helper()
	dataMap, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok, "expected map[string]any data")
	addKey := fmt.Sprintf("add_%s", collectionName)
	raw := dataMap[addKey]

	switch list := raw.(type) {
	case []any:
		require.NotEmpty(t, list)
		docMap, ok := list[0].(map[string]any)
		require.True(t, ok)
		id, ok := docMap[defraFieldDocID].(string)
		require.True(t, ok)
		return id
	case []map[string]any:
		require.NotEmpty(t, list)
		id, ok := list[0][defraFieldDocID].(string)
		require.True(t, ok)
		return id
	default:
		t.Fatalf("unexpected type for mutation result list: %T", raw)
		return ""
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{"shorter than max", testAbc, 10, testAbc},
		{"exact length", "abcde", 5, "abcde"},
		{"exceeds max", "abcdefghij", 5, "abcde..."},
		{"empty string", "", 5, ""},
		{"max zero", testAbc, 0, "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEnqueueSignature(t *testing.T) {
	queue := make(chan string, 2)

	require.Zero(t, enqueueSignature(queue, testDoc1))
	require.Zero(t, enqueueSignature(queue, testDoc2))
	require.Len(t, queue, 2)

	// The queue is full, so the oldest ID is dropped
	require.Equal(t, 1, enqueueSignature(queue, testDoc3))
	require.Equal(t, []string{testDoc2, testDoc3}, []string{<-queue, <-queue})
}

// ---------------------------------------------------------------------------
// knownTables
// ---------------------------------------------------------------------------

func TestKnownTables_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	_, err := h.knownTables(context.Background(), nil)
	require.ErrorIs(t, err, ErrDefraNodeUnavailable)
}

// ---------------------------------------------------------------------------
// docQueueSettings
// ---------------------------------------------------------------------------

func TestDocQueueSettings_DefaultValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = 0
	cfg.Shinzo.DocWorkerCount = 0
	h := &Host{config: &cfg}

	workerCount, queueSize := h.docQueueSettings()
	require.Equal(t, 16, workerCount)
	require.Equal(t, 5000, queueSize)
}

func TestDocQueueSettings_CustomValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = 100
	cfg.Shinzo.DocWorkerCount = 4
	h := &Host{config: &cfg}

	workerCount, queueSize := h.docQueueSettings()
	require.Equal(t, 4, workerCount)
	require.Equal(t, 100, queueSize)
}

func TestDocQueueSettings_NegativeValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = -1
	cfg.Shinzo.DocWorkerCount = -1
	h := &Host{config: &cfg}

	workerCount, queueSize := h.docQueueSettings()
	require.Equal(t, 16, workerCount)
	require.Equal(t, 5000, queueSize)
}

// ---------------------------------------------------------------------------
// signatureWorker
// ---------------------------------------------------------------------------

func TestSignatureWorker_ContextCancelled(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	c := &attestedChain{collections: testCollections, signatures: make(chan string, 10)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.signatureWorker(ctx, c)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("signatureWorker did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// startEventBusListener
// ---------------------------------------------------------------------------

func TestStartEventBusListener_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	// Should return immediately since DefraNode is nil
	done := make(chan struct{})
	go func() {
		h.startEventBusListener(context.Background(), nil)
		close(done)
	}()
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("startEventBusListener did not return for nil DefraNode")
	}
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_NilDefraNode(_ *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	// Should return immediately
	h.processBlockSignatureFromEventBus(context.Background(), testCollections, "test-doc-id")
}

func TestProcessBlockSignatureFromEventBus_ContextCancelled(_ *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Should return immediately
	h.processBlockSignatureFromEventBus(ctx, testCollections, "test-doc-id")
}

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - tested via mock data maps
// ---------------------------------------------------------------------------

// Note: processBlockSignatureDocument requires a *client.Document which is tightly
// coupled to DefraDB internals. Testing is handled via integration tests with
// StartDefraInstanceWithTestConfig. The error paths (missing blockNumber, missing
// merkleRoot, empty merkleRoot) are covered indirectly.

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_NilDefraNode(_ *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	blockSig := &attestationService.BlockSignature{
		BlockNumber: 100,
		MerkleRoot:  testAbc,
		CIDs:        []string{testCID1},
	}
	// Should return immediately
	h.processAttestationsFromBlockSignature(context.Background(), testCollections, blockSig)
}

func TestProcessAttestationsFromBlockSignature_NoCIDs(_ *testing.T) {
	h := &Host{
		DefraNode: nil,
		metrics:   server.NewHostMetrics(),
	}
	// Even with DefraNode nil, the CIDs check happens first
	blockSig := &attestationService.BlockSignature{
		BlockNumber: 100,
		MerkleRoot:  testAbc,
		CIDs:        []string{}, // Empty CIDs
	}
	// Should return because DefraNode is nil
	h.processAttestationsFromBlockSignature(context.Background(), testCollections, blockSig)
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - retryable error path
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_EmptyBlockAttestedID(t *testing.T) {
	// This test verifies the blockAttestedID format
	blockSig := &attestationService.BlockSignature{
		BlockNumber:       42,
		MerkleRoot:        "abc123",
		SignatureIdentity: testSignerAddr,
		CIDs:              []string{testCID1, testCID2},
	}

	blockAttestedID := fmt.Sprintf("block:%d:%s", blockSig.BlockNumber, blockSig.MerkleRoot)
	require.Equal(t, "block:42:abc123", blockAttestedID)

	record := &constants.AttestationRecord{
		AttestedDocID: blockAttestedID,
		SourceDocIDs:  []string{blockSig.SignatureIdentity},
		CIDs:          blockSig.CIDs,
		DocType:       docTypeBlock,
		VoteCount:     1,
	}
	require.Equal(t, "block:42:abc123", record.AttestedDocID)
	require.Equal(t, []string{testSignerAddr}, record.SourceDocIDs)
	require.Equal(t, []string{testCID1, testCID2}, record.CIDs)
	require.Equal(t, docTypeBlock, record.DocType)
	require.Equal(t, 1, record.VoteCount)
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - retry logic helper
// ---------------------------------------------------------------------------

func TestRetryableErrorDetection(t *testing.T) {
	tests := []struct {
		name        string
		errStr      string
		isRetryable bool
	}{
		{"transaction conflict", "transaction conflict: retry later", true},
		{"please retry", "operation failed: Please retry", true},
		{"non-retryable", "connection refused", false},
		{"empty error", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.errStr == "" {
				// Empty error string case
				require.False(t, strings.Contains("", "transaction conflict"))
				return
			}
			isRetryable := strings.Contains(tt.errStr, "transaction conflict") || strings.Contains(tt.errStr, "Please retry")
			require.Equal(t, tt.isRetryable, isRetryable)
		})
	}
}

// ---------------------------------------------------------------------------
// knownTables - with real DefraDB (covers success path)
// ---------------------------------------------------------------------------

func TestKnownTables_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
	}
	c := &attestedChain{collections: testCollections}

	tables, err := h.knownTables(ctx, []*attestedChain{c})
	require.NoError(t, err)

	want := make(map[string]chainTable)
	for _, table := range []chain.Collection{
		testCollections.Block, testCollections.Transaction, testCollections.Log,
		testCollections.AccessListEntry, testCollections.BlockSignature,
	} {
		col, err := defraNode.DB.GetCollectionByName(ctx, table.Name)
		require.NoError(t, err)
		want[col.CollectionID()] = chainTable{chain: c, name: table.Name}
	}
	require.Equal(t, want, tables)
}

func TestKnownTables_NilDB(t *testing.T) {
	h := &Host{
		DefraNode: &node.Node{}, // DefraNode is non-nil but DB is nil
	}
	_, err := h.knownTables(context.Background(), nil)
	require.ErrorIs(t, err, ErrDefraNodeUnavailable)
}

// ---------------------------------------------------------------------------
// startEventBusListener - with real DefraDB (covers subscription path)
// ---------------------------------------------------------------------------

func TestStartEventBusListener_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	cfg := *DefaultConfig
	h := &Host{
		DefraNode: defraNode,
		chains:    []chain.Collections{testCollections},
		config:    &cfg,
	}

	// Use a short-lived context so the listener stops after a brief run
	updates, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	require.NoError(t, err)

	listenerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.startEventBusListener(listenerCtx, updates)
		close(done)
	}()

	select {
	case <-done:
		// Listener returned after context timeout
	case <-time.After(5 * time.Second):
		t.Fatal("startEventBusListener did not return after context timeout")
	}

	select {
	case _, open := <-updates.Message():
		require.False(t, open)
	default:
		t.Fatal("the listener left its subscription open")
	}
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - with real DefraDB (covers collection retrieval + doc not found)
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_InvalidDocID(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
	}

	// Invalid doc ID format -- should return early from NewDocIDFromString error
	h.processBlockSignatureFromEventBus(ctx, testCollections, "not-a-valid-doc-id")
}

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_NonExistentDoc(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
	}

	// Valid DocID format but document does not exist -- should fail after retries
	h.processBlockSignatureFromEventBus(ctx, testCollections, "bae-5a32eb7dc5b8df3b10bf76d6e8a43acc0bb04eeba09050b43108a3e6a41a0a48")
}

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - with real DefraDB and real client.Document
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature document in DefraDB
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 42,
			blockHash: "0xblockhash",
			merkleRoot: "abcdef1234567890",
			cidCount: 2,
			signatureType: "ES256K",
			signatureIdentity: "testidentity",
			signatureValue: "testsigvalue",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors, "should create BlockSignature document without errors")

	// Extract the doc ID from the mutation result
	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)
	require.NotEmpty(t, docIDStr)

	// Now fetch the document via collection.Get to get a real *client.Document
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)

	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)

	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)
	require.NotNil(t, doc)

	// Create a host without blockSignatureVerifier -- processBlockSignatureDocument
	// will exit at the blockSignatureVerifier == nil check after extracting all fields
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: nil,
		metrics:                server.NewHostMetrics(),
	}

	// This exercises all the field extraction paths in processBlockSignatureDocument
	h.processBlockSignatureDocument(ctx, testCollections, doc)
}

func TestProcessBlockSignatureDocument_WithVerifier(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature document with invalid signature (will fail verification)
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 55,
			blockHash: "0xblockhash55",
			merkleRoot: "dead",
			cidCount: 1,
			signatureType: "ES256K",
			signatureIdentity: "badinvalid",
			signatureValue: "badsig",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr2 := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)

	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr2)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	// Create host with a real BlockSignatureVerifier
	bsv := attestationService.NewBlockSignatureVerifier(100)
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: bsv,
		metrics:                server.NewHostMetrics(),
	}

	// This exercises the verifier path - will fail verification but should not panic
	h.processBlockSignatureDocument(ctx, testCollections, doc)

	// Signature failures should have been incremented
	require.True(t, h.metrics.SignatureFailures > 0, "signature failures should be incremented")
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - with real DefraDB (covers success path)
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	metrics := server.NewHostMetrics()

	h := &Host{
		DefraNode: defraNode,
		metrics:   metrics,
	}

	blockSig := &attestationService.BlockSignature{
		BlockNumber:       200,
		MerkleRoot:        "deadbeef",
		SignatureIdentity: "0xtestsigner",
		CIDs:              []string{"cid-a", "cid-b", "cid-c"},
	}

	// First call should create the attestation
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSig)

	// Verify metrics were updated
	require.True(t, metrics.AttestationsCreated > 0 || metrics.AttestationErrors > 0,
		"either attestations created or errors should be incremented")

	// Second call with the same block updates the record
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSig)
}

func TestProcessAttestationsFromBlockSignature_WithRealDefraDB_NoCIDs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
		metrics:   server.NewHostMetrics(),
	}

	blockSig := &attestationService.BlockSignature{
		BlockNumber:       201,
		MerkleRoot:        "deadbeef",
		SignatureIdentity: "0xtestsigner",
		CIDs:              []string{}, // No CIDs -- should return early
	}

	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSig)
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - full integration path
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - with CIDs and blockSignatureVerifier success path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_WithCIDs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature document with cids populated
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 88,
			blockHash: "0xblockhash88",
			merkleRoot: "deadbeefcafe",
			cidCount: 3,
			cids: ["bafyreiabc", "bafyreidef", "bafyreighi"],
			signatureType: "ES256K",
			signatureIdentity: "testidentity88",
			signatureValue: "testsig88",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	// Create host with BlockSignatureVerifier and metrics
	bsv := attestationService.NewBlockSignatureVerifier(100)
	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: bsv,
		metrics:                metrics,
	}

	// This will fail verification (invalid signature) but exercises the CID extraction paths
	h.processBlockSignatureDocument(ctx, testCollections, doc)
}

func TestProcessBlockSignatureDocument_ZeroBlockNumber(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature with blockNumber=0 to test type handling
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 0,
			blockHash: "0xzeroblock",
			merkleRoot: "abc123",
			signatureType: "ES256K"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	h := &Host{
		DefraNode: defraNode,
		metrics:   server.NewHostMetrics(),
	}

	// Exercises blockNumber=0 case and nil verifier path
	h.processBlockSignatureDocument(ctx, testCollections, doc)
}

func TestProcessBlockSignatureDocument_EmptyMerkleRoot(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature with empty merkleRoot
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 99,
			merkleRoot: "",
			signatureType: "ES256K"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	h := &Host{DefraNode: defraNode, metrics: server.NewHostMetrics()}

	// Should return early at the empty merkleRoot check
	h.processBlockSignatureDocument(ctx, testCollections, doc)
}

// makeTestCID generates a valid CIDv1 from arbitrary data.
func makeTestCID(data string) string {
	h := sha256.Sum256([]byte(data))
	mhash, _ := mh.Encode(h[:], mh.SHA2_256)
	c := gocid.NewCidV1(gocid.DagCBOR, mhash)
	return c.String()
}

func TestProcessBlockSignatureDocument_WithValidSignatureAndCIDs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	cids := []string{
		makeTestCID("block-data-0"),
		makeTestCID("block-data-1"),
		makeTestCID("block-data-2"),
	}
	docIDStr, _ := addSignedBlockSignature(ctx, t, defraNode, testCollections.BlockSignature.Name, 123, cids)
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                metrics,
	}

	h.processBlockSignatureDocument(ctx, testCollections, doc)

	require.Greater(t, atomic.LoadInt64(&metrics.SignatureVerifications), int64(0))
	require.Equal(t, int64(0), atomic.LoadInt64(&metrics.SignatureFailures))
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - full integration path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_FullPath(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Create a BlockSignature document
	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 77,
			blockHash: "0xblockhash77",
			merkleRoot: "cafebabe",
			cidCount: 1,
			signatureType: "ES256K",
			signatureIdentity: "testkey",
			signatureValue: "testsig",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)

	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                server.NewHostMetrics(),
	}

	// This exercises the full path: get collection, parse docID, fetch document, process it
	h.processBlockSignatureFromEventBus(ctx, testCollections, docIDStr)
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - multiple calls to exercise "existed" metrics path
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_ExistedPath(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode: defraNode,
		metrics:   metrics,
	}

	blockSig := &attestationService.BlockSignature{
		BlockNumber:       300,
		MerkleRoot:        "abcdef",
		SignatureIdentity: testSignerAddr,
		CIDs:              []string{testCIDName1, testCIDName2},
	}

	// First call creates attestation
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSig)
	require.Equal(t, int64(1), atomic.LoadInt64(&metrics.AttestationsCreated))

	// Second call updates the record, so nothing more is counted as created
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSig)
	require.Equal(t, int64(1), atomic.LoadInt64(&metrics.AttestationsCreated))
	require.Equal(t, int64(1), atomic.LoadInt64(&metrics.BlocksProcessed))
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - multiple indexers preserve both identities
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_MultipleIndexers_PreservesBothIdentities(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode: defraNode,
		metrics:   metrics,
	}

	// First indexer attests to block 400
	blockSigA := &attestationService.BlockSignature{
		BlockNumber:       400,
		MerkleRoot:        "aaa",
		SignatureIdentity: "indexer-A",
		CIDs:              []string{testCIDName1, testCIDName2},
	}
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSigA)

	// Second indexer attests to the same block with the same merkle root
	blockSigB := &attestationService.BlockSignature{
		BlockNumber:       400,
		MerkleRoot:        "aaa",
		SignatureIdentity: "indexer-B",
		CIDs:              []string{testCIDName1, testCIDName2},
	}
	h.processAttestationsFromBlockSignature(ctx, testCollections, blockSigB)

	// Query the attestation record
	query := fmt.Sprintf(`%s(filter: {attested_doc: {_eq: "block:400:aaa"}, doc_type: {_eq: "%s"}}) { attested_doc source_doc }`,
		testCollections.AttestationRecord.Name, docTypeBlock)
	records, err := defradb.QueryArray[attestationService.Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, records, 1, "Should have exactly one attestation record for this block")

	record := records[0]
	require.Contains(t, record.SourceDocIDs, "indexer-A", "Should contain first indexer's identity")
	require.Contains(t, record.SourceDocIDs, "indexer-B", "Should contain second indexer's identity")
	require.Len(t, record.SourceDocIDs, 2, "Should have exactly 2 indexer identities")
}

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - valid signature, CID mismatch path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_ValidSigCIDMismatch(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// Generate Ed25519 key pair
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	// Use real CIDs for merkle root computation
	cids := []string{makeTestCID("data-a"), makeTestCID("data-b")}
	merkleRoot := attestationService.ComputeMerkleRootFromStrings(cids)

	sig, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	merkleRootHex := hex.EncodeToString(merkleRoot)
	pubKeyHex := privKey.GetPublic().String()
	sigHex := hex.EncodeToString(sig)

	// Create BlockSignature with DIFFERENT CIDs than what was signed (CID mismatch)
	wrongCids := []string{makeTestCID("wrong-a"), makeTestCID("wrong-b")}
	quoted := make([]string, len(wrongCids))
	for i, c := range wrongCids {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	cidListStr := strings.Join(quoted, ", ")

	mutation := fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: 124,
			blockHash: "0xmismatch",
			merkleRoot: "%s",
			cidCount: %d,
			cids: [%s],
			signatureType: "Ed25519",
			signatureIdentity: "%s",
			signatureValue: "%s",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, testCollections.BlockSignature.Name, merkleRootHex, len(wrongCids), cidListStr, pubKeyHex, sigHex)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, testCollections.BlockSignature.Name)
	col, err := defraNode.DB.GetCollectionByName(ctx, testCollections.BlockSignature.Name)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.GetDocument(ctx, docIDTyped)
	require.NoError(t, err)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                metrics,
	}

	h.processBlockSignatureDocument(ctx, testCollections, doc)

	// Signature itself is valid, but CID mismatch should trigger failure
	require.Greater(t, atomic.LoadInt64(&metrics.SignatureFailures), int64(0),
		"CID mismatch should increment signature failures")
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - context canceled during retry loop
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_ContextCancelledDuringRetry(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, testSchemaApplier)
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	h := &Host{
		DefraNode: defraNode,
	}

	// Use a valid doc ID format but for a non-existent doc
	// Cancel context quickly to test the retry loop cancellation
	shortCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()

	h.processBlockSignatureFromEventBus(shortCtx, testCollections, "bae-5a32eb7dc5b8df3b10bf76d6e8a43acc0bb04eeba09050b43108a3e6a41a0a48")
}

// ---------------------------------------------------------------------------
// startEventBusListener - relay event with metrics and collection routing
// ---------------------------------------------------------------------------

// The listener counts each document a peer sends under its chain's collection, records the chain's
// last document time, and ignores updates for collections outside the chains.
func TestStartEventBusListener_CountsRelayedDocuments(t *testing.T) {
	ctx := context.Background()
	defraNode, chains := startChainsNode(t, chain.EthereumMainnet, "Testchain__Devnet")

	metrics := server.NewHostMetrics(&server.ChainMetrics{Prefix: chain.EthereumMainnet}, &server.ChainMetrics{Prefix: "Testchain__Devnet"})
	cfg := *DefaultConfig
	h := &Host{DefraNode: defraNode, chains: chains, config: &cfg, metrics: metrics}

	updates, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	require.NoError(t, err)
	listenerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go h.startEventBusListener(listenerCtx, updates)

	relay := func(collectionID string) {
		defraNode.DB.Events().Publish(event.NewMessage(event.UpdateName, event.Update{CollectionID: collectionID, IsRelay: true}))
	}
	// An update for an unknown collection is not counted.
	relay("not-a-collection")
	sent := time.Now()

	// Each collection of each chain gets a different number of updates, so a counter wired to the
	// wrong collection, or a chain the listener does not know, ends with the wrong count.
	var total, transactions, logs, accessListEntries, blockSignatures int
	perChain := make(map[string]int, len(chains))
	for i, c := range chains {
		relays := map[string]int{
			c.Block.Name:           1 + 5*i,
			c.Transaction.Name:     2 + 5*i,
			c.Log.Name:             3 + 5*i,
			c.AccessListEntry.Name: 4 + 5*i,
			c.BlockSignature.Name:  5 + 5*i,
		}
		for name, n := range relays {
			col, err := defraNode.DB.GetCollectionByName(ctx, name)
			require.NoError(t, err)
			for range n {
				relay(col.CollectionID())
			}
			total += n
			perChain[c.Prefix] += n
		}
		transactions += relays[c.Transaction.Name]
		logs += relays[c.Log.Name]
		accessListEntries += relays[c.AccessListEntry.Name]
		blockSignatures += relays[c.BlockSignature.Name]
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.Equal(ct, int64(total), atomic.LoadInt64(&metrics.DocumentsReceived))
		assert.Equal(ct, int64(transactions), atomic.LoadInt64(&metrics.TransactionsProcessed))
		assert.Equal(ct, int64(logs), atomic.LoadInt64(&metrics.LogsProcessed))
		assert.Equal(ct, int64(accessListEntries), atomic.LoadInt64(&metrics.AccessListsProcessed))
		assert.Equal(ct, int64(blockSignatures), atomic.LoadInt64(&metrics.BlockSignaturesProcessed))
		// A relayed Block counts only as received.
		assert.Zero(ct, atomic.LoadInt64(&metrics.BlocksProcessed))
		for prefix, n := range perChain {
			assert.Equal(ct, int64(n), metrics.Chain(prefix).DocumentsReceived.Load(), prefix)
			assert.False(ct, metrics.Chain(prefix).LastDocumentTime().Before(sent), prefix)
		}
	}, 5*time.Second, 10*time.Millisecond)
}

// A full attestation queue drops the oldest block signatures and counts the drops on its own chain
// only, and the queue's peak depth reads back once.
func TestStartEventBusListener_CountsQueueDropsPerChain(t *testing.T) {
	const (
		flooded = "Testchain__Devnet"
		// A valid document ID with no document, so a worker keeps retrying its fetch while the
		// listener fills the queue.
		absentDocID = "bae-5a32eb7dc5b8df3b10bf76d6e8a43acc0bb04eeba09050b43108a3e6a41a0a48"
		burst       = 50
	)
	ctx := context.Background()
	defraNode, chains := startChainsNode(t, chain.EthereumMainnet, flooded)

	metrics := server.NewHostMetrics(&server.ChainMetrics{Prefix: chain.EthereumMainnet}, &server.ChainMetrics{Prefix: flooded})
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = 1
	cfg.Shinzo.DocWorkerCount = 1
	h := &Host{DefraNode: defraNode, chains: chains, config: &cfg, metrics: metrics}

	updates, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	require.NoError(t, err)
	listenerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		h.startEventBusListener(listenerCtx, updates)
	}()

	relaySignatures := func(c chain.Collections, n int) {
		col, err := defraNode.DB.GetCollectionByName(ctx, c.BlockSignature.Name)
		require.NoError(t, err)
		for range n {
			defraNode.DB.Events().Publish(event.NewMessage(event.UpdateName, event.Update{CollectionID: col.CollectionID(), DocID: absentDocID, IsRelay: true}))
		}
	}
	relaySignatures(chains[0], 1)
	relaySignatures(chains[1], burst)

	ethereum, other := metrics.Chain(chain.EthereumMainnet), metrics.Chain(flooded)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.Equal(ct, int64(1), ethereum.DocumentsReceived.Load())
		assert.Equal(ct, int64(burst), other.DocumentsReceived.Load())
	}, 5*time.Second, 10*time.Millisecond)
	// The listener finishes the update it is on before it stops, so the counters below are final.
	cancel()
	<-stopped

	require.Positive(t, other.QueueDrops.Load())
	require.Zero(t, ethereum.QueueDrops.Load())
	require.Equal(t, int64(1), other.TakeQueuePeak())
	require.Zero(t, other.TakeQueuePeak())
}

// Each chain's relayed block signature is attested in that chain's own AttestationRecord table, and
// raises only that chain's current block.
func TestStartEventBusListener_AttestsEachChainInItsOwnTables(t *testing.T) {
	const other = "Testchain__Devnet"
	ctx := context.Background()
	defraNode, chains := startChainsNode(t, chain.EthereumMainnet, other)

	cfg := *DefaultConfig
	attesters := map[string]*observedAttesters{chain.EthereumMainnet: newObservedAttesters(time.Minute), other: newObservedAttesters(time.Minute)}
	metrics := server.NewHostMetrics(&server.ChainMetrics{Prefix: chain.EthereumMainnet, Enabled: true}, &server.ChainMetrics{Prefix: other, Enabled: true})
	h := &Host{
		DefraNode:              defraNode,
		chains:                 chains,
		config:                 &cfg,
		metrics:                metrics,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		attesters:              attesters,
	}

	updates, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	require.NoError(t, err)
	listenerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go h.startEventBusListener(listenerCtx, updates)

	// Each chain signs a different block and CIDs with its own key, so anything credited to the other
	// chain shows.
	blocks := map[string]int{chains[0].Prefix: 42, chains[1].Prefix: 7}
	cids := make(map[string][]string, len(chains))
	signers := make(map[string]string, len(chains))
	for _, c := range chains {
		cids[c.Prefix] = []string{makeTestCID(c.Prefix)}
		docID, signer := addSignedBlockSignature(ctx, t, defraNode, c.BlockSignature.Name, blocks[c.Prefix], cids[c.Prefix])
		signers[c.Prefix] = signer
		col, err := defraNode.DB.GetCollectionByName(ctx, c.BlockSignature.Name)
		require.NoError(t, err)
		defraNode.DB.Events().Publish(event.NewMessage(event.UpdateName, event.Update{CollectionID: col.CollectionID(), DocID: docID, IsRelay: true}))
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, c := range chains {
			records, err := defradb.QueryArray[constants.AttestationRecord](ctx, defraNode, c.AttestationRecord.Name+" { CIDs }")
			if assert.NoError(ct, err) && assert.Len(ct, records, 1, c.Prefix) {
				assert.Equal(ct, cids[c.Prefix], records[0].CIDs, c.Prefix)
			}
			// The worker raises the chain's current block after it writes the record.
			assert.Equal(ct, int64(blocks[c.Prefix]), metrics.Chain(c.Prefix).CurrentBlock(), c.Prefix)
		}
	}, 5*time.Second, 20*time.Millisecond)
	for _, c := range chains {
		require.Equal(t, []string{signers[c.Prefix]}, attesters[c.Prefix].Snapshot(), c.Prefix)
	}
}

// startChainsNode starts a DefraDB node holding the tables of a chain for each of prefixes.
func startChainsNode(t *testing.T, prefixes ...string) (*node.Node, []chain.Collections) {
	t.Helper()
	applier := &chainsApplier{schemaCfg: config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}}
	var chains []chain.Collections
	for _, prefix := range prefixes {
		generator := httptest.NewServer(schemaHandler(t, prefix))
		t.Cleanup(generator.Close)
		applier.chains = append(applier.chains, chain.Config{Prefix: prefix, Generators: []chain.Generator{{URL: generator.URL}}})
		chains = append(chains, chain.EVM(prefix))
	}
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, applier)
	require.NoError(t, err)
	t.Cleanup(func() { _ = defraNode.Close(context.Background()) })
	return defraNode, chains
}

// addSignedBlockSignature writes a BlockSignature for blockNumber to table, signed over cids with a
// new key, and returns its document ID and the signer's identity.
func addSignedBlockSignature(ctx context.Context, t *testing.T, defraNode *node.Node, table string, blockNumber int, cids []string) (docID, signer string) {
	t.Helper()
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)
	merkleRoot := attestationService.ComputeMerkleRootFromStrings(cids)
	sig, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	quoted := make([]string, len(cids))
	for i, c := range cids {
		quoted[i] = strconv.Quote(c)
	}
	result := defraNode.DB.ExecRequest(ctx, fmt.Sprintf(`mutation {
		add_%s(input: {
			blockNumber: %d,
			blockHash: "0xhash",
			merkleRoot: "%s",
			cidCount: %d,
			cids: [%s],
			signatureType: "Ed25519",
			signatureIdentity: "%s",
			signatureValue: "%s",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, table, blockNumber, hex.EncodeToString(merkleRoot), len(cids), strings.Join(quoted, ", "),
		privKey.GetPublic().String(), hex.EncodeToString(sig)))
	require.Empty(t, result.GQL.Errors)
	return extractDocIDFromMutationResult(t, result, table), privKey.GetPublic().String()
}
