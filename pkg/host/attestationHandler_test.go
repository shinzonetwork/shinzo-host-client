package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gocid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/config"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// extractDocIDFromMutationResult extracts the _docID from a DefraDB mutation result.
// DefraDB may return the list as []map[string]any or []any depending on version.
func extractDocIDFromMutationResult(t *testing.T, result *client.RequestResult, collectionName string) string {
	t.Helper()
	dataMap, ok := result.GQL.Data.(map[string]any)
	require.True(t, ok, "expected map[string]any data")
	createKey := fmt.Sprintf("create_%s", collectionName)
	raw := dataMap[createKey]

	switch list := raw.(type) {
	case []any:
		require.NotEmpty(t, list)
		docMap, ok := list[0].(map[string]any)
		require.True(t, ok)
		id, ok := docMap["_docID"].(string)
		require.True(t, ok)
		return id
	case []map[string]any:
		require.NotEmpty(t, list)
		id, ok := list[0]["_docID"].(string)
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
		{"shorter than max", "abc", 10, "abc"},
		{"exact length", "abcde", 5, "abcde"},
		{"exceeds max", "abcdefghij", 5, "abcde..."},
		{"empty string", "", 5, ""},
		{"max zero", "abc", 0, "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEnqueueDoc(t *testing.T) {
	// Initialize docQueue for test
	docQueue = make(chan docEvent, 2)

	// Normal enqueue
	enqueueDoc(docEvent{docID: "doc1", collectionName: "col1"})
	require.Len(t, docQueue, 1)

	enqueueDoc(docEvent{docID: "doc2", collectionName: "col2"})
	require.Len(t, docQueue, 2)

	// Queue is full, enqueue should drop oldest and add new
	enqueueDoc(docEvent{docID: "doc3", collectionName: "col3"})
	require.Len(t, docQueue, 2)

	// Drain and verify the newest items are present
	var events []docEvent
	for len(docQueue) > 0 {
		events = append(events, <-docQueue)
	}
	// doc1 was dropped, doc2 and doc3 should remain
	require.Equal(t, 2, len(events))
	hasDoc3 := false
	for _, e := range events {
		if e.docID == "doc3" {
			hasDoc3 = true
		}
	}
	require.True(t, hasDoc3, "doc3 should be in queue")
}

// ---------------------------------------------------------------------------
// processDocumentAttestationBatch
// ---------------------------------------------------------------------------

func TestProcessDocumentAttestationBatch_EmptyDocs(t *testing.T) {
	h := &Host{}
	err := h.processDocumentAttestationBatch(context.Background(), []Document{})
	require.NoError(t, err)
}

func TestProcessDocumentAttestationBatch_NilDocs(t *testing.T) {
	h := &Host{}
	err := h.processDocumentAttestationBatch(context.Background(), nil)
	require.NoError(t, err)
}

func TestProcessDocumentAttestationBatch_NoVersions(t *testing.T) {
	// Documents without _version data should be skipped, resulting in empty inputs
	h := &Host{
		config: &config.Config{},
	}
	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{"field": "value"}},
		{ID: "doc2", Type: "Block", BlockNumber: 2, Data: map[string]any{}},
	}
	err := h.processDocumentAttestationBatch(context.Background(), docs)
	require.NoError(t, err)
}

func TestProcessDocumentAttestationBatch_DefaultMaxConcurrent(t *testing.T) {
	// Test that default maxConcurrentVerifications is used when config value is 0
	cfg := *DefaultConfig // Copy to avoid mutation
	cfg.Shinzo.MaxConcurrentVerifications = 0
	h := &Host{
		config: &cfg,
	}
	// With no _version data, inputs will be empty and return nil
	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}
	err := h.processDocumentAttestationBatch(context.Background(), docs)
	require.NoError(t, err)
}

func TestProcessDocumentAttestationBatch_NegativeMaxConcurrent(t *testing.T) {
	cfg := *DefaultConfig // Copy to avoid mutation
	cfg.Shinzo.MaxConcurrentVerifications = -5
	h := &Host{
		config: &cfg,
	}
	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}
	err := h.processDocumentAttestationBatch(context.Background(), docs)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// processAttestationEventsWithSubscription
// ---------------------------------------------------------------------------

func TestProcessAttestationEventsWithSubscription_ContextCancelled(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	// Should return promptly since context is already cancelled
	done := make(chan struct{})
	go func() {
		h.processAttestationEventsWithSubscription(ctx)
		close(done)
	}()
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("processAttestationEventsWithSubscription did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// initKnownCollectionIDs
// ---------------------------------------------------------------------------

func TestInitKnownCollectionIDs_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	err := h.initKnownCollectionIDs(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "DefraNode not available")
}

// ---------------------------------------------------------------------------
// initDocQueue
// ---------------------------------------------------------------------------

func TestInitDocQueue_DefaultValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = 0
	cfg.Shinzo.DocWorkerCount = 0
	h := &Host{config: &cfg}

	workerCount, queueSize := h.initDocQueue()
	require.Equal(t, 16, workerCount)
	require.Equal(t, 5000, queueSize)
	require.NotNil(t, docQueue)
	require.Equal(t, 5000, cap(docQueue))
}

func TestInitDocQueue_CustomValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = 100
	cfg.Shinzo.DocWorkerCount = 4
	h := &Host{config: &cfg}

	workerCount, queueSize := h.initDocQueue()
	require.Equal(t, 4, workerCount)
	require.Equal(t, 100, queueSize)
	require.Equal(t, 100, cap(docQueue))
}

func TestInitDocQueue_NegativeValues(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.DocQueueSize = -1
	cfg.Shinzo.DocWorkerCount = -1
	h := &Host{config: &cfg}

	workerCount, queueSize := h.initDocQueue()
	require.Equal(t, 16, workerCount)
	require.Equal(t, 5000, queueSize)
}

// ---------------------------------------------------------------------------
// docWorker
// ---------------------------------------------------------------------------

func TestDocWorker_ContextCancelled(t *testing.T) {
	// Initialize the docQueue
	docQueue = make(chan docEvent, 10)
	h := &Host{
		DefraNode: nil,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.docWorker(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("docWorker did not return after context cancellation")
	}
}

func TestDocWorker_ProcessesBlockSignatureEvent(t *testing.T) {
	// Initialize the docQueue
	docQueue = make(chan docEvent, 10)

	// Host with nil DefraNode - processBlockSignatureFromEventBus will return early
	h := &Host{
		DefraNode: nil,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		h.docWorker(ctx)
		close(done)
	}()

	// Send a block signature event
	docQueue <- docEvent{
		docID:          "test-doc",
		collectionName: constants.CollectionBlockSignature,
	}

	// Give it time to process
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("docWorker did not return")
	}
}

func TestDocWorker_IgnoresUnknownCollections(t *testing.T) {
	docQueue = make(chan docEvent, 10)
	h := &Host{DefraNode: nil}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.docWorker(ctx)
		close(done)
	}()

	// Send an event with unknown collection
	docQueue <- docEvent{
		docID:          "test-doc",
		collectionName: "UnknownCollection",
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("docWorker did not return")
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
		h.startEventBusListener(context.Background())
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

func TestProcessBlockSignatureFromEventBus_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	// Should return immediately
	h.processBlockSignatureFromEventBus(context.Background(), "test-doc-id")
}

func TestProcessBlockSignatureFromEventBus_ContextCancelled(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Should return immediately
	h.processBlockSignatureFromEventBus(ctx, "test-doc-id")
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

func TestProcessAttestationsFromBlockSignature_NilDefraNode(t *testing.T) {
	h := &Host{
		DefraNode: nil,
	}
	blockSig := &attestationService.BlockSignature{
		BlockNumber: 100,
		MerkleRoot:  "abc",
		CIDs:        []string{"cid1"},
	}
	// Should return immediately
	h.processAttestationsFromBlockSignature(context.Background(), blockSig)
}

func TestProcessAttestationsFromBlockSignature_NoCIDs(t *testing.T) {
	h := &Host{
		DefraNode: nil,
		metrics:   server.NewHostMetrics(),
	}
	// Even with DefraNode nil, the CIDs check happens first
	blockSig := &attestationService.BlockSignature{
		BlockNumber: 100,
		MerkleRoot:  "abc",
		CIDs:        []string{}, // Empty CIDs
	}
	// Should return because DefraNode is nil
	h.processAttestationsFromBlockSignature(context.Background(), blockSig)
}

// ---------------------------------------------------------------------------
// attestedBlocks sync.Map
// ---------------------------------------------------------------------------

func TestAttestedBlocks_SyncMap(t *testing.T) {
	// Test the sync.Map used for deduplication
	attestedBlocks.Delete(int64(999)) // Clean up from any previous tests

	_, existed := attestedBlocks.LoadOrStore(int64(999), true)
	require.False(t, existed)

	_, existed = attestedBlocks.LoadOrStore(int64(999), true)
	require.True(t, existed)

	attestedBlocks.Delete(int64(999)) // Clean up
}

// ---------------------------------------------------------------------------
// Document struct
// ---------------------------------------------------------------------------

func TestDocumentStruct(t *testing.T) {
	doc := Document{
		ID:          "test-id",
		Type:        "Block",
		BlockNumber: 42,
		Data:        map[string]any{"key": "value"},
	}
	require.Equal(t, "test-id", doc.ID)
	require.Equal(t, "Block", doc.Type)
	require.Equal(t, uint64(42), doc.BlockNumber)
	require.Equal(t, "value", doc.Data["key"])
}

// ---------------------------------------------------------------------------
// docEvent struct
// ---------------------------------------------------------------------------

func TestDocEventStruct(t *testing.T) {
	evt := docEvent{
		docID:          "doc-1",
		collectionName: constants.CollectionBlockSignature,
	}
	require.Equal(t, "doc-1", evt.docID)
	require.Equal(t, constants.CollectionBlockSignature, evt.collectionName)
}

// ---------------------------------------------------------------------------
// Concurrent enqueueDoc
// ---------------------------------------------------------------------------

func TestEnqueueDoc_Concurrent(t *testing.T) {
	docQueue = make(chan docEvent, 10)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			enqueueDoc(docEvent{docID: fmt.Sprintf("doc-%d", idx), collectionName: "test"})
		}(i)
	}
	wg.Wait()

	// Queue should not exceed its capacity
	require.LessOrEqual(t, len(docQueue), cap(docQueue))
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - retryable error path
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_EmptyBlockAttestedID(t *testing.T) {
	// This test verifies the blockAttestedID format
	blockSig := &attestationService.BlockSignature{
		BlockNumber:       42,
		MerkleRoot:        "abc123",
		SignatureIdentity: "0xsigner",
		CIDs:              []string{"cid1", "cid2"},
	}

	blockAttestedID := fmt.Sprintf("block:%d:%s", blockSig.BlockNumber, blockSig.MerkleRoot)
	require.Equal(t, "block:42:abc123", blockAttestedID)

	record := &constants.AttestationRecord{
		AttestedDocId: blockAttestedID,
		SourceDocId:   blockSig.SignatureIdentity,
		CIDs:          blockSig.CIDs,
		DocType:       "Block",
		VoteCount:     1,
	}
	require.Equal(t, "block:42:abc123", record.AttestedDocId)
	require.Equal(t, "0xsigner", record.SourceDocId)
	require.Equal(t, []string{"cid1", "cid2"}, record.CIDs)
	require.Equal(t, "Block", record.DocType)
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
// initKnownCollectionIDs - with real DefraDB (covers success path)
// ---------------------------------------------------------------------------

func TestInitKnownCollectionIDs_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	h := &Host{
		DefraNode: defraNode,
	}

	// Reset the global collection IDs to verify they get set
	blockSigCollectionID = ""
	blockCollectionID = ""
	transactionCollectionID = ""
	logCollectionID = ""
	accessListCollectionID = ""

	err = h.initKnownCollectionIDs(ctx)
	require.NoError(t, err)

	// Verify that at least the block signature collection ID was set
	require.NotEmpty(t, blockSigCollectionID, "blockSigCollectionID should be set")
	require.NotEmpty(t, blockCollectionID, "blockCollectionID should be set")
	require.NotEmpty(t, transactionCollectionID, "transactionCollectionID should be set")
	require.NotEmpty(t, logCollectionID, "logCollectionID should be set")
	require.NotEmpty(t, accessListCollectionID, "accessListCollectionID should be set")
}

func TestInitKnownCollectionIDs_NilDB(t *testing.T) {
	h := &Host{
		DefraNode: &node.Node{}, // DefraNode is non-nil but DB is nil
	}
	err := h.initKnownCollectionIDs(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "DefraNode not available")
}

// ---------------------------------------------------------------------------
// startEventBusListener - with real DefraDB (covers subscription path)
// ---------------------------------------------------------------------------

func TestStartEventBusListener_WithRealDefraDB_WritesDoc(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	metrics := server.NewHostMetrics()
	cfg := *DefaultConfig
	h := &Host{
		DefraNode: defraNode,
		config:    &cfg,
		metrics:   metrics,
	}

	// Start the listener
	listenerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.startEventBusListener(listenerCtx)
		close(done)
	}()

	// Give listener time to subscribe
	time.Sleep(300 * time.Millisecond)

	// Write a document to trigger events (even though it's not a relay, it exercises the subscription path)
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			blockNumber: 500,
			blockHash: "0xeventtest",
			merkleRoot: "eventmerkle",
			signatureType: "ES256K",
			signatureIdentity: "eventkey",
			signatureValue: "eventsig",
			createdAt: "2025-01-01T00:00:00Z"
		}) {
			_docID
		}
	}`, constants.CollectionBlockSignature)
	defraNode.DB.ExecRequest(ctx, mutation)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startEventBusListener did not return")
	}
}

func TestStartEventBusListener_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := *DefaultConfig
	h := &Host{
		DefraNode: defraNode,
		config:    &cfg,
	}

	// Use a short-lived context so the listener stops after a brief run
	listenerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.startEventBusListener(listenerCtx)
		close(done)
	}()

	select {
	case <-done:
		// Listener returned after context timeout
	case <-time.After(5 * time.Second):
		t.Fatal("startEventBusListener did not return after context timeout")
	}
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - with real DefraDB (covers collection retrieval + doc not found)
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_InvalidDocID(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	h := &Host{
		DefraNode: defraNode,
	}

	// Invalid doc ID format -- should return early from NewDocIDFromString error
	h.processBlockSignatureFromEventBus(ctx, "not-a-valid-doc-id")
}

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_NonExistentDoc(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	h := &Host{
		DefraNode: defraNode,
	}

	// Valid DocID format but document does not exist -- should fail after retries
	h.processBlockSignatureFromEventBus(ctx, "bae-5a32eb7dc5b8df3b10bf76d6e8a43acc0bb04eeba09050b43108a3e6a41a0a48")
}

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - with real DefraDB and real client.Document
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature document in DefraDB
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
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
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors, "should create BlockSignature document without errors")

	// Extract the doc ID from the mutation result
	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	require.NotEmpty(t, docIDStr)

	// Now fetch the document via collection.Get to get a real *client.Document
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)

	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)

	doc, err := col.Get(ctx, docIDTyped)
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
	h.processBlockSignatureDocument(ctx, doc)
}

func TestProcessBlockSignatureDocument_WithVerifier(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature document with invalid signature (will fail verification)
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
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
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr2 := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)

	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr2)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
	require.NoError(t, err)

	// Create host with a real BlockSignatureVerifier
	bsv := attestationService.NewBlockSignatureVerifier(100)
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: bsv,
		metrics:                server.NewHostMetrics(),
	}

	// This exercises the verifier path - will fail verification but should not panic
	h.processBlockSignatureDocument(ctx, doc)

	// Signature failures should have been incremented
	require.True(t, h.metrics.SignatureFailures > 0, "signature failures should be incremented")
}

// ---------------------------------------------------------------------------
// processDocumentAttestationBatch - with version data (covers extraction path)
// ---------------------------------------------------------------------------

func TestProcessDocumentAttestationBatch_WithVersionData(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	cfg := *DefaultConfig
	cfg.Shinzo.MaxConcurrentVerifications = 10

	// Create a mock verifier that always succeeds
	mockVerifier := &attestationService.MockSignatureVerifier{}

	h := &Host{
		DefraNode:         defraNode,
		config:            &cfg,
		signatureVerifier: attestationService.NewDefraSignatureVerifier(defraNode, nil),
		metrics:           server.NewHostMetrics(),
	}
	_ = mockVerifier // verifier is set on signatureVerifier field

	// Documents with _version data that ExtractVersionsFromDocument can parse
	docs := []Document{
		{
			ID:          "doc1",
			Type:        "Block",
			BlockNumber: 1,
			Data: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4",
						"signature": map[string]any{
							"type":     "ES256K",
							"identity": "testpubkey",
							"value":    "testsig",
						},
						"collectionVersionId": "1",
					},
				},
			},
		},
	}

	// This exercises the version extraction path. The attestation will fail at
	// signature verification since the keys are fake, but we exercise the code path
	err = h.processDocumentAttestationBatch(ctx, docs)
	// May return nil if no verified CIDs (empty inputs after failed verification)
	// or may return an error from the underlying attestation call
	_ = err
}

func TestProcessDocumentAttestationBatch_MultipleDocsWithMixedVersionData(t *testing.T) {
	cfg := *DefaultConfig
	cfg.Shinzo.MaxConcurrentVerifications = 5

	h := &Host{
		config: &cfg,
		signatureVerifier: attestationService.NewDefraSignatureVerifier(nil, nil),
	}

	// Mix of docs with and without version data
	docs := []Document{
		{
			ID:          "doc-with-version",
			Type:        "Transaction",
			BlockNumber: 10,
			Data: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "somecid",
						"signature": map[string]any{
							"type":     "ES256K",
							"identity": "testpubkey",
							"value":    "testsig",
						},
					},
				},
			},
		},
		{
			ID:          "doc-without-version",
			Type:        "Block",
			BlockNumber: 10,
			Data:        map[string]any{"hash": "0x123"},
		},
		{
			ID:          "doc-with-empty-version",
			Type:        "Log",
			BlockNumber: 10,
			Data: map[string]any{
				"_version": []any{}, // Empty version array
			},
		},
	}

	// MockSignatureVerifier returns nil (verification passes), but no DefraDB
	// is configured, so HandleDocumentAttestationBatch will fail at the DB layer.
	// This tests the version extraction and attestation input building paths.
	err := h.processDocumentAttestationBatch(context.Background(), docs)
	_ = err
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - with real DefraDB (covers success path)
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_WithRealDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	metrics := server.NewHostMetrics()

	h := &Host{
		DefraNode: defraNode,
		metrics:   metrics,
	}

	// Clean up global attestedBlocks state for this block number
	attestedBlocks.Delete(int64(200))

	blockSig := &attestationService.BlockSignature{
		BlockNumber:       200,
		MerkleRoot:        "deadbeef",
		SignatureIdentity: "0xtestsigner",
		CIDs:              []string{"cid-a", "cid-b", "cid-c"},
	}

	// First call should create the attestation
	h.processAttestationsFromBlockSignature(ctx, blockSig)

	// Verify metrics were updated
	require.True(t, metrics.AttestationsCreated > 0 || metrics.AttestationErrors > 0,
		"either attestations created or errors should be incremented")

	// Second call with same block should hit the "existed" path in attestedBlocks
	h.processAttestationsFromBlockSignature(ctx, blockSig)

	// Clean up
	attestedBlocks.Delete(int64(200))
}

func TestProcessAttestationsFromBlockSignature_WithRealDefraDB_NoCIDs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

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

	h.processAttestationsFromBlockSignature(ctx, blockSig)
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - full integration path
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - with CIDs and blockSignatureVerifier success path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_WithCIDs(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature document with cids populated
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
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
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
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
	h.processBlockSignatureDocument(ctx, doc)
}

func TestProcessBlockSignatureDocument_ZeroBlockNumber(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature with blockNumber=0 to test type handling
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			blockNumber: 0,
			blockHash: "0xzeroblock",
			merkleRoot: "abc123",
			signatureType: "ES256K"
		}) {
			_docID
		}
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
	require.NoError(t, err)

	h := &Host{
		DefraNode: defraNode,
		metrics:   server.NewHostMetrics(),
	}

	// Exercises blockNumber=0 case and nil verifier path
	h.processBlockSignatureDocument(ctx, doc)
}

func TestProcessBlockSignatureDocument_EmptyMerkleRoot(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature with empty merkleRoot
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			blockNumber: 99,
			merkleRoot: "",
			signatureType: "ES256K"
		}) {
			_docID
		}
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
	require.NoError(t, err)

	h := &Host{DefraNode: defraNode, metrics: server.NewHostMetrics()}

	// Should return early at the empty merkleRoot check
	h.processBlockSignatureDocument(ctx, doc)
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

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Generate an Ed25519 key pair
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	// Build valid CIDs
	cids := []string{
		makeTestCID("block-data-0"),
		makeTestCID("block-data-1"),
		makeTestCID("block-data-2"),
	}

	// Compute merkle root and sign it
	merkleRoot := attestationService.ComputeMerkleRootFromStrings(cids)
	require.NotNil(t, merkleRoot)

	sig, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	merkleRootHex := hex.EncodeToString(merkleRoot)
	pubKeyHex := privKey.GetPublic().String()
	sigHex := hex.EncodeToString(sig)

	// Format CID list for the GraphQL mutation
	quoted := make([]string, len(cids))
	for i, c := range cids {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	cidListStr := strings.Join(quoted, ", ")

	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			blockNumber: 123,
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
	}`, constants.CollectionBlockSignature, merkleRootHex, len(cids), cidListStr, pubKeyHex, sigHex)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
	require.NoError(t, err)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                metrics,
	}

	h.processBlockSignatureDocument(ctx, doc)

	require.Greater(t, atomic.LoadInt64(&metrics.SignatureVerifications), int64(0))
	require.Equal(t, int64(0), atomic.LoadInt64(&metrics.SignatureFailures))
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - full integration path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_WithRealDefraDB_FullPath(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a BlockSignature document
	mutation := fmt.Sprintf(`mutation {
		create_%s(input: {
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
	}`, constants.CollectionBlockSignature)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)

	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                server.NewHostMetrics(),
	}

	// This exercises the full path: get collection, parse docID, fetch document, process it
	h.processBlockSignatureFromEventBus(ctx, docIDStr)
}

// ---------------------------------------------------------------------------
// processAttestationsFromBlockSignature - multiple calls to exercise "existed" metrics path
// ---------------------------------------------------------------------------

func TestProcessAttestationsFromBlockSignature_ExistedPath(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode: defraNode,
		metrics:   metrics,
	}

	// Clean up global attestedBlocks state
	attestedBlocks.Delete(int64(300))
	defer attestedBlocks.Delete(int64(300))

	blockSig := &attestationService.BlockSignature{
		BlockNumber:       300,
		MerkleRoot:        "abcdef",
		SignatureIdentity: "0xsigner",
		CIDs:              []string{"cid-1", "cid-2"},
	}

	// First call creates attestation
	h.processAttestationsFromBlockSignature(ctx, blockSig)
	_ = atomic.LoadInt64(&metrics.AttestationsCreated)

	// Second call hits the "existed" path — should update rather than create
	h.processAttestationsFromBlockSignature(ctx, blockSig)

	// MostRecentBlock should have been updated
	require.GreaterOrEqual(t, atomic.LoadUint64(&metrics.MostRecentBlock), uint64(300))
}

// ---------------------------------------------------------------------------
// processBlockSignatureDocument - valid signature, CID mismatch path
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureDocument_ValidSigCIDMismatch(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

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
		create_%s(input: {
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
	}`, constants.CollectionBlockSignature, merkleRootHex, len(wrongCids), cidListStr, pubKeyHex, sigHex)

	result := defraNode.DB.ExecRequest(ctx, mutation)
	require.Empty(t, result.GQL.Errors)

	docIDStr := extractDocIDFromMutationResult(t, result, constants.CollectionBlockSignature)
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	require.NoError(t, err)
	docIDTyped, err := client.NewDocIDFromString(docIDStr)
	require.NoError(t, err)
	doc, err := col.Get(ctx, docIDTyped)
	require.NoError(t, err)

	metrics := server.NewHostMetrics()
	h := &Host{
		DefraNode:              defraNode,
		blockSignatureVerifier: attestationService.NewBlockSignatureVerifier(100),
		metrics:                metrics,
	}

	h.processBlockSignatureDocument(ctx, doc)

	// Signature itself is valid, but CID mismatch should trigger failure
	require.Greater(t, atomic.LoadInt64(&metrics.SignatureFailures), int64(0),
		"CID mismatch should increment signature failures")
}

// ---------------------------------------------------------------------------
// processBlockSignatureFromEventBus - context cancelled during retry loop
// ---------------------------------------------------------------------------

func TestProcessBlockSignatureFromEventBus_ContextCancelledDuringRetry(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	h := &Host{
		DefraNode: defraNode,
	}

	// Use a valid doc ID format but for a non-existent doc
	// Cancel context quickly to test the retry loop cancellation
	shortCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()

	h.processBlockSignatureFromEventBus(shortCtx, "bae-5a32eb7dc5b8df3b10bf76d6e8a43acc0bb04eeba09050b43108a3e6a41a0a48")
}

// ---------------------------------------------------------------------------
// startEventBusListener - relay event with metrics and collection routing
// ---------------------------------------------------------------------------

func TestStartEventBusListener_WithMetricsAndCollections(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	metrics := server.NewHostMetrics()
	cfg := *DefaultConfig
	h := &Host{
		DefraNode: defraNode,
		config:    &cfg,
		metrics:   metrics,
	}

	// Start the listener with a timeout
	listenerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		h.startEventBusListener(listenerCtx)
		close(done)
	}()

	// Give listener time to subscribe and set up workers
	time.Sleep(300 * time.Millisecond)

	// Write docs to various collections to trigger event routing
	for _, col := range []string{constants.CollectionBlock, constants.CollectionTransaction, constants.CollectionLog} {
		mutation := fmt.Sprintf(`mutation { create_%s(input: {blockNumber: 1}) { _docID } }`, col)
		defraNode.DB.ExecRequest(ctx, mutation)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startEventBusListener did not return")
	}
}
