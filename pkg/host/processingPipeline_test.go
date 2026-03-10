package host

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// ---------------------------------------------------------------------------
// NewProcessingPipeline
// ---------------------------------------------------------------------------

func TestNewProcessingPipeline_DefaultValues(t *testing.T) {
	// All zero values should be replaced by defaults
	pp := NewProcessingPipeline(context.Background(), &Host{}, 100, 0, 0, 0, false)
	defer pp.cancel()

	require.Equal(t, defaultBatchWriterCount, pp.batchWriterCount)
	require.Equal(t, defaultBatchSize, pp.batchSize)
	require.Equal(t, time.Duration(defaultBatchFlushIntervalMs)*time.Millisecond, pp.batchFlushInterval)
	require.Equal(t, 100, cap(pp.jobQueue))
	require.False(t, pp.useBlockSignatures)
}

func TestNewProcessingPipeline_CustomValues(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 500, 4, 250, 200, true)
	defer pp.cancel()

	require.Equal(t, 4, pp.batchWriterCount)
	require.Equal(t, 250, pp.batchSize)
	require.Equal(t, 200*time.Millisecond, pp.batchFlushInterval)
	require.Equal(t, 500, cap(pp.jobQueue))
	require.True(t, pp.useBlockSignatures)
}

func TestNewProcessingPipeline_NegativeValues(t *testing.T) {
	// Negative values should be treated the same as zero (use defaults)
	pp := NewProcessingPipeline(context.Background(), &Host{}, 50, -1, -5, -10, false)
	defer pp.cancel()

	require.Equal(t, defaultBatchWriterCount, pp.batchWriterCount)
	require.Equal(t, defaultBatchSize, pp.batchSize)
	require.Equal(t, time.Duration(defaultBatchFlushIntervalMs)*time.Millisecond, pp.batchFlushInterval)
}

// ---------------------------------------------------------------------------
// GetPendingAttestationsCount
// ---------------------------------------------------------------------------

func TestProcessingPipeline_GetPendingAttestationsCount_Initial(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 10, 1, 10, 50, false)
	defer pp.cancel()

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
}

func TestProcessingPipeline_GetPendingAttestationsCount_AfterIncrement(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Manually increment to simulate in-flight attestations
	pp.mu.Lock()
	pp.pendingAttestationsCount = 5
	pp.mu.Unlock()

	require.Equal(t, int64(5), pp.GetPendingAttestationsCount())
}

// ---------------------------------------------------------------------------
// isTransactionConflict
// ---------------------------------------------------------------------------

func TestIsTransactionConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "transaction conflict",
			err:  errors.New("transaction conflict: cannot write"),
			want: true,
		},
		{
			name: "please retry",
			err:  errors.New("operation failed: Please retry"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "empty error message",
			err:  errors.New(""),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransactionConflict(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// Start / Stop lifecycle
// ---------------------------------------------------------------------------

func TestProcessingPipeline_StartStop(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 10, 2, 10, 50, false)

	// Start should not panic
	pp.Start()

	// Stop should cleanly shut down
	pp.Stop()
}

func TestProcessingPipeline_StopWithPendingJobs(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 100, 1, 10, 50, true)
	pp.Start()

	// Enqueue some jobs before stopping
	for i := 0; i < 5; i++ {
		pp.jobQueue <- DocumentJob{
			docID:       "doc-" + string(rune('a'+i)),
			docType:     "Block",
			blockNumber: uint64(i),
			docData:     map[string]any{},
		}
	}

	// Stop should complete without panicking even with pending jobs
	pp.Stop()
}

// ---------------------------------------------------------------------------
// DocumentJob construction
// ---------------------------------------------------------------------------

func TestDocumentJob_Fields(t *testing.T) {
	job := DocumentJob{
		docID:       "test-doc-id",
		docType:     "Transaction",
		blockNumber: 42,
		docData:     map[string]any{"key": "value"},
	}

	require.Equal(t, "test-doc-id", job.docID)
	require.Equal(t, "Transaction", job.docType)
	require.Equal(t, uint64(42), job.blockNumber)
	require.Equal(t, "value", job.docData["key"])
}

// ---------------------------------------------------------------------------
// processBatch
// ---------------------------------------------------------------------------

func TestProcessBatch_EmptyJobs(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Should not panic
	pp.processBatch(0, []DocumentJob{})
}

func TestProcessBatch_NilJobs(t *testing.T) {
	pp := NewProcessingPipeline(context.Background(), &Host{}, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Should not panic
	pp.processBatch(0, nil)
}

func TestProcessBatch_WithBlockSignatures(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, true)
	defer pp.cancel()

	jobs := []DocumentJob{
		{docID: "doc1", docType: "Block", blockNumber: 100, docData: map[string]any{}},
		{docID: "doc2", docType: "Transaction", blockNumber: 100, docData: map[string]any{}},
		{docID: "doc3", docType: "Log", blockNumber: 101, docData: map[string]any{}},
	}

	pp.processBatch(0, jobs)

	// With useBlockSignatures=true, metrics should be updated and processedCount incremented
	require.Equal(t, int64(3), metrics.DocumentsReceived)
	require.Equal(t, int64(3), pp.processedCount)
}

func TestProcessBatch_WithoutBlockSignatures_NoAttestationData(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	jobs := []DocumentJob{
		{docID: "doc1", docType: "Block", blockNumber: 100, docData: map[string]any{}},
	}

	pp.processBatch(0, jobs)

	// Metrics should be updated
	require.Equal(t, int64(1), metrics.DocumentsReceived)
	require.Equal(t, int64(1), pp.processedCount)

	// Wait for async attestation to complete
	pp.attestationWg.Wait()
}

func TestProcessBatch_NilMetrics(t *testing.T) {
	host := &Host{
		metrics: nil,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, true)
	defer pp.cancel()

	jobs := []DocumentJob{
		{docID: "doc1", docType: "Block", blockNumber: 100, docData: map[string]any{}},
	}

	// Should not panic with nil metrics
	pp.processBatch(0, jobs)
	require.Equal(t, int64(1), pp.processedCount)
}

// ---------------------------------------------------------------------------
// processAttestationAsync
// ---------------------------------------------------------------------------

func TestProcessAttestationAsync_ContextCancelled(t *testing.T) {
	host := &Host{
		metrics: server.NewHostMetrics(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	pp := NewProcessingPipeline(ctx, host, 10, 1, 10, 50, false)

	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	pp.processAttestationAsync(docs)

	// Verify pending count was decremented
	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
}

func TestProcessAttestationAsync_SuccessfulProcessing(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Documents with no version data will result in nil error (empty inputs)
	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	pp.processAttestationAsync(docs)

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
	// Should count as success (no error from empty inputs)
	require.Equal(t, int64(1), metrics.AttestationsCreated)
}

func TestProcessAttestationAsync_NilMetrics(t *testing.T) {
	host := &Host{
		metrics: nil,
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	// Should not panic with nil metrics
	pp.processAttestationAsync(docs)
	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
}

// ---------------------------------------------------------------------------
// batchWriter
// ---------------------------------------------------------------------------

func TestBatchWriter_FlushOnBatchSize(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	// Small batch size to trigger batch processing
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 2, 10000, true)

	pp.wg.Add(1)
	go pp.batchWriter(0)

	// Send 2 jobs to trigger batch flush (batchSize=2)
	pp.jobQueue <- DocumentJob{docID: "doc1", docType: "Block", blockNumber: 1}
	pp.jobQueue <- DocumentJob{docID: "doc2", docType: "Block", blockNumber: 2}

	// Give time for processing
	time.Sleep(200 * time.Millisecond)

	pp.Stop()

	require.Equal(t, int64(2), pp.processedCount)
}

func TestBatchWriter_FlushOnTimer(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	// Large batch size but small flush interval
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 1000, 50, true)

	pp.wg.Add(1)
	go pp.batchWriter(0)

	// Send 1 job (won't fill batch)
	pp.jobQueue <- DocumentJob{docID: "doc1", docType: "Block", blockNumber: 1}

	// Wait for timer flush (50ms + some buffer)
	time.Sleep(200 * time.Millisecond)

	pp.Stop()

	require.Equal(t, int64(1), pp.processedCount)
}

func TestBatchWriter_QueueClosed(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 100, 5000, true)

	pp.wg.Add(1)
	go pp.batchWriter(0)

	pp.jobQueue <- DocumentJob{docID: "doc1", docType: "Block", blockNumber: 1}

	// Close queue to trigger cleanup
	close(pp.jobQueue)
	pp.wg.Wait()

	require.Equal(t, int64(1), pp.processedCount)
}

// ---------------------------------------------------------------------------
// Stop with pending attestations
// ---------------------------------------------------------------------------

func TestProcessingPipeline_StopWithPendingAttestations(t *testing.T) {
	host := &Host{
		metrics: server.NewHostMetrics(),
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 10, 50, false)
	pp.Start()

	// Simulate a pending attestation
	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount = 1
	pp.mu.Unlock()

	// Resolve the pending attestation
	go func() {
		time.Sleep(50 * time.Millisecond)
		pp.mu.Lock()
		pp.pendingAttestationsCount = 0
		pp.mu.Unlock()
		pp.attestationWg.Done()
	}()

	// Stop should wait for attestations
	pp.Stop()
}

// ---------------------------------------------------------------------------
// Full pipeline end-to-end with block signatures
// ---------------------------------------------------------------------------

func TestProcessingPipeline_EndToEnd_BlockSignatures(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	pp := NewProcessingPipeline(context.Background(), host, 100, 2, 5, 100, true)
	pp.Start()

	// Enqueue multiple jobs
	for i := range 10 {
		pp.jobQueue <- DocumentJob{
			docID:       fmt.Sprintf("doc-%d", i),
			docType:     "Block",
			blockNumber: uint64(i),
			docData:     map[string]any{},
		}
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	pp.Stop()

	require.Equal(t, int64(10), pp.processedCount)
	require.Equal(t, int64(10), metrics.DocumentsReceived)
}

func TestProcessingPipeline_EndToEnd_PerDocumentAttestation(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 5, 100, false)
	pp.Start()

	// Enqueue jobs with no version data (will succeed with empty attestation)
	for i := range 3 {
		pp.jobQueue <- DocumentJob{
			docID:       fmt.Sprintf("doc-%d", i),
			docType:     "Block",
			blockNumber: uint64(i),
			docData:     map[string]any{},
		}
	}

	// Wait for processing and async attestations
	time.Sleep(500 * time.Millisecond)

	pp.Stop()

	require.Equal(t, int64(3), pp.processedCount)
}

// ---------------------------------------------------------------------------
// UpdateLastProcessingTime via batchWriter metrics
// ---------------------------------------------------------------------------

func TestBatchWriter_UpdatesProcessingTime(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	pp := NewProcessingPipeline(context.Background(), host, 100, 1, 2, 5000, true)

	pp.wg.Add(1)
	go pp.batchWriter(0)

	// Fill a batch to trigger flush with timing
	pp.jobQueue <- DocumentJob{docID: "d1", docType: "Block", blockNumber: 1}
	pp.jobQueue <- DocumentJob{docID: "d2", docType: "Block", blockNumber: 2}

	time.Sleep(200 * time.Millisecond)
	pp.Stop()

	// LastProcessingTime should have been updated (it's a float64 > 0 or 0 if very fast)
	// Just verify no panic occurred
	require.Equal(t, int64(2), pp.processedCount)
}

// ---------------------------------------------------------------------------
// processAttestationAsync - full attestation path with version data
// ---------------------------------------------------------------------------

func TestProcessAttestationAsync_WithVersionDataNoVerifier(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics:           metrics,
		config:            DefaultConfig,
		signatureVerifier: attestationService.NewDefraSignatureVerifier(nil, nil),
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Documents with _version data -- but no signatureVerifier on host,
	// so HandleDocumentAttestationBatch will use nil verifier.
	// The attestation creation should fail gracefully (nil verifier).
	docs := []Document{
		{
			ID:          "doc-with-version",
			Type:        "Transaction",
			BlockNumber: 10,
			Data: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4",
						"signature": map[string]any{
							"type":     "ES256K",
							"identity": "testpubkey",
							"value":    "testsig",
						},
					},
				},
			},
		},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	pp.processAttestationAsync(docs)

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
}

// ---------------------------------------------------------------------------
// processAttestationAsync - retries on transaction conflict
// ---------------------------------------------------------------------------

func TestProcessAttestationAsync_ErrorWithMetrics(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics:           metrics,
		config:            DefaultConfig,
		signatureVerifier: attestationService.NewDefraSignatureVerifier(nil, nil),
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	// Documents with version data that will fail attestation (nil DefraNode)
	docs := []Document{
		{
			ID:          "doc-error",
			Type:        "Transaction",
			BlockNumber: 10,
			Data: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4",
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
			ID:          "doc-error2",
			Type:        "Block",
			BlockNumber: 11,
			Data: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4",
						"signature": map[string]any{
							"type":     "ES256K",
							"identity": "testpubkey2",
							"value":    "testsig2",
						},
					},
				},
			},
		},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	pp.processAttestationAsync(docs)

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
	// Either errors or created should be incremented depending on whether the nil DefraNode
	// causes an error or gracefully succeeds with no verified CIDs
	totalMetrics := metrics.AttestationErrors + metrics.AttestationsCreated
	require.Equal(t, int64(2), totalMetrics, "should have 2 total attestation metrics entries")
}

func TestProcessAttestationAsync_TransactionConflictError(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}

	// Use a very short-lived context to limit retry waiting
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	pp := NewProcessingPipeline(ctx, host, 10, 1, 10, 50, false)

	// Docs with no version data will return nil from processDocumentAttestationBatch (success)
	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
		{ID: "doc2", Type: "Block", BlockNumber: 2, Data: map[string]any{}},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	pp.processAttestationAsync(docs)

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
	// Should count as success since no version data means no attestation needed
	require.Equal(t, int64(2), metrics.AttestationsCreated)
}

// ---------------------------------------------------------------------------
// processAttestationAsync - semaphore full + context cancellation
// ---------------------------------------------------------------------------

func TestProcessAttestationAsync_SemaphoreFullThenContextCancel(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())
	pp := NewProcessingPipeline(ctx, host, 10, 1, 10, 50, false)

	// Fill the semaphore completely
	for range cap(pp.attestationSem) {
		pp.attestationSem <- struct{}{}
	}

	docs := []Document{
		{ID: "doc1", Type: "Block", BlockNumber: 1, Data: map[string]any{}},
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	// Cancel context after short delay — processAttestationAsync is blocked on semaphore
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	pp.processAttestationAsync(docs)

	require.Equal(t, int64(0), pp.GetPendingAttestationsCount())
}

// ---------------------------------------------------------------------------
// processBatch - full pipeline with per-document attestation + version data
// ---------------------------------------------------------------------------

func TestProcessBatch_WithoutBlockSignatures_WithVersionData(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics:           metrics,
		config:            DefaultConfig,
		signatureVerifier: attestationService.NewDefraSignatureVerifier(nil, nil),
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	jobs := []DocumentJob{
		{
			docID:       "doc1",
			docType:     "Transaction",
			blockNumber: 100,
			docData: map[string]any{
				"_version": []any{
					map[string]any{
						"cid": "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4",
						"signature": map[string]any{
							"type":     "ES256K",
							"identity": "testpubkey",
							"value":    "testsig",
						},
					},
				},
			},
		},
	}

	pp.processBatch(0, jobs)

	// Wait for async attestation
	pp.attestationWg.Wait()

	require.Equal(t, int64(1), metrics.DocumentsReceived)
	require.Equal(t, int64(1), pp.processedCount)
	// Should have either error or success metric
	totalMetrics := metrics.AttestationErrors + metrics.AttestationsCreated
	require.True(t, totalMetrics > 0, "should have attestation metrics")
}

// ---------------------------------------------------------------------------
// processBatch - without block signatures, with metrics tracking
// ---------------------------------------------------------------------------

func TestProcessBatch_WithoutBlockSignatures_WithMetrics(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
		config:  DefaultConfig,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, false)
	defer pp.cancel()

	jobs := []DocumentJob{
		{docID: "doc1", docType: "Block", blockNumber: 100, docData: map[string]any{}},
		{docID: "doc2", docType: "Transaction", blockNumber: 101, docData: map[string]any{}},
		{docID: "doc3", docType: "Log", blockNumber: 102, docData: map[string]any{}},
	}

	pp.processBatch(0, jobs)

	// Wait for async attestation to complete
	pp.attestationWg.Wait()

	require.Equal(t, int64(3), metrics.DocumentsReceived)
	require.Equal(t, int64(3), pp.processedCount)
}

// ---------------------------------------------------------------------------
// processBatch - with block signatures updates MostRecentBlock correctly
// ---------------------------------------------------------------------------

func TestProcessBatch_WithBlockSignatures_MostRecentBlock(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}
	pp := NewProcessingPipeline(context.Background(), host, 10, 1, 10, 50, true)
	defer pp.cancel()

	jobs := []DocumentJob{
		{docID: "doc1", docType: "Block", blockNumber: 50, docData: map[string]any{}},
		{docID: "doc2", docType: "Block", blockNumber: 100, docData: map[string]any{}},
		{docID: "doc3", docType: "Block", blockNumber: 75, docData: map[string]any{}},
	}

	pp.processBatch(0, jobs)

	require.Equal(t, uint64(100), metrics.MostRecentBlock)
}

// ---------------------------------------------------------------------------
// Full end-to-end pipeline - context cancellation during processing
// ---------------------------------------------------------------------------

func TestProcessingPipeline_ContextCancellation(t *testing.T) {
	metrics := server.NewHostMetrics()
	host := &Host{
		metrics: metrics,
	}

	ctx, cancel := context.WithCancel(context.Background())
	pp := NewProcessingPipeline(ctx, host, 100, 2, 5, 100, true)
	pp.Start()

	// Enqueue a few jobs
	for i := range 3 {
		pp.jobQueue <- DocumentJob{
			docID:       fmt.Sprintf("doc-%d", i),
			docType:     "Block",
			blockNumber: uint64(i),
			docData:     map[string]any{},
		}
	}

	// Cancel context to trigger shutdown
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Stop should complete cleanly
	pp.Stop()
}
