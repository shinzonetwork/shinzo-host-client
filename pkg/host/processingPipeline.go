package host

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
)

const (
	defaultBatchWriterCount          = 50
	defaultBatchSize                 = 500
	defaultBatchFlushIntervalMs      = 50
	defaultMaxConcurrentAttestations = 200

	maxRetries            = 10
	baseDelay             = 10 * time.Millisecond
	maxDelay              = 2 * time.Second
	dedupeExpiry          = 5 * time.Minute
	dedupeCleanupInterval = 1 * time.Minute
)

// DocumentJob represents a document to be processed.
type DocumentJob struct {
	docID       string
	docType     string
	blockNumber uint64
	docData     map[string]any
}

// ProcessingPipeline coordinates document processing with bounded workers and queue.
type ProcessingPipeline struct {
	host   *Host
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	jobQueue chan DocumentJob

	batchWriterCount   int
	batchSize          int
	batchFlushInterval time.Duration

	seenDocs sync.Map
	dedupeMu sync.Mutex

	// Async attestation control
	attestationSem chan struct{}
	attestationWg  sync.WaitGroup

	// Metrics for monitoring
	queuedCount              int64
	processedCount           int64
	droppedCount             int64
	skippedCount             int64
	pendingAttestationsCount int64
	mu                       sync.Mutex
}

// NewProcessingPipeline creates a processing pipeline with bounded workers and queue.
func NewProcessingPipeline(
	ctx context.Context,
	host *Host,
	queueSize int,
	batchWriterCount, batchSize, batchFlushIntervalMs int,
) *ProcessingPipeline {
	pipelineCtx, cancel := context.WithCancel(ctx)

	if batchWriterCount <= 0 {
		batchWriterCount = defaultBatchWriterCount
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchFlushIntervalMs <= 0 {
		batchFlushIntervalMs = defaultBatchFlushIntervalMs
	}

	return &ProcessingPipeline{
		host:               host,
		ctx:                pipelineCtx,
		cancel:             cancel,
		jobQueue:           make(chan DocumentJob, queueSize),
		batchWriterCount:   batchWriterCount,
		batchSize:          batchSize,
		batchFlushInterval: time.Duration(batchFlushIntervalMs) * time.Millisecond,
		attestationSem:     make(chan struct{}, defaultMaxConcurrentAttestations),
	}
}

// Start starts the processing pipeline with worker pool.
func (pp *ProcessingPipeline) Start() {
	logger.Sugar.Infof("🚀 Starting processing pipeline with %d batch writers (batch size: %d, flush interval: %v), queue size %d",
		pp.batchWriterCount, pp.batchSize, pp.batchFlushInterval, cap(pp.jobQueue))

	for i := range pp.batchWriterCount {
		pp.wg.Add(1)
		go pp.batchWriter(i)
	}

	go pp.cleanupDedupeCache()

	logger.Sugar.Info("✅ Processing pipeline ready")
}

// cleanupDedupeCache periodically removes expired entries from the deduplication cache.
func (pp *ProcessingPipeline) cleanupDedupeCache() {
	ticker := time.NewTicker(dedupeCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pp.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			expiredCount := 0
			pp.seenDocs.Range(func(key, value any) bool {
				if expiry, ok := value.(time.Time); ok {
					if now.After(expiry) {
						pp.seenDocs.Delete(key)
						expiredCount++
					}
				}
				return true
			})
		}
	}
}

// Stop stops the processing pipeline.
func (pp *ProcessingPipeline) Stop() {
	logger.Sugar.Info("🛑 Stopping processing pipeline")
	pp.cancel()

	close(pp.jobQueue)

	// Wait for batch writers to finish
	pp.wg.Wait()

	// Wait for any in-flight attestation goroutines to complete
	pp.mu.Lock()
	pending := pp.pendingAttestationsCount
	pp.mu.Unlock()
	if pending > 0 {
		logger.Sugar.Infof("⏳ Waiting for %d pending attestation goroutines to complete...", pending)
	}
	pp.attestationWg.Wait()

	logger.Sugar.Infof("✅ Processing pipeline stopped (processed: %d, skipped: %d, dropped: %d)",
		pp.processedCount, pp.skippedCount, pp.droppedCount)
}

// batchWriter collects jobs into batches and processes them together to reduce DB transaction overhead.
func (pp *ProcessingPipeline) batchWriter(writerID int) {
	defer pp.wg.Done()

	batch := make([]DocumentJob, 0, pp.batchSize)
	flushTimer := time.NewTimer(pp.batchFlushInterval)
	defer flushTimer.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		startTime := time.Now()
		pp.processBatch(writerID, batch)
		processingTime := time.Since(startTime)

		if pp.host.metrics != nil {
			avgProcessingTimeMs := float64(processingTime.Nanoseconds()) / float64(len(batch)) / 1000000.0
			pp.host.metrics.UpdateAverageProcessingTime(avgProcessingTimeMs)
		}

		batch = batch[:0]
	}

	for {
		select {
		case job, ok := <-pp.jobQueue:
			if !ok {
				flushBatch()
				logger.Sugar.Debugf("BatchWriter %d stopped (queue closed)", writerID)
				return
			}

			batch = append(batch, job)

			if len(batch) >= pp.batchSize {
				flushBatch()
				flushTimer.Reset(pp.batchFlushInterval)
			}

		case <-flushTimer.C:
			flushBatch()
			flushTimer.Reset(pp.batchFlushInterval)

		case <-pp.ctx.Done():
			flushBatch()
			logger.Sugar.Debugf("BatchWriter %d stopped (context cancelled)", writerID)
			return
		}
	}
}

// processBatch processes a batch of documents andmarks them as received immediately.
// Then it spawns async goroutine for attestation creation.
func (pp *ProcessingPipeline) processBatch(_ int, jobs []DocumentJob) {
	if len(jobs) == 0 {
		return
	}

	if pp.host.metrics != nil {
		for _, job := range jobs {
			pp.host.metrics.IncrementDocumentsReceived()
			pp.host.metrics.IncrementDocumentsProcessed()
			pp.host.metrics.IncrementDocumentByType(job.docType)
			pp.host.metrics.UpdateMostRecentBlock(job.blockNumber)
		}
	}

	pp.mu.Lock()
	pp.processedCount += int64(len(jobs))
	pp.mu.Unlock()

	// Prepare documents for attestation
	docs := make([]Document, len(jobs))
	for i, job := range jobs {
		docs[i] = Document{
			ID:          job.docID,
			Type:        job.docType,
			BlockNumber: job.blockNumber,
			Data:        job.docData,
		}
	}

	pp.attestationWg.Add(1)
	pp.mu.Lock()
	pp.pendingAttestationsCount++
	pp.mu.Unlock()

	go pp.processAttestationAsync(docs)
}

// processAttestationAsync handles attestation creation in the background.
func (pp *ProcessingPipeline) processAttestationAsync(docs []Document) {
	defer pp.attestationWg.Done()
	defer func() {
		pp.mu.Lock()
		pp.pendingAttestationsCount--
		pp.mu.Unlock()
	}()

	select {
	case pp.attestationSem <- struct{}{}:
		defer func() { <-pp.attestationSem }()
	case <-pp.ctx.Done():
		return
	}

	var err error
	for attempt := range maxRetries {
		err = pp.host.processDocumentAttestationBatch(pp.ctx, docs)
		if err == nil {
			break
		}

		if !isTransactionConflict(err) {
			break
		}

		if pp.ctx.Err() != nil {
			break
		}

		delay := min(baseDelay*time.Duration(1<<attempt), maxDelay)
		jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()))
		if attempt < maxRetries-1 {
			select {
			case <-time.After(jitter):
			case <-pp.ctx.Done():
				return
			}
		}
	}

	if pp.host.metrics != nil {
		if err != nil {
			for range docs {
				pp.host.metrics.IncrementAttestationErrors()
			}
		} else {
			for range docs {
				pp.host.metrics.IncrementAttestationsCreated()
			}
		}
	}

	if err != nil && logger.Sugar != nil {
		logger.Sugar.Warnf("Async attestation failed for %d documents: %v", len(docs), err)
	}
}

// isTransactionConflict checks if the error is a transaction conflict that should be retried.
func isTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "transaction conflict") || strings.Contains(errStr, "Please retry")
}

// processDocumentDirect enqueues a document for processing.
func (pp *ProcessingPipeline) processDocumentDirect(docID, docType string, blockNumber uint64, docData map[string]any) {
	dedupeKey := docType + ":" + docID

	if _, exists := pp.seenDocs.Load(dedupeKey); exists {
		pp.mu.Lock()
		pp.skippedCount++
		pp.mu.Unlock()
		if pp.host.metrics != nil {
			pp.host.metrics.IncrementDocumentsSkipped()
		}
		return
	}

	pp.seenDocs.Store(dedupeKey, time.Now().Add(dedupeExpiry))

	if pp.host.metrics != nil {
		pp.host.metrics.IncrementUniqueDocumentByType(docType)
	}

	job := DocumentJob{
		docID:       docID,
		docType:     docType,
		blockNumber: blockNumber,
		docData:     docData,
	}

	select {
	case pp.jobQueue <- job:
		pp.mu.Lock()
		pp.queuedCount++
		pp.mu.Unlock()
	case <-pp.ctx.Done():
		return
	}
}

// GetPendingAttestationsCount returns the number of in-flight attestation goroutines.
func (pp *ProcessingPipeline) GetPendingAttestationsCount() int64 {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return pp.pendingAttestationsCount
}
