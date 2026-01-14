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
	attestationWriterCount = 400
	maxRetries             = 10
	baseDelay              = 10 * time.Millisecond
	maxDelay               = 2 * time.Second
	dedupeExpiry           = 5 * time.Minute
	dedupeCleanupInterval  = 1 * time.Minute
)

// DocumentJob represents a document to be processed.
type DocumentJob struct {
	docID       string
	docType     string
	blockNumber uint64
	docData     map[string]interface{}
}

// ProcessingPipeline coordinates document processing with bounded workers and queue.
type ProcessingPipeline struct {
	host        *Host
	ctx         context.Context
	cancel      context.CancelFunc
	jobQueue    chan DocumentJob
	workerCount int
	wg          sync.WaitGroup

	seenDocs sync.Map
	dedupeMu sync.Mutex

	// Metrics for monitoring
	queuedCount    int64
	processedCount int64
	droppedCount   int64
	skippedCount   int64
	mu             sync.Mutex
}

// NewProcessingPipeline creates a processing pipeline with bounded workers and queue.
func NewProcessingPipeline(
	ctx context.Context,
	host *Host,
	bufferTimeout time.Duration,
	cacheMaxAge time.Duration,
	cacheSize, queueSize, workerCount int,
) *ProcessingPipeline {
	pipelineCtx, cancel := context.WithCancel(ctx)

	return &ProcessingPipeline{
		host:        host,
		ctx:         pipelineCtx,
		cancel:      cancel,
		jobQueue:    make(chan DocumentJob, queueSize),
		workerCount: workerCount,
	}
}

// Start starts the processing pipeline with worker pool.
func (pp *ProcessingPipeline) Start() {
	logger.Sugar.Infof("🚀 Starting processing pipeline with %d workers, %d attestation writers, queue size %d",
		pp.workerCount, attestationWriterCount, cap(pp.jobQueue))

	for i := 0; i < attestationWriterCount; i++ {
		pp.wg.Add(1)
		go pp.jobWriter(i)
	}

	for i := 0; i < pp.workerCount; i++ {
		pp.wg.Add(1)
		go pp.worker(i)
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
			pp.seenDocs.Range(func(key, value interface{}) bool {
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

	pp.wg.Wait()

	logger.Sugar.Infof("✅ Processing pipeline stopped (processed: %d, skipped: %d, dropped: %d)",
		pp.processedCount, pp.skippedCount, pp.droppedCount)
}

// worker processes jobs from the queue and enqueues attestation writes.
func (pp *ProcessingPipeline) worker(workerID int) {
	defer pp.wg.Done()
	for {
		select {
		case <-pp.ctx.Done():
			logger.Sugar.Debugf("Worker %d stopped (context cancelled)", workerID)
			return
		case job, ok := <-pp.jobQueue:
			if !ok {
				logger.Sugar.Debugf("Worker %d stopped (queue closed)", workerID)
				return
			}
			select {
			case pp.jobQueue <- DocumentJob{
				docID:       job.docID,
				docType:     job.docType,
				blockNumber: job.blockNumber,
				docData:     job.docData,
			}:
			case <-pp.ctx.Done():
				return
			}
		}
	}
}

// attestationWriter processes attestation jobs with retry logic for transaction conflicts.
func (pp *ProcessingPipeline) jobWriter(writerID int) {
	defer pp.wg.Done()
	for {
		select {
		case job, ok := <-pp.jobQueue:
			if !ok {
				logger.Sugar.Debugf("Writer %d stopped (queue closed)", writerID)
				return
			}

			// Track processing time
			startTime := time.Now()
			pp.processAttestation(writerID, job)
			processingTime := time.Since(startTime)

			// Update average processing time metrics
			if pp.host.metrics != nil {
				avgProcessingTimeMs := float64(processingTime.Nanoseconds()) / 1000000.0 // Convert to milliseconds
				pp.host.metrics.UpdateAverageProcessingTime(avgProcessingTimeMs)
			}

		case <-pp.ctx.Done():
			logger.Sugar.Debugf("Writer %d stopped (context cancelled)", writerID)
			return
		}
	}
}

// processAttestation processes a single attestation job with retry logic for transaction conflicts.
func (pp *ProcessingPipeline) processAttestation(writerID int, job DocumentJob) {
	if pp.host.metrics != nil {
		pp.host.metrics.IncrementDocumentsReceived()
	}

	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = pp.host.processDocumentAttestation(pp.ctx, job.docID, job.docType, job.blockNumber, job.docData)
		if err == nil {
			break
		}

		if !isTransactionConflict(err) {
			break
		}

		if pp.ctx.Err() != nil {
			break
		}

		delay := baseDelay * time.Duration(1<<attempt)
		if delay > maxDelay {
			delay = maxDelay
		}
		jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()))
		if attempt < maxRetries-1 {
			// logger.Sugar.Debugf("Writer %d: transaction conflict for %s %s, retrying in %v (attempt %d/%d)",
			// 	writerID, job.docType, job.docID, jitter, attempt+1, maxRetries)
			select {
			case <-time.After(jitter):
			case <-pp.ctx.Done():
				return
			}
		}
	}

	if pp.host.metrics != nil {
		if err != nil {
			pp.host.metrics.IncrementAttestationErrors()
		} else {
			pp.host.metrics.IncrementAttestationsCreated()
			pp.host.metrics.IncrementDocumentsProcessed()
			pp.host.metrics.IncrementDocumentByType(job.docType)
			pp.host.metrics.UpdateMostRecentBlock(job.blockNumber)
		}
	}

	if err != nil {
		if logger.Sugar != nil {
			logger.Sugar.Warnf("Attestation failed for %s %s: %v", job.docType, job.docID, err)
		}
	}

	pp.mu.Lock()
	pp.processedCount++
	pp.mu.Unlock()
}

// isTransactionConflict checks if the error is a transaction conflict that should be retried.
func isTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "transaction conflict") || strings.Contains(errStr, "Please retry")
}

// processDocumentDirect enqueues a document for processing (blocks when queue is full).
func (pp *ProcessingPipeline) processDocumentDirect(docID, docType string, blockNumber uint64, docData map[string]interface{}) {
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
