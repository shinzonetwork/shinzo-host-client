package host

import (
	"context"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
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

	// Metrics for monitoring
	queuedCount    int64
	processedCount int64
	droppedCount   int64
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
	logger.Sugar.Infof("🚀 Starting processing pipeline with %d workers and queue size %d", pp.workerCount, cap(pp.jobQueue))

	for i := 0; i < pp.workerCount; i++ {
		pp.wg.Add(1)
		go pp.worker(i)
	}

	logger.Sugar.Info("✅ Processing pipeline ready")
}

// Stop stops the processing pipeline.
func (pp *ProcessingPipeline) Stop() {
	logger.Sugar.Info("🛑 Stopping processing pipeline")
	pp.cancel()

	close(pp.jobQueue)

	pp.wg.Wait()

	logger.Sugar.Infof("✅ Processing pipeline stopped (processed: %d, dropped: %d)", pp.processedCount, pp.droppedCount)
}

// worker processes jobs from the queue.
func (pp *ProcessingPipeline) worker(workerID int) {
	defer pp.wg.Done()

	// logger.Sugar.Debugf("🔧 Worker %d started", workerID)

	for {
		select {
		case <-pp.ctx.Done():
			logger.Sugar.Debugf("🛑 Worker %d stopped (context cancelled)", workerID)
			return

		case job, ok := <-pp.jobQueue:
			if !ok {
				logger.Sugar.Debugf("🛑 Worker %d stopped (queue closed)", workerID)
				return
			}
			pp.processJob(workerID, job)
		}
	}
}

// processJob processes a single document job.
func (pp *ProcessingPipeline) processJob(workerID int, job DocumentJob) {
	// logger.Sugar.Debugf("👷 Worker %d processing %s document %s", workerID, job.docType, job.docID)

	err := pp.host.processDocumentAttestation(pp.ctx, job.docID, job.docType, job.blockNumber, job.docData)
	if err != nil {
		logger.Sugar.Errorf("❌ Worker %d failed: %v", workerID, err)
	}
	// else {
	// 	logger.Sugar.Debugf("✅ Worker %d completed %s document %s", workerID, job.docType, job.docID)
	// }

	pp.mu.Lock()
	pp.processedCount++
	pp.mu.Unlock()
}

// processDocumentDirect enqueues a document for processing (blocks when queue is full).
func (pp *ProcessingPipeline) processDocumentDirect(docID, docType string, blockNumber uint64, docData map[string]interface{}) {
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
