package host

import (
	"context"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
)

// ProcessingPipeline coordinates direct document processing (no cache needed)
type ProcessingPipeline struct {
	host   *Host
	ctx    context.Context
	cancel context.CancelFunc
}

// NewProcessingPipeline creates a simplified processing pipeline
func NewProcessingPipeline(ctx context.Context, cacheSize int, cacheMaxAge time.Duration, queueSize int, bufferTimeout time.Duration, host *Host) *ProcessingPipeline {
	pipelineCtx, cancel := context.WithCancel(ctx)

	return &ProcessingPipeline{
		host:   host,
		ctx:    pipelineCtx,
		cancel: cancel,
	}
}

// Start starts the simplified processing pipeline
func (pp *ProcessingPipeline) Start() {
	logger.Sugar.Info("🚀 Starting direct processing pipeline (no cache)")
	// No background processes needed - direct processing from subscriptions
	logger.Sugar.Info("✅ Processing pipeline ready")
}

// Stop stops the processing pipeline
func (pp *ProcessingPipeline) Stop() {
	logger.Sugar.Info("🛑 Stopping processing pipeline")
	pp.cancel()

	logger.Sugar.Info("✅ Processing pipeline stopped")
}

// processDocumentDirect processes a document directly (simple approach)
func (pp *ProcessingPipeline) processDocumentDirect(docID, docType string, blockNumber uint64, docData map[string]interface{}) {
	workerID := int(blockNumber % 1000)
	logger.Sugar.Infof("👷 Worker %d processing %s document %s", workerID, docType, docID)

	// Process directly
	err := pp.host.processDocumentAttestation(pp.ctx, docID, docType, blockNumber, docData)
	if err != nil {
		logger.Sugar.Errorf("❌ Worker %d failed: %v", workerID, err)
	} else {
		logger.Sugar.Debugf("✅ Worker %d completed %s document %s", workerID, docType, docID)
	}
}
