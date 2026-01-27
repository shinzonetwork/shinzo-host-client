package server

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// HostMetrics tracks various metrics for the host
type HostMetrics struct {
	// Attestation metrics
	AttestationsCreated    int64 `json:"attestations_created"`
	AttestationErrors      int64 `json:"attestation_errors"`
	SignatureVerifications int64 `json:"signature_verifications"`
	SignatureFailures      int64 `json:"signature_failures"`

	// Document processing metrics
	DocumentsReceived     int64 `json:"documents_received"`
	BlocksProcessed       int64 `json:"blocks_processed"`
	TransactionsProcessed int64 `json:"transactions_processed"`
	LogsProcessed         int64 `json:"logs_processed"`
	AccessListsProcessed  int64 `json:"access_lists_processed"`

	// Unique document counters (tracks first-time attestations only)
	UniqueBlocks       int64 `json:"unique_blocks"`
	UniqueTransactions int64 `json:"unique_transactions"`
	UniqueLogs         int64 `json:"unique_logs"`
	UniqueAccessLists  int64 `json:"unique_access_lists"`

	// View metrics
	ViewsRegistered     int64 `json:"views_registered"`
	ViewsActive         int64 `json:"views_active"`
	ViewProcessingJobs  int64 `json:"view_processing_jobs"`
	ViewTransformations int64 `json:"view_transformations"`

	// Batch Processing Metrics (NEW)
	BatchJobsCreated       int64   `json:"batch_jobs_created"`
	BatchJobsCompleted     int64   `json:"batch_jobs_completed"`
	BatchViewsProcessed    int64   `json:"batch_views_processed"`
	BatchDuplicateDocs     int64   `json:"batch_duplicate_documents"`
	BatchQueueFullDrops    int64   `json:"batch_queue_full_drops"`
	BatchProcessingErrors  int64   `json:"batch_processing_errors"`
	BatchSuccessfulBatches int64   `json:"batch_successful_batches"`
	BatchAvgViewsPerJob    float64 `json:"batch_avg_views_per_job"`
	BatchSuccessRate       float64 `json:"batch_success_rate_percent"`

	// Internal fields for atomic float64 operations
	batchAvgViewsPerJobBits uint64 `json:"-"`
	batchSuccessRateBits    uint64 `json:"-"`
	avgProcessingTimeBits   uint64 `json:"-"`

	// Performance metrics
	ProcessingQueueSize   int64   `json:"processing_queue_size"`
	ViewQueueSize         int64   `json:"view_queue_size"`
	AverageProcessingTime float64 `json:"average_processing_time_ms"`

	// System metrics
	StartTime        time.Time `json:"start_time"`
	LastDocumentTime time.Time `json:"last_document_time"`
	MostRecentBlock  uint64    `json:"most_recent_block"`

	// Build configuration metrics
	BuildTags  string `json:"build_tags"`
	SchemaType string `json:"schema_type"`
}

// NewHostMetrics creates a new metrics instance
func NewHostMetrics() *HostMetrics {
	buildTags := "standard"
	schemaType := "non-branchable"

	if schema.IsBranchable() {
		buildTags = "branchable"
		schemaType = "branchable"
	}

	return &HostMetrics{
		StartTime:  time.Now(),
		BuildTags:  buildTags,
		SchemaType: schemaType,
	}
}

// IncrementAttestationsCreated atomically increments the attestations created counter
func (m *HostMetrics) IncrementAttestationsCreated() {
	atomic.AddInt64(&m.AttestationsCreated, 1)
}

// IncrementAttestationErrors atomically increments the attestation errors counter
func (m *HostMetrics) IncrementAttestationErrors() {
	atomic.AddInt64(&m.AttestationErrors, 1)
}

// IncrementSignatureVerifications atomically increments the signature verifications counter
func (m *HostMetrics) IncrementSignatureVerifications() {
	atomic.AddInt64(&m.SignatureVerifications, 1)
}

// IncrementSignatureFailures atomically increments the signature failures counter
func (m *HostMetrics) IncrementSignatureFailures() {
	atomic.AddInt64(&m.SignatureFailures, 1)
}

// IncrementDocumentsReceived atomically increments the documents received counter
func (m *HostMetrics) IncrementDocumentsReceived() {
	atomic.AddInt64(&m.DocumentsReceived, 1)
	m.LastDocumentTime = time.Now()
}

// IncrementDocumentByType atomically increments the counter for a specific document type
func (m *HostMetrics) IncrementDocumentByType(docType string) {
	switch docType {
	case constants.CollectionBlock:
		atomic.AddInt64(&m.BlocksProcessed, 1)
	case constants.CollectionTransaction:
		atomic.AddInt64(&m.TransactionsProcessed, 1)
	case constants.CollectionLog:
		atomic.AddInt64(&m.LogsProcessed, 1)
	case constants.CollectionAccessListEntry:
		atomic.AddInt64(&m.AccessListsProcessed, 1)
	}
}

// IncrementUniqueDocumentByType atomically increments the unique counter for a specific document type
func (m *HostMetrics) IncrementUniqueDocumentByType(docType string) {
	switch docType {
	case constants.CollectionBlock:
		atomic.AddInt64(&m.UniqueBlocks, 1)
	case constants.CollectionTransaction:
		atomic.AddInt64(&m.UniqueTransactions, 1)
	case constants.CollectionLog:
		atomic.AddInt64(&m.UniqueLogs, 1)
	case constants.CollectionAccessListEntry:
		atomic.AddInt64(&m.UniqueAccessLists, 1)
	}
}

// IncrementViewsRegistered atomically increments the views registered counter
func (m *HostMetrics) IncrementViewsRegistered() {
	atomic.AddInt64(&m.ViewsRegistered, 1)
}

// SetViewsActive sets the number of active views
func (m *HostMetrics) SetViewsActive(count int64) {
	atomic.StoreInt64(&m.ViewsActive, count)
}

// IncrementViewProcessingJobs atomically increments the view processing jobs counter
func (m *HostMetrics) IncrementViewProcessingJobs() {
	atomic.AddInt64(&m.ViewProcessingJobs, 1)
}

// IncrementViewTransformations atomically increments the view transformations counter
func (m *HostMetrics) IncrementViewTransformations() {
	atomic.AddInt64(&m.ViewTransformations, 1)
}

// Batch Processing Metrics Methods (NEW)

// IncrementBatchJobsCreated atomically increments the batch jobs created counter
func (m *HostMetrics) IncrementBatchJobsCreated() {
	atomic.AddInt64(&m.BatchJobsCreated, 1)
}

// IncrementBatchJobsCompleted atomically increments the batch jobs completed counter
func (m *HostMetrics) IncrementBatchJobsCompleted() {
	atomic.AddInt64(&m.BatchJobsCompleted, 1)
}

// IncrementBatchViewsProcessed atomically increments the batch views processed counter
func (m *HostMetrics) IncrementBatchViewsProcessed(count int64) {
	atomic.AddInt64(&m.BatchViewsProcessed, count)
}

// IncrementBatchDuplicateDocs atomically increments the batch duplicate documents counter
func (m *HostMetrics) IncrementBatchDuplicateDocs() {
	atomic.AddInt64(&m.BatchDuplicateDocs, 1)
}

// IncrementBatchQueueFullDrops atomically increments the batch queue full drops counter
func (m *HostMetrics) IncrementBatchQueueFullDrops() {
	atomic.AddInt64(&m.BatchQueueFullDrops, 1)
}

// IncrementBatchProcessingErrors atomically increments the batch processing errors counter
func (m *HostMetrics) IncrementBatchProcessingErrors() {
	atomic.AddInt64(&m.BatchProcessingErrors, 1)
}

// IncrementBatchSuccessfulBatches atomically increments the batch successful batches counter
func (m *HostMetrics) IncrementBatchSuccessfulBatches() {
	atomic.AddInt64(&m.BatchSuccessfulBatches, 1)
}

// UpdateBatchAvgViewsPerJob updates the average views per batch job
func (m *HostMetrics) UpdateBatchAvgViewsPerJob(avg float64) {
	bits := *(*uint64)(unsafe.Pointer(&avg))
	atomic.StoreUint64(&m.batchAvgViewsPerJobBits, bits)
}

// UpdateBatchSuccessRate updates the batch success rate percentage
func (m *HostMetrics) UpdateBatchSuccessRate(rate float64) {
	bits := *(*uint64)(unsafe.Pointer(&rate))
	atomic.StoreUint64(&m.batchSuccessRateBits, bits)
}

// SetProcessingQueueSize sets the current processing queue size
func (m *HostMetrics) SetProcessingQueueSize(size int64) {
	atomic.StoreInt64(&m.ProcessingQueueSize, size)
}

// UpdateAverageProcessingTime updates the average processing time in milliseconds
func (m *HostMetrics) UpdateAverageProcessingTime(avgMs float64) {
	// For float64, we need to use atomic operations with bits
	bits := *(*uint64)(unsafe.Pointer(&avgMs))
	atomic.StoreUint64(&m.avgProcessingTimeBits, bits)
}

// UpdateMostRecentBlock updates the most recent block number
func (m *HostMetrics) UpdateMostRecentBlock(blockNumber uint64) {
	for {
		current := atomic.LoadUint64(&m.MostRecentBlock)
		if blockNumber <= current {
			return
		}
		if atomic.CompareAndSwapUint64(&m.MostRecentBlock, current, blockNumber) {
			return
		}
	}
}

func (m *HostMetrics) GetSnapshot() *HostMetrics {
	// Load float64 values from atomic storage
	avgViewsPerJobBits := atomic.LoadUint64(&m.batchAvgViewsPerJobBits)
	successRateBits := atomic.LoadUint64(&m.batchSuccessRateBits)
	avgProcessingTimeBits := atomic.LoadUint64(&m.avgProcessingTimeBits)

	avgViewsPerJob := *(*float64)(unsafe.Pointer(&avgViewsPerJobBits))
	successRate := *(*float64)(unsafe.Pointer(&successRateBits))
	avgProcessingTime := *(*float64)(unsafe.Pointer(&avgProcessingTimeBits))

	return &HostMetrics{
		AttestationsCreated:    atomic.LoadInt64(&m.AttestationsCreated),
		AttestationErrors:      atomic.LoadInt64(&m.AttestationErrors),
		SignatureVerifications: atomic.LoadInt64(&m.SignatureVerifications),
		SignatureFailures:      atomic.LoadInt64(&m.SignatureFailures),
		DocumentsReceived:      atomic.LoadInt64(&m.DocumentsReceived),
		BlocksProcessed:        atomic.LoadInt64(&m.BlocksProcessed),
		TransactionsProcessed:  atomic.LoadInt64(&m.TransactionsProcessed),
		LogsProcessed:          atomic.LoadInt64(&m.LogsProcessed),
		AccessListsProcessed:   atomic.LoadInt64(&m.AccessListsProcessed),
		UniqueBlocks:           atomic.LoadInt64(&m.UniqueBlocks),
		UniqueTransactions:     atomic.LoadInt64(&m.UniqueTransactions),
		UniqueLogs:             atomic.LoadInt64(&m.UniqueLogs),
		UniqueAccessLists:      atomic.LoadInt64(&m.UniqueAccessLists),
		ViewsRegistered:        atomic.LoadInt64(&m.ViewsRegistered),
		ViewsActive:            atomic.LoadInt64(&m.ViewsActive),
		ViewProcessingJobs:     atomic.LoadInt64(&m.ViewProcessingJobs),
		ViewTransformations:    atomic.LoadInt64(&m.ViewTransformations),
		// Batch Processing Metrics
		BatchJobsCreated:       atomic.LoadInt64(&m.BatchJobsCreated),
		BatchJobsCompleted:     atomic.LoadInt64(&m.BatchJobsCompleted),
		BatchViewsProcessed:    atomic.LoadInt64(&m.BatchViewsProcessed),
		BatchDuplicateDocs:     atomic.LoadInt64(&m.BatchDuplicateDocs),
		BatchQueueFullDrops:    atomic.LoadInt64(&m.BatchQueueFullDrops),
		BatchProcessingErrors:  atomic.LoadInt64(&m.BatchProcessingErrors),
		BatchSuccessfulBatches: atomic.LoadInt64(&m.BatchSuccessfulBatches),
		BatchAvgViewsPerJob:    avgViewsPerJob,
		BatchSuccessRate:       successRate,
		// Performance metrics
		ProcessingQueueSize:   atomic.LoadInt64(&m.ProcessingQueueSize),
		ViewQueueSize:         atomic.LoadInt64(&m.ViewQueueSize),
		AverageProcessingTime: avgProcessingTime,
		StartTime:             m.StartTime,
		LastDocumentTime:      m.LastDocumentTime,
		MostRecentBlock:       atomic.LoadUint64(&m.MostRecentBlock),
		BuildTags:             m.BuildTags,
		SchemaType:            m.SchemaType,
	}
}

// ServeHTTP implements http.Handler for the metrics endpoint
func (m *HostMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	snapshot := m.GetSnapshot()

	// Add computed metrics
	uptime := time.Since(snapshot.StartTime)

	response := map[string]interface{}{
		"metrics":        snapshot,
		"current_block":  snapshot.MostRecentBlock,
		"timestamp":      time.Now().Unix(),
		"uptime_human":   uptime.String(),
		"uptime_seconds": uptime.Seconds(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}
