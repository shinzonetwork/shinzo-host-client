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
	DocumentsReceived  int64 `json:"documents_received"`
	DocumentsProcessed int64 `json:"documents_processed"`
	DocumentsDropped   int64 `json:"documents_dropped"`
	DocumentsSkipped   int64 `json:"documents_skipped"` // Skipped due to deduplication

	// Document type breakdown (counts attestation events, not unique documents)
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
	ViewsRegistered int64 `json:"views_registered"`
	ViewsActive     int64 `json:"views_active"`

	// Internal fields for atomic float64 operations
	lastProcessingTimeBits uint64 `json:"-"`

	// Performance metrics
	ProcessingQueueSize int64   `json:"processing_queue_size"`
	ViewQueueSize       int64   `json:"view_queue_size"`
	LastProcessingTime  float64 `json:"last_processing_time_ms"`

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

// IncrementDocumentsProcessed atomically increments the documents processed counter
func (m *HostMetrics) IncrementDocumentsProcessed() {
	atomic.AddInt64(&m.DocumentsProcessed, 1)
}

// IncrementDocumentsDropped atomically increments the documents dropped counter
func (m *HostMetrics) IncrementDocumentsDropped() {
	atomic.AddInt64(&m.DocumentsDropped, 1)
}

// IncrementDocumentsSkipped atomically increments the documents skipped counter
func (m *HostMetrics) IncrementDocumentsSkipped() {
	atomic.AddInt64(&m.DocumentsSkipped, 1)
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

// UpdateLastProcessingTime updates the last processing time in milliseconds
func (m *HostMetrics) UpdateLastProcessingTime(avgMs float64) {
	// For float64, we need to use atomic operations with bits
	bits := *(*uint64)(unsafe.Pointer(&avgMs))
	atomic.StoreUint64(&m.lastProcessingTimeBits, bits)
}

// UpdateMostRecentBlock updates the most recent block number
func (m *HostMetrics) UpdateMostRecentBlock(blockNumber uint64) {
	atomic.StoreUint64(&m.MostRecentBlock, blockNumber)
}

func (m *HostMetrics) GetSnapshot() *HostMetrics {
	// Load float64 values from atomic storage
	lastProcessingTimeBits := atomic.LoadUint64(&m.lastProcessingTimeBits)
	lastProcessingTime := *(*float64)(unsafe.Pointer(&lastProcessingTimeBits))

	return &HostMetrics{
		AttestationsCreated:    atomic.LoadInt64(&m.AttestationsCreated),
		AttestationErrors:      atomic.LoadInt64(&m.AttestationErrors),
		SignatureVerifications: atomic.LoadInt64(&m.SignatureVerifications),
		SignatureFailures:      atomic.LoadInt64(&m.SignatureFailures),
		DocumentsReceived:      atomic.LoadInt64(&m.DocumentsReceived),
		DocumentsProcessed:     atomic.LoadInt64(&m.DocumentsProcessed),
		DocumentsDropped:       atomic.LoadInt64(&m.DocumentsDropped),
		DocumentsSkipped:       atomic.LoadInt64(&m.DocumentsSkipped),
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
		// Performance metrics
		ProcessingQueueSize: atomic.LoadInt64(&m.ProcessingQueueSize),
		ViewQueueSize:       atomic.LoadInt64(&m.ViewQueueSize),
		LastProcessingTime:  lastProcessingTime,
		StartTime:           m.StartTime,
		LastDocumentTime:    m.LastDocumentTime,
		MostRecentBlock:     atomic.LoadUint64(&m.MostRecentBlock),
		BuildTags:           m.BuildTags,
		SchemaType:          m.SchemaType,
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
		"uptime_seconds": uptime.Seconds(),
		"uptime_human":   uptime.String(),
		"timestamp":      time.Now().Unix(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}
