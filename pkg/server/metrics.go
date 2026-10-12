package server

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

//go:embed metrics_client_page.html
var embeddedMetricsClientPageHTML string

var metricsClientPagePath = filepath.Join("pkg", "server", "metrics_client_page.html") //nolint:gochecknoglobals

// HostMetrics tracks various metrics for the host.
type HostMetrics struct {
	// Attestation metrics
	AttestationsCreated    int64 `json:"attestations_created"`
	AttestationErrors      int64 `json:"attestation_errors"`
	SignatureVerifications int64 `json:"signature_verifications"`
	SignatureFailures      int64 `json:"signature_failures"`

	BlockSigEventsReceived int64 `json:"block_sig_events_received"`
	// Document processing metrics
	DocumentsReceived        int64 `json:"documents_received"`
	BlocksProcessed          int64 `json:"blocks_processed"`
	TransactionsProcessed    int64 `json:"transactions_processed"`
	LogsProcessed            int64 `json:"logs_processed"`
	AccessListsProcessed     int64 `json:"access_lists_processed"`
	BlockSignaturesProcessed int64 `json:"block_signatures_processed"`

	// View metrics
	ViewsRegistered int64 `json:"views_registered"`
	ViewsActive     int64 `json:"views_active"`

	// Internal fields for atomic float64 operations
	lastProcessingTimeBits atomic.Uint64 `json:"-"`

	// Performance metrics
	LastProcessingTime float64 `json:"last_processing_time_ms"`

	// System metrics
	StartTime        time.Time `json:"start_time"`
	LastDocumentTime time.Time `json:"last_document_time"`

	// Build configuration metrics
	BuildTags  string `json:"build_tags"`
	SchemaType string `json:"schema_type"`

	// chains holds each configured chain's counters, in config order.
	chains []*ChainMetrics
}

// ChainMetrics is one configured chain's counters: its relayed documents, its attestation queue and
// the newest block it attested. Prefix, Enabled and Cutoff are set when it is built and only read
// after.
type ChainMetrics struct {
	Prefix            string
	Enabled           bool           // Whether the chain is served at this start
	Cutoff            *pruner.Cutoff // The chain's pruner cutoff, nil when the host does not prune
	DocumentsReceived atomic.Int64
	QueueDrops        atomic.Int64 // Block signatures dropped because the chain's attestation queue was full
	queuePeak         atomic.Int64
	currentBlock      atomic.Int64
}

// ChainStatus is one configured chain as the health and metrics endpoints report it. CurrentBlock is
// omitted until the chain attests a block, and Cutoff until its pruner sets one.
type ChainStatus struct {
	Chain        string `json:"chain"`
	Enabled      bool   `json:"enabled"`
	CurrentBlock int64  `json:"current_block,omitempty"`
	Cutoff       int64  `json:"cutoff,omitempty"`
}

// NewHostMetrics creates a new metrics instance reporting chains, in the order given.
func NewHostMetrics(chains ...*ChainMetrics) *HostMetrics {
	buildTags := "standard"
	schemaType := "non-branchable"

	return &HostMetrics{
		StartTime:  time.Now(),
		BuildTags:  buildTags,
		SchemaType: schemaType,
		chains:     chains,
	}
}

// Chain returns the counters of the chain with prefix, or nil when m has none for it.
func (m *HostMetrics) Chain(prefix string) *ChainMetrics {
	for _, c := range m.chains {
		if c.Prefix == prefix {
			return c
		}
	}
	return nil
}

// Chains returns every chain's counters, in config order.
func (m *HostMetrics) Chains() []*ChainMetrics {
	return m.chains
}

// ChainStatuses returns every chain's status, in config order.
func (m *HostMetrics) ChainStatuses() []ChainStatus {
	statuses := make([]ChainStatus, len(m.chains))
	for i, c := range m.chains {
		statuses[i] = ChainStatus{Chain: c.Prefix, Enabled: c.Enabled, CurrentBlock: c.CurrentBlock()}
		if c.Cutoff != nil {
			statuses[i].Cutoff = c.Cutoff.Load()
		}
	}
	return statuses
}

// soleCurrentBlock returns the block the endpoints' top-level height fields report: the chain's
// current block on a host configured with one chain. With several chains there is none, and ok is
// false.
func soleCurrentBlock(chains []ChainStatus) (block int64, ok bool) {
	if len(chains) != 1 {
		return 0, false
	}
	return chains[0].CurrentBlock, true
}

// UpdateCurrentBlock records blockNumber as the chain's current block if it is newer than the one
// recorded.
func (c *ChainMetrics) UpdateCurrentBlock(blockNumber int64) {
	for {
		current := c.currentBlock.Load()
		if blockNumber <= current || c.currentBlock.CompareAndSwap(current, blockNumber) {
			return
		}
	}
}

// CurrentBlock returns the newest block the chain attested since the host started, or 0 before the
// first.
func (c *ChainMetrics) CurrentBlock() int64 {
	return c.currentBlock.Load()
}

// ObserveQueueDepth records depth if it is the deepest the chain's queue has been since the last
// TakeQueuePeak.
func (c *ChainMetrics) ObserveQueueDepth(depth int) {
	d := int64(depth)
	for {
		current := c.queuePeak.Load()
		if d <= current || c.queuePeak.CompareAndSwap(current, d) {
			return
		}
	}
}

// TakeQueuePeak returns the deepest queue depth observed since the previous call and resets it.
func (c *ChainMetrics) TakeQueuePeak() int64 {
	return c.queuePeak.Swap(0)
}

// IncrementAttestationsCreated atomically increments the attestations created counter.
func (m *HostMetrics) IncrementAttestationsCreated() {
	atomic.AddInt64(&m.AttestationsCreated, 1)
}

// IncrementAttestationErrors atomically increments the attestation errors counter.
func (m *HostMetrics) IncrementAttestationErrors() {
	atomic.AddInt64(&m.AttestationErrors, 1)
}

// IncrementSignatureVerifications atomically increments the signature verifications counter.
func (m *HostMetrics) IncrementSignatureVerifications() {
	atomic.AddInt64(&m.SignatureVerifications, 1)
}

// IncrementSignatureFailures atomically increments the signature failures counter.
func (m *HostMetrics) IncrementSignatureFailures() {
	atomic.AddInt64(&m.SignatureFailures, 1)
}

// IncrementBlockSigEventsReceived atomically increments the block sig events received counter.
func (m *HostMetrics) IncrementBlockSigEventsReceived() {
	atomic.AddInt64(&m.BlockSigEventsReceived, 1)
}

// IncrementDocumentsReceived atomically increments the documents received counter.
func (m *HostMetrics) IncrementDocumentsReceived() {
	atomic.AddInt64(&m.DocumentsReceived, 1)
	m.LastDocumentTime = time.Now()
}

// IncrementBlocksProcessed atomically increments the blocks processed counter.
func (m *HostMetrics) IncrementBlocksProcessed() {
	atomic.AddInt64(&m.BlocksProcessed, 1)
}

// IncrementTransactionsProcessed atomically increments the transactions processed counter.
func (m *HostMetrics) IncrementTransactionsProcessed() {
	atomic.AddInt64(&m.TransactionsProcessed, 1)
}

// IncrementLogsProcessed atomically increments the logs processed counter.
func (m *HostMetrics) IncrementLogsProcessed() {
	atomic.AddInt64(&m.LogsProcessed, 1)
}

// IncrementAccessListsProcessed atomically increments the access lists processed counter.
func (m *HostMetrics) IncrementAccessListsProcessed() {
	atomic.AddInt64(&m.AccessListsProcessed, 1)
}

// IncrementBlockSignaturesProcessed atomically increments the block signatures processed counter.
func (m *HostMetrics) IncrementBlockSignaturesProcessed() {
	atomic.AddInt64(&m.BlockSignaturesProcessed, 1)
}

// IncrementViewsRegistered atomically increments the views registered counter.
func (m *HostMetrics) IncrementViewsRegistered() {
	atomic.AddInt64(&m.ViewsRegistered, 1)
}

// SetViewsActive sets the number of active views.
func (m *HostMetrics) SetViewsActive(count int64) {
	atomic.StoreInt64(&m.ViewsActive, count)
}

// UpdateLastProcessingTime updates the last processing time in milliseconds.
func (m *HostMetrics) UpdateLastProcessingTime(avgMs float64) {
	// For float64, we need to use atomic operations with bits
	bits := *(*uint64)(unsafe.Pointer(&avgMs)) //nolint:gosec // unsafe pointer used for float64 to uint64 bit conversion
	m.lastProcessingTimeBits.Store(bits)
}

// GetSnapshot returns a snapshot of the current metrics, loading atomic values safely.
func (m *HostMetrics) GetSnapshot() *HostMetrics {
	// Load float64 values from atomic storage
	lastProcessingTimeBits := m.lastProcessingTimeBits.Load()
	lastProcessingTime := *(*float64)(unsafe.Pointer(&lastProcessingTimeBits)) // nolint:gosec // unsafe pointer used for float64 to uint64 bit conversion

	return &HostMetrics{
		AttestationsCreated:      atomic.LoadInt64(&m.AttestationsCreated),
		AttestationErrors:        atomic.LoadInt64(&m.AttestationErrors),
		SignatureVerifications:   atomic.LoadInt64(&m.SignatureVerifications),
		SignatureFailures:        atomic.LoadInt64(&m.SignatureFailures),
		BlockSigEventsReceived:   atomic.LoadInt64(&m.BlockSigEventsReceived),
		DocumentsReceived:        atomic.LoadInt64(&m.DocumentsReceived),
		BlocksProcessed:          atomic.LoadInt64(&m.BlocksProcessed),
		TransactionsProcessed:    atomic.LoadInt64(&m.TransactionsProcessed),
		LogsProcessed:            atomic.LoadInt64(&m.LogsProcessed),
		AccessListsProcessed:     atomic.LoadInt64(&m.AccessListsProcessed),
		BlockSignaturesProcessed: atomic.LoadInt64(&m.BlockSignaturesProcessed),
		ViewsRegistered:          atomic.LoadInt64(&m.ViewsRegistered),
		ViewsActive:              atomic.LoadInt64(&m.ViewsActive),
		// Performance metrics
		LastProcessingTime: lastProcessingTime,
		StartTime:          m.StartTime,
		LastDocumentTime:   m.LastDocumentTime,
		BuildTags:          m.BuildTags,
		SchemaType:         m.SchemaType,
	}
}

// ServeHTTP implements http.Handler for the metrics endpoint.
func (m *HostMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Content negotiation: Default to HTML for browsers, only serve JSON if explicitly requested
	accept := r.Header.Get("Accept")
	acceptLower := strings.ToLower(accept)

	// Serve JSON only if explicitly requested (Accept contains application/json and not text/html)
	// Otherwise, default to HTML for browser requests
	if strings.Contains(acceptLower, "text/html") && !strings.Contains(acceptLower, "application/json") {
		// Default to HTML (browser request or Accept header includes text/html)
		htmlContent := m.getMetricsClientPageHTML()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(htmlContent)
		return
	}

	// Serve JSON response
	w.Header().Set("Content-Type", "application/json")

	snapshot := m.GetSnapshot()

	// Add computed metrics
	uptime := time.Since(snapshot.StartTime)

	// Read once so the top-level heights match the chains list.
	chains := m.ChainStatuses()
	metrics := struct {
		*HostMetrics

		MostRecentBlock *int64 `json:"most_recent_block,omitempty"`
	}{HostMetrics: snapshot}
	response := map[string]any{
		"metrics":        &metrics,
		"chains":         chains,
		"timestamp":      time.Now().Unix(),
		"uptime_human":   uptime.String(),
		"uptime_seconds": uptime.Seconds(),
	}
	if block, ok := soleCurrentBlock(chains); ok {
		metrics.MostRecentBlock = &block
		response["current_block"] = block
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}

// getMetricsClientPageHTML reads the HTML file from disk at runtime, falling back to embedded version
// This allows hot-reloading during development without rebuilding.
func (m *HostMetrics) getMetricsClientPageHTML() []byte {
	// Try to read from disk first (for development hot-reload)
	// Check multiple possible paths relative to where the binary might be running
	possiblePaths := []string{
		metricsClientPagePath,                          // pkg/server/metrics_client_page.html
		filepath.Join(".", "metrics_client_page.html"), // ./metrics_client_pages.html (if running from pkg/server)
	}

	for _, path := range possiblePaths {
		if data, err := os.ReadFile(filepath.Clean(path)); err == nil {
			logger.Sugar.Debugf("Loaded metrics client page from: %s", path)
			return data
		}
	}

	// Fallback to embedded version (for production or if file not found)
	logger.Sugar.Debug("Using embedded metrics client page")
	return []byte(embeddedMetricsClientPageHTML)
}
