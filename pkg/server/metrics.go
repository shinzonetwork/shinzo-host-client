package server

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	// View metrics
	ViewsRegistered int64 `json:"views_registered"`
	ViewsActive     int64 `json:"views_active"`

	// Internal fields for atomic float64 operations
	lastProcessingTimeBits atomic.Uint64 `json:"-"`

	// Performance metrics
	LastProcessingTime float64 `json:"last_processing_time_ms"`

	// System metrics
	StartTime time.Time `json:"start_time"`

	// Build configuration metrics
	BuildTags  string `json:"build_tags"`
	SchemaType string `json:"schema_type"`

	// chains holds each configured chain's counters, in config order, then the removed chains.
	chains []*ChainMetrics
}

// Reasons a chain in the endpoints' chains list is not enabled.
const (
	ReasonSchemaDrift         = "schema_drift"          // Its stored tables match no answering generator's schema
	ReasonNoGeneratorAnswered = "no_generator_answered" // The store holds none of its tables and no generator answered
	ReasonRemoved             = "removed"               // The store holds its tables but the config does not list it
)

// ChainMetrics is one chain's counters: its relayed documents, in total and per table, and when the
// last one arrived, its block signatures and attestation records, its attestation queue and the
// newest block it attested. Prefix, Enabled, Reason and Cutoff are set when it is built and only read
// after.
type ChainMetrics struct {
	Prefix                   string
	Enabled                  bool           // Whether the chain is served at this start
	Reason                   string         // Why the chain is not enabled, empty when it is
	Cutoff                   *pruner.Cutoff // The chain's pruner cutoff, nil when the host does not prune
	DocumentsReceived        atomic.Int64
	documentsByTable         sync.Map // Table name to *atomic.Int64, for each table a peer sent a document to
	BlockSignaturesProcessed atomic.Int64
	SignatureVerifications   atomic.Int64 // Block signatures that passed verification
	SignatureFailures        atomic.Int64 // Block signatures rejected for an invalid signature or a mismatched CID list
	AttestationsCreated      atomic.Int64
	AttestationErrors        atomic.Int64
	QueueDrops               atomic.Int64 // Block signatures dropped because the chain's attestation queue was full
	queuePeak                atomic.Int64
	currentBlock             atomic.Int64
	lastDocument             atomic.Int64 // Unix nanoseconds of the last document a peer sent, 0 before the first
}

// ChainCounters is a reading of one chain's counters, as /metrics reports them. DocumentsByTable is
// keyed by table name without the chain's prefix, and holds only tables a peer sent a document to.
type ChainCounters struct {
	DocumentsReceived        int64            `json:"documents_received"`
	DocumentsByTable         map[string]int64 `json:"documents_by_table"`
	BlockSignaturesProcessed int64            `json:"block_signatures_processed"`
	SignatureVerifications   int64            `json:"signature_verifications"`
	SignatureFailures        int64            `json:"signature_failures"`
	AttestationsCreated      int64            `json:"attestations_created"`
	AttestationErrors        int64            `json:"attestation_errors"`
}

// chainReport is one chain's entry in /metrics.
type chainReport struct {
	ChainStatus
	ChainCounters
}

// chainReadyWindow is how long an enabled chain stays ready after its last document, or after the
// host's start before its first.
const chainReadyWindow = 5 * time.Minute

// ChainStatus is one chain as the health and metrics endpoints report it. A chain is Ready while it
// is enabled and its last document, or the host's start before its first, is within
// chainReadyWindow. CurrentBlock is omitted until the chain attests a block, LastDocumentTime until
// its first document, Cutoff until its pruner sets one, and Reason while the chain is enabled.
type ChainStatus struct {
	Chain            string    `json:"chain"`
	Enabled          bool      `json:"enabled"`
	Ready            bool      `json:"ready"`
	Reason           string    `json:"reason,omitempty"`
	CurrentBlock     int64     `json:"current_block,omitempty"`
	LastDocumentTime time.Time `json:"last_document_time,omitzero"`
	Cutoff           int64     `json:"cutoff,omitempty"`
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

// Chains returns each configured chain's counters, in config order.
func (m *HostMetrics) Chains() []*ChainMetrics {
	return slices.DeleteFunc(slices.Clone(m.chains), func(c *ChainMetrics) bool {
		return c.Reason == ReasonRemoved
	})
}

// ChainStatuses returns each configured chain's status, in config order, then each removed chain's.
func (m *HostMetrics) ChainStatuses() []ChainStatus {
	now := time.Now()
	statuses := make([]ChainStatus, len(m.chains))
	for i, c := range m.chains {
		statuses[i] = c.status(now, m.StartTime)
	}
	return statuses
}

// status returns the chain's status at now, on a host that started at start.
func (c *ChainMetrics) status(now, start time.Time) ChainStatus {
	last := c.LastDocumentTime()
	since := last
	if since.IsZero() {
		since = start
	}
	status := ChainStatus{
		Chain:            c.Prefix,
		Enabled:          c.Enabled,
		Ready:            c.Enabled && now.Sub(since) <= chainReadyWindow,
		Reason:           c.Reason,
		CurrentBlock:     c.CurrentBlock(),
		LastDocumentTime: last,
	}
	if c.Cutoff != nil {
		status.Cutoff = c.Cutoff.Load()
	}
	return status
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

// RecordDocument counts a document a peer sent to the chain's table, named without the chain's
// prefix, and records when it arrived.
func (c *ChainMetrics) RecordDocument(table string) {
	c.DocumentsReceived.Add(1)
	// LoadOrStore alone would build a new counter for every document; Load finds the table's counter
	// once it exists.
	n, ok := c.documentsByTable.Load(table)
	if !ok {
		n, _ = c.documentsByTable.LoadOrStore(table, new(atomic.Int64))
	}
	n.(*atomic.Int64).Add(1)
	c.lastDocument.Store(time.Now().UnixNano())
}

// LastDocumentTime returns when a peer last sent the chain a document, or the zero time before the
// first.
func (c *ChainMetrics) LastDocumentTime() time.Time {
	ns := c.lastDocument.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Counters reads the chain's counters.
func (c *ChainMetrics) Counters() ChainCounters {
	byTable := make(map[string]int64)
	c.documentsByTable.Range(func(table, n any) bool {
		byTable[table.(string)] = n.(*atomic.Int64).Load()
		return true
	})
	return ChainCounters{
		DocumentsReceived:        c.DocumentsReceived.Load(),
		DocumentsByTable:         byTable,
		BlockSignaturesProcessed: c.BlockSignaturesProcessed.Load(),
		SignatureVerifications:   c.SignatureVerifications.Load(),
		SignatureFailures:        c.SignatureFailures.Load(),
		AttestationsCreated:      c.AttestationsCreated.Load(),
		AttestationErrors:        c.AttestationErrors.Load(),
	}
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
		ViewsRegistered: atomic.LoadInt64(&m.ViewsRegistered),
		ViewsActive:     atomic.LoadInt64(&m.ViewsActive),
		// Performance metrics
		LastProcessingTime: lastProcessingTime,
		StartTime:          m.StartTime,
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

	now := time.Now()
	chains := make([]chainReport, len(m.chains))
	for i, c := range m.chains {
		chains[i] = chainReport{ChainStatus: c.status(now, m.StartTime), ChainCounters: c.Counters()}
	}
	response := map[string]any{
		"metrics":        snapshot,
		"chains":         chains,
		"timestamp":      time.Now().Unix(),
		"uptime_human":   uptime.String(),
		"uptime_seconds": uptime.Seconds(),
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
