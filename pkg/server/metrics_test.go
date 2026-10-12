package server

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// ---------------------------------------------------------------------------
// NewHostMetrics
// ---------------------------------------------------------------------------

func TestNewHostMetrics(t *testing.T) {
	m := NewHostMetrics()
	require.NotNil(t, m)
	require.False(t, m.StartTime.IsZero(), "StartTime must be set")
	require.Equal(t, int64(0), m.AttestationsCreated)
	require.Equal(t, int64(0), m.AttestationErrors)
	require.Equal(t, int64(0), m.SignatureVerifications)
	require.Equal(t, int64(0), m.SignatureFailures)
	require.Equal(t, int64(0), m.DocumentsReceived)
	require.Equal(t, int64(0), m.BlocksProcessed)
	require.Equal(t, int64(0), m.TransactionsProcessed)
	require.Equal(t, int64(0), m.LogsProcessed)
	require.Equal(t, int64(0), m.AccessListsProcessed)
	require.Equal(t, int64(0), m.BlockSignaturesProcessed)
	require.Equal(t, int64(0), m.ViewsRegistered)
	require.Equal(t, int64(0), m.ViewsActive)
	// Without branchable build tag, defaults should be "standard" / "non-branchable"
	require.Equal(t, "standard", m.BuildTags)
	require.Equal(t, "non-branchable", m.SchemaType)
}

// A host's chain counters are found by prefix and listed in config order; a removed chain is not
// configured and is left out.
func TestHostMetricsChains(t *testing.T) {
	m := NewHostMetrics(&ChainMetrics{Prefix: "Ethereum__Mainnet"}, &ChainMetrics{Prefix: "BSC__Mainnet"}, &ChainMetrics{Prefix: "Old__Chain", Reason: ReasonRemoved})
	chains := m.Chains()
	require.Len(t, chains, 2)
	require.Equal(t, "Ethereum__Mainnet", chains[0].Prefix)
	require.Equal(t, "BSC__Mainnet", chains[1].Prefix)
	require.Same(t, chains[1], m.Chain("BSC__Mainnet"))
	require.Nil(t, m.Chain("Other__Chain"))
}

// A chain is ready when it is enabled and its last document, or the host's start before its first
// document, is within chainReadyWindow.
func TestChainStatusesReady(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * chainReadyWindow)
	tests := []struct {
		name         string
		enabled      bool
		start        time.Time
		lastDocument time.Time // zero for none
		want         bool
	}{
		{name: "recent document", enabled: true, start: stale, lastDocument: now, want: true},
		{name: "stale document", enabled: true, start: stale, lastDocument: stale, want: false},
		{name: "no document, host just started", enabled: true, start: now, want: true},
		{name: "no document, host started long ago", enabled: true, start: stale, want: false},
		{name: "disabled chain, host just started", enabled: false, start: now, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ChainMetrics{Prefix: "Ethereum__Mainnet", Enabled: tt.enabled}
			if !tt.lastDocument.IsZero() {
				c.lastDocument.Store(tt.lastDocument.UnixNano())
			}
			m := NewHostMetrics(c)
			m.StartTime = tt.start

			require.Equal(t, tt.want, m.ChainStatuses()[0].Ready)
		})
	}
}

// A document makes a chain that stopped receiving ready again.
func TestRecordDocument(t *testing.T) {
	stale := time.Now().Add(-2 * chainReadyWindow)
	c := &ChainMetrics{Prefix: "Ethereum__Mainnet", Enabled: true}
	c.lastDocument.Store(stale.UnixNano())
	m := NewHostMetrics(c)
	m.StartTime = stale
	require.False(t, m.ChainStatuses()[0].Ready)

	c.RecordDocument()
	require.True(t, m.ChainStatuses()[0].Ready)
}

// TakeQueuePeak returns the deepest depth observed since the previous call.
func TestChainMetricsQueuePeak(t *testing.T) {
	c := &ChainMetrics{Prefix: "Ethereum__Mainnet"}
	for _, depth := range []int{3, 5, 2} {
		c.ObserveQueueDepth(depth)
	}
	require.Equal(t, int64(5), c.TakeQueuePeak())
	require.Zero(t, c.TakeQueuePeak())
}

// ---------------------------------------------------------------------------
// Increment* methods – single and concurrent
// ---------------------------------------------------------------------------

func TestIncrementAttestationsCreated(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementAttestationsCreated()
	m.IncrementAttestationsCreated()
	require.Equal(t, int64(2), atomic.LoadInt64(&m.AttestationsCreated))
}

func TestIncrementAttestationErrors(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementAttestationErrors()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.AttestationErrors))
}

func TestIncrementSignatureVerifications(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementSignatureVerifications()
	m.IncrementSignatureVerifications()
	m.IncrementSignatureVerifications()
	require.Equal(t, int64(3), atomic.LoadInt64(&m.SignatureVerifications))
}

func TestIncrementSignatureFailures(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementSignatureFailures()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.SignatureFailures))
}

func TestIncrementDocumentsReceived(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementDocumentsReceived()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.DocumentsReceived))
}

func TestIncrementViewsRegistered(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementViewsRegistered()
	m.IncrementViewsRegistered()
	require.Equal(t, int64(2), atomic.LoadInt64(&m.ViewsRegistered))
}

// Test atomic behavior under concurrency.
func TestIncrementConcurrent(t *testing.T) {
	m := NewHostMetrics()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			m.IncrementAttestationsCreated()
			m.IncrementAttestationErrors()
			m.IncrementSignatureVerifications()
			m.IncrementSignatureFailures()
			m.IncrementDocumentsReceived()
			m.IncrementViewsRegistered()
		}()
	}
	wg.Wait()

	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.AttestationsCreated))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.AttestationErrors))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.SignatureVerifications))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.SignatureFailures))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.DocumentsReceived))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.ViewsRegistered))
}

// ---------------------------------------------------------------------------
// SetViewsActive
// ---------------------------------------------------------------------------

func TestSetViewsActive(t *testing.T) {
	m := NewHostMetrics()
	m.SetViewsActive(42)
	require.Equal(t, int64(42), atomic.LoadInt64(&m.ViewsActive))

	m.SetViewsActive(0)
	require.Equal(t, int64(0), atomic.LoadInt64(&m.ViewsActive))
}

// ---------------------------------------------------------------------------
// UpdateLastProcessingTime
// ---------------------------------------------------------------------------

func TestUpdateLastProcessingTime(t *testing.T) {
	m := NewHostMetrics()

	m.UpdateLastProcessingTime(3.14)
	snap := m.GetSnapshot()
	require.InDelta(t, 3.14, snap.LastProcessingTime, 1e-9)

	m.UpdateLastProcessingTime(0.0)
	snap = m.GetSnapshot()
	require.InDelta(t, 0.0, snap.LastProcessingTime, 1e-9)

	m.UpdateLastProcessingTime(math.MaxFloat64)
	snap = m.GetSnapshot()
	require.InDelta(t, math.MaxFloat64, snap.LastProcessingTime, 1)
}

// ---------------------------------------------------------------------------
// ChainMetrics.UpdateCurrentBlock
// ---------------------------------------------------------------------------

func TestUpdateCurrentBlock_HigherValue(t *testing.T) {
	c := &ChainMetrics{}

	c.UpdateCurrentBlock(100)
	require.Equal(t, int64(100), c.CurrentBlock())

	c.UpdateCurrentBlock(200)
	require.Equal(t, int64(200), c.CurrentBlock())
}

func TestUpdateCurrentBlock_LowerValue_NoOp(t *testing.T) {
	c := &ChainMetrics{}

	c.UpdateCurrentBlock(200)
	c.UpdateCurrentBlock(100) // lower => no-op
	require.Equal(t, int64(200), c.CurrentBlock())
}

func TestUpdateCurrentBlock_EqualValue_NoOp(t *testing.T) {
	c := &ChainMetrics{}

	c.UpdateCurrentBlock(200)
	c.UpdateCurrentBlock(200) // equal => no-op
	require.Equal(t, int64(200), c.CurrentBlock())
}

func TestUpdateCurrentBlock_CASRetry(t *testing.T) {
	c := &ChainMetrics{}
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(n int64) {
			defer wg.Done()
			c.UpdateCurrentBlock(n)
		}(int64(i + 1))
	}
	wg.Wait()

	require.Equal(t, int64(goroutines), c.CurrentBlock())
}

// ---------------------------------------------------------------------------
// GetSnapshot
// ---------------------------------------------------------------------------

func TestGetSnapshot(t *testing.T) {
	m := NewHostMetrics()

	// Set a variety of fields
	m.IncrementAttestationsCreated()
	m.IncrementAttestationsCreated()
	m.IncrementAttestationErrors()
	m.IncrementSignatureVerifications()
	m.IncrementSignatureFailures()
	m.IncrementDocumentsReceived()
	m.IncrementBlocksProcessed()
	m.IncrementTransactionsProcessed()
	m.IncrementLogsProcessed()
	m.IncrementAccessListsProcessed()
	m.IncrementBlockSignaturesProcessed()
	m.IncrementViewsRegistered()
	m.SetViewsActive(7)
	m.UpdateLastProcessingTime(1.5)

	snap := m.GetSnapshot()
	require.NotNil(t, snap)

	require.Equal(t, int64(2), snap.AttestationsCreated)
	require.Equal(t, int64(1), snap.AttestationErrors)
	require.Equal(t, int64(1), snap.SignatureVerifications)
	require.Equal(t, int64(1), snap.SignatureFailures)
	require.Equal(t, int64(1), snap.DocumentsReceived)
	require.Equal(t, int64(1), snap.BlocksProcessed)
	require.Equal(t, int64(1), snap.TransactionsProcessed)
	require.Equal(t, int64(1), snap.LogsProcessed)
	require.Equal(t, int64(1), snap.AccessListsProcessed)
	require.Equal(t, int64(1), snap.BlockSignaturesProcessed)
	require.Equal(t, int64(1), snap.ViewsRegistered)
	require.Equal(t, int64(7), snap.ViewsActive)
	require.InDelta(t, 1.5, snap.LastProcessingTime, 1e-9)
	require.Equal(t, m.StartTime, snap.StartTime)
	require.Equal(t, m.BuildTags, snap.BuildTags)
	require.Equal(t, m.SchemaType, snap.SchemaType)
}

// ---------------------------------------------------------------------------
// ServeHTTP
// ---------------------------------------------------------------------------

// /metrics lists every chain and reports heights only per chain.
func TestServeHTTP_JSON(t *testing.T) {
	ethCutoff, bscCutoff := &pruner.Cutoff{}, &pruner.Cutoff{}
	ethCutoff.Raise(30)
	bscCutoff.Raise(70)
	lastDocument := time.Now()
	tests := []struct {
		name         string
		chains       []*ChainMetrics
		blocks       []int64   // each chain's current block, 0 for none
		lastDocument time.Time // the first chain's last document, zero for none
		// wantChains is the chains list as raw JSON, so an omitted field and a 0 differ.
		wantChains string
	}{
		{
			name:         "one chain with a document",
			chains:       []*ChainMetrics{{Prefix: "Ethereum__Mainnet", Enabled: true}},
			blocks:       []int64{0},
			lastDocument: lastDocument,
			wantChains:   fmt.Sprintf(`[{"chain":"Ethereum__Mainnet","enabled":true,"ready":true,"last_document_time":%q}]`, lastDocument.Format(time.RFC3339Nano)),
		},
		{
			name: "several chains",
			chains: []*ChainMetrics{
				{Prefix: "Ethereum__Mainnet", Enabled: true, Cutoff: ethCutoff},
				{Prefix: "BSC__Mainnet", Reason: ReasonSchemaDrift, Cutoff: bscCutoff},
			},
			blocks: []int64{42, 0},
			wantChains: `[
				{"chain":"Ethereum__Mainnet","enabled":true,"ready":true,"current_block":42,"cutoff":30},
				{"chain":"BSC__Mainnet","enabled":false,"ready":false,"reason":"schema_drift","cutoff":70}
			]`,
		},
		{
			name: "one chain and a removed one",
			chains: []*ChainMetrics{
				{Prefix: "Ethereum__Mainnet", Enabled: true, Cutoff: ethCutoff},
				{Prefix: "BSC__Mainnet", Reason: ReasonRemoved},
			},
			blocks: []int64{42, 0},
			wantChains: `[
				{"chain":"Ethereum__Mainnet","enabled":true,"ready":true,"current_block":42,"cutoff":30},
				{"chain":"BSC__Mainnet","enabled":false,"ready":false,"reason":"removed"}
			]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHostMetrics(tt.chains...)
			for i, c := range tt.chains {
				c.UpdateCurrentBlock(tt.blocks[i])
			}
			if !tt.lastDocument.IsZero() {
				tt.chains[0].lastDocument.Store(tt.lastDocument.UnixNano())
			}

			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			req.Header.Set("Accept", "application/json")
			w := httptest.NewRecorder()
			m.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Header().Get("Content-Type"), "application/json")

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Contains(t, resp, "metrics")
			require.Contains(t, resp, "chains")
			require.Contains(t, resp, "timestamp")
			require.Contains(t, resp, "uptime_human")
			require.Contains(t, resp, "uptime_seconds")
			require.JSONEq(t, tt.wantChains, string(resp["chains"]))

			require.NotContains(t, resp, "current_block")

			var metrics map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(resp["metrics"], &metrics))
			// The snapshot's counters sit directly under metrics.
			require.Contains(t, metrics, "blocks_processed")
		})
	}
}

func TestServeHTTP_HTML(t *testing.T) {
	m := NewHostMetrics()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/html")
	w := httptest.NewRecorder()

	m.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "text/html")
	require.NotEmpty(t, w.Body.Bytes())
}

func TestServeHTTP_DefaultAccept(t *testing.T) {
	m := NewHostMetrics()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	// No Accept header => JSON path
	w := httptest.NewRecorder()

	m.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

func TestServeHTTP_BothHTMLAndJSON(t *testing.T) {
	m := NewHostMetrics()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/html, application/json")
	w := httptest.NewRecorder()

	m.ServeHTTP(w, req)

	// When both are present, the condition (has text/html AND NOT application/json)
	// is false, so it falls through to JSON.
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

// ---------------------------------------------------------------------------
// getMetricsClientPageHTML
// ---------------------------------------------------------------------------

func TestGetMetricsClientPageHTML(t *testing.T) {
	m := NewHostMetrics()
	html := m.getMetricsClientPageHTML()
	require.NotEmpty(t, html, "metrics client page HTML must not be empty")
}

// ---------------------------------------------------------------------------
// ServeHTTP – JSON encoding error path
// ---------------------------------------------------------------------------

// failWriter is a ResponseWriter that fails on Write to trigger encoding errors.
type failWriter struct {
	header     http.Header
	statusCode int
	written    bool
}

func newFailWriter() *failWriter {
	return &failWriter{header: make(http.Header)}
}

func (w *failWriter) Header() http.Header        { return w.header }
func (w *failWriter) WriteHeader(statusCode int) { w.statusCode = statusCode }
func (w *failWriter) Write(_ []byte) (int, error) {
	if !w.written {
		// Let the first write through (for the Content-Type / status), then fail
		w.written = true
		return 0, errSimulatedWrite
	}
	return 0, errSimulatedWrite
}

func TestServeHTTP_JSONEncodeError(t *testing.T) {
	m := NewHostMetrics()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/json")
	w := newFailWriter()

	// This exercises the json.Encode error path in ServeHTTP
	m.ServeHTTP(w, req)

	// The error handler calls http.Error which also writes, but the key point
	// is that the error path is exercised
	require.Equal(t, http.StatusInternalServerError, w.statusCode)
}

// ---------------------------------------------------------------------------
// getMetricsClientPageHTML – disk-read path
// ---------------------------------------------------------------------------

func TestGetMetricsClientPageHTML_DiskReadPath(t *testing.T) {
	// Save original and restore after test
	origPath := metricsClientPagePath
	defer func() { metricsClientPagePath = origPath }()

	// Write a temp file to a known path
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "metrics_client_page.html")
	testContent := "<html><body>test metrics page</body></html>"
	err := os.WriteFile(filepath.Clean(tmpFile), []byte(testContent), 0o600)
	require.NoError(t, err)

	// Override the package-level path to point to our temp file
	metricsClientPagePath = tmpFile

	m := NewHostMetrics()
	html := m.getMetricsClientPageHTML()
	require.Equal(t, testContent, string(html), "should read from disk when file exists")
}

func TestGetMetricsClientPageHTML_SecondPath(t *testing.T) {
	// Test the second possible path ("./metrics_client_page.html").
	// Override the primary path to a non-existent file so the loop falls through
	// to the second path. Since tests run from pkg/server/, the file exists there.
	origPath := metricsClientPagePath
	defer func() { metricsClientPagePath = origPath }()

	metricsClientPagePath = filepath.Join(t.TempDir(), "nonexistent.html")

	m := NewHostMetrics()
	html := m.getMetricsClientPageHTML()
	require.NotEmpty(t, html, "second disk path should find the HTML file")
	require.Contains(t, string(html), "<html", "loaded content should be valid HTML")
}
