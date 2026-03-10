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

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/require"
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
	require.Equal(t, int64(0), m.BlockSigEventsReceived)
	require.Equal(t, int64(0), m.BlocksReceived)
	require.Equal(t, int64(0), m.DocumentsReceived)
	require.Equal(t, int64(0), m.BlocksProcessed)
	require.Equal(t, int64(0), m.TransactionsProcessed)
	require.Equal(t, int64(0), m.LogsProcessed)
	require.Equal(t, int64(0), m.AccessListsProcessed)
	require.Equal(t, int64(0), m.BlockSignaturesProcessed)
	require.Equal(t, int64(0), m.UniqueBlocks)
	require.Equal(t, int64(0), m.UniqueTransactions)
	require.Equal(t, int64(0), m.UniqueLogs)
	require.Equal(t, int64(0), m.UniqueAccessLists)
	require.Equal(t, int64(0), m.ViewsRegistered)
	require.Equal(t, int64(0), m.ViewsActive)
	require.Equal(t, uint64(0), m.MostRecentBlock)
	// Without branchable build tag, defaults should be "standard" / "non-branchable"
	require.Equal(t, "standard", m.BuildTags)
	require.Equal(t, "non-branchable", m.SchemaType)
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

func TestIncrementBlockSigEventsReceived(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementBlockSigEventsReceived()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.BlockSigEventsReceived))
}

func TestIncrementBlocksReceived(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementBlocksReceived()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.BlocksReceived))
}

func TestIncrementDocumentsReceived(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementDocumentsReceived()
	require.Equal(t, int64(1), atomic.LoadInt64(&m.DocumentsReceived))
	require.False(t, m.LastDocumentTime.IsZero(), "LastDocumentTime should be updated")
}

func TestIncrementViewsRegistered(t *testing.T) {
	m := NewHostMetrics()
	m.IncrementViewsRegistered()
	m.IncrementViewsRegistered()
	require.Equal(t, int64(2), atomic.LoadInt64(&m.ViewsRegistered))
}

// Test atomic behaviour under concurrency
func TestIncrementConcurrent(t *testing.T) {
	m := NewHostMetrics()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			m.IncrementAttestationsCreated()
			m.IncrementAttestationErrors()
			m.IncrementSignatureVerifications()
			m.IncrementSignatureFailures()
			m.IncrementBlockSigEventsReceived()
			m.IncrementBlocksReceived()
			m.IncrementDocumentsReceived()
			m.IncrementViewsRegistered()
		}()
	}
	wg.Wait()

	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.AttestationsCreated))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.AttestationErrors))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.SignatureVerifications))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.SignatureFailures))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.BlockSigEventsReceived))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.BlocksReceived))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.DocumentsReceived))
	require.Equal(t, int64(goroutines), atomic.LoadInt64(&m.ViewsRegistered))
}

// ---------------------------------------------------------------------------
// IncrementDocumentByType
// ---------------------------------------------------------------------------

func TestIncrementDocumentByType(t *testing.T) {
	tests := []struct {
		name    string
		docType string
		field   func(m *HostMetrics) int64
	}{
		{"Block", constants.CollectionBlock, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.BlocksProcessed) }},
		{"Transaction", constants.CollectionTransaction, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.TransactionsProcessed) }},
		{"Log", constants.CollectionLog, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.LogsProcessed) }},
		{"AccessListEntry", constants.CollectionAccessListEntry, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.AccessListsProcessed) }},
		{"BlockSignature", constants.CollectionBlockSignature, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.BlockSignaturesProcessed) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewHostMetrics()
			m.IncrementDocumentByType(tc.docType)
			m.IncrementDocumentByType(tc.docType)
			require.Equal(t, int64(2), tc.field(m))
		})
	}

	// Unknown type should be a no-op
	t.Run("Unknown", func(t *testing.T) {
		m := NewHostMetrics()
		m.IncrementDocumentByType("SomeUnknownCollection")
		require.Equal(t, int64(0), atomic.LoadInt64(&m.BlocksProcessed))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.TransactionsProcessed))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.LogsProcessed))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.AccessListsProcessed))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.BlockSignaturesProcessed))
	})
}

// ---------------------------------------------------------------------------
// IncrementUniqueDocumentByType
// ---------------------------------------------------------------------------

func TestIncrementUniqueDocumentByType(t *testing.T) {
	tests := []struct {
		name    string
		docType string
		field   func(m *HostMetrics) int64
	}{
		{"Block", constants.CollectionBlock, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.UniqueBlocks) }},
		{"Transaction", constants.CollectionTransaction, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.UniqueTransactions) }},
		{"Log", constants.CollectionLog, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.UniqueLogs) }},
		{"AccessListEntry", constants.CollectionAccessListEntry, func(m *HostMetrics) int64 { return atomic.LoadInt64(&m.UniqueAccessLists) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewHostMetrics()
			m.IncrementUniqueDocumentByType(tc.docType)
			require.Equal(t, int64(1), tc.field(m))
		})
	}

	// Unknown and BlockSignature are no-ops for unique counters
	t.Run("Unknown", func(t *testing.T) {
		m := NewHostMetrics()
		m.IncrementUniqueDocumentByType("SomeUnknownCollection")
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueBlocks))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueTransactions))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueLogs))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueAccessLists))
	})

	t.Run("BlockSignature_NoUniqueCounter", func(t *testing.T) {
		m := NewHostMetrics()
		m.IncrementUniqueDocumentByType(constants.CollectionBlockSignature)
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueBlocks))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueTransactions))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueLogs))
		require.Equal(t, int64(0), atomic.LoadInt64(&m.UniqueAccessLists))
	})
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
// UpdateMostRecentBlock
// ---------------------------------------------------------------------------

func TestUpdateMostRecentBlock_HigherValue(t *testing.T) {
	m := NewHostMetrics()

	m.UpdateMostRecentBlock(100)
	require.Equal(t, uint64(100), atomic.LoadUint64(&m.MostRecentBlock))

	m.UpdateMostRecentBlock(200)
	require.Equal(t, uint64(200), atomic.LoadUint64(&m.MostRecentBlock))
}

func TestUpdateMostRecentBlock_LowerValue_NoOp(t *testing.T) {
	m := NewHostMetrics()

	m.UpdateMostRecentBlock(200)
	m.UpdateMostRecentBlock(100) // lower => no-op
	require.Equal(t, uint64(200), atomic.LoadUint64(&m.MostRecentBlock))
}

func TestUpdateMostRecentBlock_EqualValue_NoOp(t *testing.T) {
	m := NewHostMetrics()

	m.UpdateMostRecentBlock(200)
	m.UpdateMostRecentBlock(200) // equal => no-op
	require.Equal(t, uint64(200), atomic.LoadUint64(&m.MostRecentBlock))
}

func TestUpdateMostRecentBlock_CASRetry(t *testing.T) {
	m := NewHostMetrics()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n uint64) {
			defer wg.Done()
			m.UpdateMostRecentBlock(n)
		}(uint64(i + 1))
	}
	wg.Wait()

	require.Equal(t, uint64(goroutines), atomic.LoadUint64(&m.MostRecentBlock))
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
	m.IncrementBlockSigEventsReceived()
	m.IncrementBlocksReceived()
	m.IncrementDocumentsReceived()
	m.IncrementDocumentByType(constants.CollectionBlock)
	m.IncrementDocumentByType(constants.CollectionTransaction)
	m.IncrementDocumentByType(constants.CollectionLog)
	m.IncrementDocumentByType(constants.CollectionAccessListEntry)
	m.IncrementDocumentByType(constants.CollectionBlockSignature)
	m.IncrementUniqueDocumentByType(constants.CollectionBlock)
	m.IncrementUniqueDocumentByType(constants.CollectionTransaction)
	m.IncrementUniqueDocumentByType(constants.CollectionLog)
	m.IncrementUniqueDocumentByType(constants.CollectionAccessListEntry)
	m.IncrementViewsRegistered()
	m.SetViewsActive(7)
	m.UpdateLastProcessingTime(1.5)
	m.UpdateMostRecentBlock(999)

	snap := m.GetSnapshot()
	require.NotNil(t, snap)

	require.Equal(t, int64(2), snap.AttestationsCreated)
	require.Equal(t, int64(1), snap.AttestationErrors)
	require.Equal(t, int64(1), snap.SignatureVerifications)
	require.Equal(t, int64(1), snap.SignatureFailures)
	require.Equal(t, int64(1), snap.BlockSigEventsReceived)
	require.Equal(t, int64(1), snap.BlocksReceived)
	require.Equal(t, int64(1), snap.DocumentsReceived)
	require.Equal(t, int64(1), snap.BlocksProcessed)
	require.Equal(t, int64(1), snap.TransactionsProcessed)
	require.Equal(t, int64(1), snap.LogsProcessed)
	require.Equal(t, int64(1), snap.AccessListsProcessed)
	require.Equal(t, int64(1), snap.BlockSignaturesProcessed)
	require.Equal(t, int64(1), snap.UniqueBlocks)
	require.Equal(t, int64(1), snap.UniqueTransactions)
	require.Equal(t, int64(1), snap.UniqueLogs)
	require.Equal(t, int64(1), snap.UniqueAccessLists)
	require.Equal(t, int64(1), snap.ViewsRegistered)
	require.Equal(t, int64(7), snap.ViewsActive)
	require.InDelta(t, 1.5, snap.LastProcessingTime, 1e-9)
	require.Equal(t, uint64(999), snap.MostRecentBlock)
	require.Equal(t, m.StartTime, snap.StartTime)
	require.Equal(t, m.LastDocumentTime, snap.LastDocumentTime)
	require.Equal(t, m.BuildTags, snap.BuildTags)
	require.Equal(t, m.SchemaType, snap.SchemaType)
	require.Equal(t, int64(0), snap.ProcessingQueueSize)
	require.Equal(t, int64(0), snap.ViewQueueSize)
}

// ---------------------------------------------------------------------------
// ServeHTTP
// ---------------------------------------------------------------------------

func TestServeHTTP_JSON(t *testing.T) {
	m := NewHostMetrics()
	m.UpdateMostRecentBlock(42)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	m.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Contains(t, resp, "metrics")
	require.Contains(t, resp, "current_block")
	require.Contains(t, resp, "timestamp")
	require.Contains(t, resp, "uptime_human")
	require.Contains(t, resp, "uptime_seconds")

	require.Equal(t, float64(42), resp["current_block"])
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

func (w *failWriter) Header() http.Header         { return w.header }
func (w *failWriter) WriteHeader(statusCode int)   { w.statusCode = statusCode }
func (w *failWriter) Write(b []byte) (int, error) {
	if !w.written {
		// Let the first write through (for the Content-Type / status), then fail
		w.written = true
		return 0, fmt.Errorf("simulated write error")
	}
	return 0, fmt.Errorf("simulated write error")
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
	err := os.WriteFile(tmpFile, []byte(testContent), 0644)
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
