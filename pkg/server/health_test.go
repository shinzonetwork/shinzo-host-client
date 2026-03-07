package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// ---------------------------------------------------------------------------
// Mock HealthChecker
// ---------------------------------------------------------------------------

type mockHealthChecker struct {
	healthy       bool
	currentBlock  int64
	lastProcessed time.Time
	peerInfo      *P2PInfo
	peerInfoErr   error
	signErr       error
	defraReg      DefraPKRegistration
	peerReg       PeerIDRegistration
}

func (m *mockHealthChecker) IsHealthy() bool                { return m.healthy }
func (m *mockHealthChecker) GetCurrentBlock() int64         { return m.currentBlock }
func (m *mockHealthChecker) GetLastProcessedTime() time.Time { return m.lastProcessed }
func (m *mockHealthChecker) GetPeerInfo() (*P2PInfo, error)  { return m.peerInfo, m.peerInfoErr }
func (m *mockHealthChecker) SignMessages(_ string) (DefraPKRegistration, PeerIDRegistration, error) {
	return m.defraReg, m.peerReg, m.signErr
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newHS(host HealthChecker, defraURL string) *HealthServer {
	return &HealthServer{
		host:     host,
		defraURL: defraURL,
	}
}

// ---------------------------------------------------------------------------
// healthHandler
// ---------------------------------------------------------------------------

func TestHealthHandler_GET_Healthy(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  100,
		lastProcessed: time.Now(),
		peerInfo:      &P2PInfo{Enabled: true},
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "healthy", resp.Status)
	require.Equal(t, int64(100), resp.CurrentBlock)
	require.True(t, resp.DefraDBConnected)
}

func TestHealthHandler_GET_Unhealthy(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       false,
		currentBlock:  50,
		lastProcessed: time.Now(),
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "unhealthy", resp.Status)
}

func TestHealthHandler_GET_HTML(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "text/html")
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "text/html")
	require.NotEmpty(t, w.Body.Bytes())
}

func TestHealthHandler_POST_MethodNotAllowed(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHealthHandler_NilHost(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	// With nil host, status should remain "healthy" (no IsHealthy call)
	require.Equal(t, "healthy", resp.Status)
	require.Equal(t, int64(0), resp.CurrentBlock)
}

// ---------------------------------------------------------------------------
// registrationHandler
// ---------------------------------------------------------------------------

func TestRegistrationHandler_GET_Ready(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  200,
		lastProcessed: time.Now(), // recent => ready
		peerInfo:      &P2PInfo{Enabled: true},
		defraReg:      DefraPKRegistration{PublicKey: "abc", SignedPKMsg: "def"},
		peerReg:       PeerIDRegistration{PeerID: "peer1", SignedPeerMsg: "sig1"},
	}
	hs := newHS(mock, "") // empty defraURL => checkDefraDB returns true

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	// Status 200 means ready
	require.Equal(t, http.StatusOK, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "ready", resp.Status)
	require.NotNil(t, resp.Registration)
}

func TestRegistrationHandler_GET_NotReady_StaleTime(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  200,
		lastProcessed: time.Now().Add(-10 * time.Minute), // stale
		peerInfo:      &P2PInfo{Enabled: true},
		defraReg:      DefraPKRegistration{PublicKey: "abc", SignedPKMsg: "def"},
		peerReg:       PeerIDRegistration{PeerID: "peer1", SignedPeerMsg: "sig1"},
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "not ready", resp.Status)
}

func TestRegistrationHandler_POST_MethodNotAllowed(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodPost, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestRegistrationHandler_PeerInfoError(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  200,
		lastProcessed: time.Now(),
		peerInfo:      nil,
		peerInfoErr:   fmt.Errorf("peer info error"),
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// ---------------------------------------------------------------------------
// registrationAppHandler
// ---------------------------------------------------------------------------

func TestRegistrationAppHandler_SuccessRedirect(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:  true,
		defraReg: DefraPKRegistration{PublicKey: "0xabc", SignedPKMsg: "0xdef"},
		peerReg:  PeerIDRegistration{PeerID: "0xpeer1", SignedPeerMsg: "0xsig1"},
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration-app", nil)
	w := httptest.NewRecorder()

	hs.registrationAppHandler(w, req)

	require.Equal(t, http.StatusTemporaryRedirect, w.Code)
	loc := w.Header().Get("Location")
	require.Contains(t, loc, "https://register.shinzo.network/")
	require.Contains(t, loc, "defraPublicKey=")
}

func TestRegistrationAppHandler_NilHost(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/registration-app", nil)
	w := httptest.NewRecorder()

	hs.registrationAppHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestRegistrationAppHandler_SignError(t *testing.T) {
	mock := &mockHealthChecker{
		healthy: true,
		signErr: fmt.Errorf("signing error"),
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration-app", nil)
	w := httptest.NewRecorder()

	hs.registrationAppHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// ---------------------------------------------------------------------------
// metricsHandler (the /stats endpoint)
// ---------------------------------------------------------------------------

func TestMetricsHandler_GET_WithHost(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  300,
		lastProcessed: time.Now(),
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()

	hs.metricsHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var resp MetricsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, int64(300), resp.CurrentBlock)
}

func TestMetricsHandler_GET_WithoutHost(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()

	hs.metricsHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp MetricsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, int64(0), resp.CurrentBlock)
}

func TestMetricsHandler_POST_MethodNotAllowed(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodPost, "/stats", nil)
	w := httptest.NewRecorder()

	hs.metricsHandler(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// ---------------------------------------------------------------------------
// rootHandler
// ---------------------------------------------------------------------------

func TestRootHandler_RootPath(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	hs.rootHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "Shinzo Network Host", resp["service"])
	require.Equal(t, "running", resp["status"])
}

func TestRootHandler_NonRootPath_404(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	w := httptest.NewRecorder()

	hs.rootHandler(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// ---------------------------------------------------------------------------
// checkDefraDB
// ---------------------------------------------------------------------------

func TestCheckDefraDB_EmptyURL(t *testing.T) {
	hs := newHS(nil, "")
	require.True(t, hs.checkDefraDB(), "empty URL should return true (embedded mode)")
}

func TestCheckDefraDB_LocalhostURL(t *testing.T) {
	hs := newHS(nil, "http://localhost:9181")
	require.True(t, hs.checkDefraDB(), "localhost URL should return true")
}

func TestCheckDefraDB_Localhost127(t *testing.T) {
	hs := newHS(nil, "http://127.0.0.1:9181")
	require.True(t, hs.checkDefraDB(), "127.0.0.1 URL should return true")
}

func TestCheckDefraDB_ExternalURL_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	hs := newHS(nil, ts.URL)
	require.True(t, hs.checkDefraDB())
}

func TestCheckDefraDB_ExternalURL_BadRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // GraphQL endpoint returns 400 for GET
	}))
	defer ts.Close()

	hs := newHS(nil, ts.URL)
	require.True(t, hs.checkDefraDB())
}

func TestCheckDefraDB_ExternalURL_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	// httptest uses 127.0.0.1, which checkDefraDB short-circuits as localhost.
	// Use the unreachable-host approach instead to actually test the HTTP-status branch.
	// We start a real listener on a non-localhost interface-agnostic address by replacing
	// 127.0.0.1 with an external-looking URL that still routes locally via the test server.
	// Since we can't easily bind to a non-localhost, just verify the production code's 500
	// behavior through CheckDefraDB logic: 500 != 200 && 500 != 400 => false.
	// The short-circuit means httptest servers on localhost always return true,
	// so we skip this test since checkDefraDB intentionally returns true for localhost.
	t.Skip("checkDefraDB always returns true for localhost/127.0.0.1 URLs (production design)")
}

func TestCheckDefraDB_ExternalURL_Unreachable(t *testing.T) {
	hs := newHS(nil, "http://192.0.2.1:9999") // RFC 5737 TEST-NET: guaranteed unreachable
	require.False(t, hs.checkDefraDB())
}

// ---------------------------------------------------------------------------
// normalizeHex
// ---------------------------------------------------------------------------

func TestNormalizeHex(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"empty string", "", ""},
		{"0x prefix", "0xabcdef", "0xabcdef"},
		{"0X prefix", "0Xabcdef", "0xabcdef"},
		{"no prefix", "abcdef", "0xabcdef"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, normalizeHex(tc.input))
		})
	}
}

// ---------------------------------------------------------------------------
// getHealthStatusPageHTML
// ---------------------------------------------------------------------------

func TestGetHealthStatusPageHTML_FallbackToEmbedded(t *testing.T) {
	hs := newHS(nil, "")
	html := hs.getHealthStatusPageHTML()
	require.NotEmpty(t, html, "embedded HTML must not be empty")
}

// ---------------------------------------------------------------------------
// NewHealthServer
// ---------------------------------------------------------------------------

func TestNewHealthServer(t *testing.T) {
	mock := &mockHealthChecker{healthy: true}
	metrics := NewHostMetrics()

	hs := NewHealthServer(9999, mock, "http://localhost:9181", metrics)
	require.NotNil(t, hs)
	require.NotNil(t, hs.server)
	require.Equal(t, ":9999", hs.server.Addr)
	require.Equal(t, "http://localhost:9181", hs.defraURL)
}

func TestNewHealthServer_NilMetrics(t *testing.T) {
	hs := NewHealthServer(9999, nil, "", nil)
	require.NotNil(t, hs)
}

// ---------------------------------------------------------------------------
// getRegistrationData
// ---------------------------------------------------------------------------

func TestGetRegistrationData_NilHost(t *testing.T) {
	hs := newHS(nil, "")
	reg, err := hs.getRegistrationData()
	require.Error(t, err)
	require.Nil(t, reg)
}

func TestGetRegistrationData_WithHost(t *testing.T) {
	mock := &mockHealthChecker{
		defraReg: DefraPKRegistration{PublicKey: "abc", SignedPKMsg: "def"},
		peerReg:  PeerIDRegistration{PeerID: "peer1", SignedPeerMsg: "sig1"},
	}
	hs := newHS(mock, "")

	reg, err := hs.getRegistrationData()
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.True(t, reg.Enabled)
	// The function normalizes hex strings
	require.Equal(t, "0xabc", reg.DefraPKRegistration.PublicKey)
	require.Equal(t, "0xdef", reg.DefraPKRegistration.SignedPKMsg)
	require.Equal(t, "0xpeer1", reg.PeerIDRegistration.PeerID)
	require.Equal(t, "0xsig1", reg.PeerIDRegistration.SignedPeerMsg)
}

func TestGetRegistrationData_SignError(t *testing.T) {
	mock := &mockHealthChecker{
		signErr: fmt.Errorf("signing error"),
	}
	hs := newHS(mock, "")

	reg, err := hs.getRegistrationData()
	require.Error(t, err)
	require.NotNil(t, reg)
	require.False(t, reg.Enabled)
}

// ---------------------------------------------------------------------------
// registrationHandler – nil host (no PeerInfo, no Registration calls)
// ---------------------------------------------------------------------------

func TestRegistrationHandler_NilHost(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "ready", resp.Status)
	require.Nil(t, resp.Registration)
}

// ---------------------------------------------------------------------------
// registrationHandler – zero lastProcessed time (should still be ready)
// ---------------------------------------------------------------------------

func TestRegistrationHandler_ZeroLastProcessed(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  1,
		lastProcessed: time.Time{}, // zero value
		peerInfo:      &P2PInfo{Enabled: true},
		defraReg:      DefraPKRegistration{PublicKey: "abc", SignedPKMsg: "def"},
		peerReg:       PeerIDRegistration{PeerID: "peer1", SignedPeerMsg: "sig1"},
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	// Zero time is treated as "not stale" because IsZero() returns true,
	// so the stale check is skipped.
	require.Equal(t, http.StatusOK, w.Code)
}

// ---------------------------------------------------------------------------
// registrationHandler – DefraDB not connected makes it not ready
// ---------------------------------------------------------------------------

func TestRegistrationHandler_DefraDBNotConnected(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  100,
		lastProcessed: time.Now(),
		peerInfo:      &P2PInfo{Enabled: true},
		defraReg:      DefraPKRegistration{PublicKey: "abc", SignedPKMsg: "def"},
		peerReg:       PeerIDRegistration{PeerID: "peer1", SignedPeerMsg: "sig1"},
	}
	// External URL that is unreachable
	hs := newHS(mock, "http://192.0.2.1:9999")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// ---------------------------------------------------------------------------
// healthHandler – default accept header (no explicit JSON/HTML)
// ---------------------------------------------------------------------------

func TestHealthHandler_DefaultAccept(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	// No Accept header set -> defaults to JSON path
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

// ---------------------------------------------------------------------------
// healthHandler – Accept: text/html, application/json (both set)
// ---------------------------------------------------------------------------

func TestHealthHandler_AcceptBothHTMLAndJSON(t *testing.T) {
	hs := newHS(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "text/html, application/json")
	w := httptest.NewRecorder()

	hs.healthHandler(w, req)

	// When both text/html and application/json are present, it goes to JSON
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

// ---------------------------------------------------------------------------
// Start / Stop lifecycle
// ---------------------------------------------------------------------------

func TestHealthServer_StartStop(t *testing.T) {
	// Use a random free port by first finding one
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close() // free the port for the server to use

	hs := NewHealthServer(port, nil, "", nil)

	// Start() calls ListenAndServe which blocks, so run it in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- hs.Start()
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Verify the server is serving by making a request
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Stop the server gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = hs.Stop(ctx)
	require.NoError(t, err)

	// Verify Start() returned http.ErrServerClosed
	serveErr := <-errCh
	require.Equal(t, http.ErrServerClosed, serveErr)
}

// ---------------------------------------------------------------------------
// checkDefraDB – external URL with 500 response (non-localhost)
// ---------------------------------------------------------------------------

func TestCheckDefraDB_ExternalURL_ServerError_CustomListener(t *testing.T) {
	// Create a test server that returns 500
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	// httptest URLs use 127.0.0.1 which checkDefraDB short-circuits as localhost.
	// Use a custom listener on 0.0.0.0 to exercise the HTTP request path.
	_ = ts // ts is not used since its URL contains 127.0.0.1
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})}
	go srv.Serve(listener)
	defer srv.Close()

	// Get the port and construct a URL without localhost/127.0.0.1
	port := listener.Addr().(*net.TCPAddr).Port
	// 0.0.0.0 is not "localhost" or "127.0.0.1" so this exercises the HTTP path
	url := fmt.Sprintf("http://0.0.0.0:%d", port)
	hs := newHS(nil, url)
	require.False(t, hs.checkDefraDB(), "500 status should return false")
}

func TestCheckDefraDB_ExternalURL_200_CustomListener(t *testing.T) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	go srv.Serve(listener)
	defer srv.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("http://0.0.0.0:%d", port)
	hs := newHS(nil, url)
	require.True(t, hs.checkDefraDB(), "200 status should return true")
}

func TestCheckDefraDB_ExternalURL_400_CustomListener(t *testing.T) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})}
	go srv.Serve(listener)
	defer srv.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("http://0.0.0.0:%d", port)
	hs := newHS(nil, url)
	require.True(t, hs.checkDefraDB(), "400 status should return true (GraphQL endpoint)")
}

// ---------------------------------------------------------------------------
// registrationHandler – host with sign error but peer info OK
// ---------------------------------------------------------------------------

func TestRegistrationHandler_SignError_PeerInfoOK(t *testing.T) {
	mock := &mockHealthChecker{
		healthy:       true,
		currentBlock:  100,
		lastProcessed: time.Now(),
		peerInfo:      &P2PInfo{Enabled: true},
		peerInfoErr:   nil,
		signErr:       fmt.Errorf("signing not configured"),
	}
	hs := newHS(mock, "")

	req := httptest.NewRequest(http.MethodGet, "/registration", nil)
	w := httptest.NewRecorder()

	hs.registrationHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp HealthResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "ready", resp.Status)
	// Registration should be present but disabled due to signing error
	require.NotNil(t, resp.Registration)
	require.False(t, resp.Registration.Enabled)
}

// ---------------------------------------------------------------------------
// getHealthStatusPageHTML – disk-read path
// ---------------------------------------------------------------------------

func TestGetHealthStatusPageHTML_DiskReadPath(t *testing.T) {
	// Save original and restore after test
	origPath := healthStatusPagePath
	defer func() { healthStatusPagePath = origPath }()

	// Write a temp file to a known path
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "health_status_page.html")
	testContent := "<html><body>test health page</body></html>"
	err := os.WriteFile(tmpFile, []byte(testContent), 0644)
	require.NoError(t, err)

	// Override the package-level path to point to our temp file
	healthStatusPagePath = tmpFile

	hs := newHS(nil, "")
	html := hs.getHealthStatusPageHTML()
	require.Equal(t, testContent, string(html), "should read from disk when file exists")
}

func TestGetHealthStatusPageHTML_SecondPath(t *testing.T) {
	// Test the second possible path ("./health_status_page.html").
	// Override the primary path to a non-existent file so the loop falls through
	// to the second path. Since tests run from pkg/server/, the file exists there.
	origPath := healthStatusPagePath
	defer func() { healthStatusPagePath = origPath }()

	healthStatusPagePath = filepath.Join(t.TempDir(), "nonexistent.html")

	hs := newHS(nil, "")
	html := hs.getHealthStatusPageHTML()
	// The second path should find ./health_status_page.html in the test working directory
	require.NotEmpty(t, html, "second disk path should find the HTML file")
	// Verify it loaded actual HTML content (not an empty fallback)
	require.Contains(t, string(html), "<html", "loaded content should be valid HTML")
}
