package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMountStatusReportsOkAndUptime(t *testing.T) {
	mux := http.NewServeMux()
	startedAt := time.Now().Add(-5 * time.Second)
	mountStatus(mux, startedAt)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body statusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "ok", body.Status)
	require.NotEmpty(t, body.Uptime)

	uptime, err := time.ParseDuration(body.Uptime)
	require.NoError(t, err)
	require.GreaterOrEqual(t, uptime, 5*time.Second)
}
