//go:build hostplayground

package playground

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewServer_OnlyProxiesGraphQLPost(t *testing.T) {
	var upstreamCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		require.Equal(t, "/api/v0/graphql", r.URL.Path)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"data":{}}`)
	}))
	t.Cleanup(upstream.Close)

	handler, err := NewServer(upstream.URL)
	require.NoError(t, err)

	graphqlRequest := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", strings.NewReader(`{"query":"{ __typename }"}`))
	graphqlResponse := httptest.NewRecorder()
	handler.ServeHTTP(graphqlResponse, graphqlRequest)
	require.Equal(t, http.StatusAccepted, graphqlResponse.Code)
	require.Equal(t, int64(1), upstreamCalls.Load())

	for _, test := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/api/v0/graphql", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/v0/collections", http.StatusNotFound},
		{http.MethodPost, "/api/v0/schema", http.StatusNotFound},
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, test.status, response.Code)
	}
	require.Equal(t, int64(1), upstreamCalls.Load())
}
