package acp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaptureWriter_TeesAndCaptures(t *testing.T) {
	client := httptest.NewRecorder()
	cw := newCaptureWriter(client)

	cw.WriteHeader(http.StatusTeapot)
	_, err := cw.Write([]byte(`{"data":`))
	require.NoError(t, err)
	_, err = cw.Write([]byte(`{"V":[1]}}`))
	require.NoError(t, err)

	require.Equal(t, http.StatusTeapot, client.Code)
	require.Equal(t, `{"data":{"V":[1]}}`, client.Body.String())
	require.Equal(t, http.StatusTeapot, cw.status)
	require.Equal(t, `{"data":{"V":[1]}}`, cw.body.String())
}

func TestCaptureWriter_DefaultStatusIsOK(t *testing.T) {
	cw := newCaptureWriter(httptest.NewRecorder())
	_, err := cw.Write([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, cw.status)
}

func TestCaptureWriter_FlushDelegatesToUnderlying(t *testing.T) {
	client := httptest.NewRecorder()
	cw := newCaptureWriter(client)
	cw.Flush()
	require.True(t, client.Flushed, "Flush must reach the wrapped writer so SSE is not downgraded")
}

func TestCaptureWriter_StreamingNotBuffered(t *testing.T) {
	client := httptest.NewRecorder()
	cw := newCaptureWriter(client)
	cw.Header().Set("Content-Type", "text/event-stream")
	cw.WriteHeader(http.StatusOK)
	_, err := cw.Write([]byte("data: {\"x\":1}\n\n"))
	require.NoError(t, err)

	require.Equal(t, "data: {\"x\":1}\n\n", client.Body.String(), "client still receives the stream")
	require.Empty(t, cw.body.Bytes(), "a streaming response must not be buffered")
}

func TestViewRows(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		keys       []string
		wantRows   uint64
		wantServed bool
	}{
		{"single view array", `{"data":{"FilteredLogs":[{"x":1},{"x":2},{"x":3}]}}`, []string{"FilteredLogs"}, 3, true},
		{"aliased view key", `{"data":{"myLogs":[{"x":1}]}}`, []string{"myLogs"}, 1, true},
		{"view plus base collection counts only the view", `{"data":{"FilteredLogs":[{"x":1},{"x":2}],"Block":[{"x":1},{"x":2},{"x":3}]}}`, []string{"FilteredLogs"}, 2, true},
		{"view selected twice sums both", `{"data":{"a":[{"x":1}],"b":[{"x":1},{"x":2}]}}`, []string{"a", "b"}, 3, true},
		{"served zero rows is still served", `{"data":{"FilteredLogs":[]}}`, []string{"FilteredLogs"}, 0, true},
		{"sibling field error does not zero the view", `{"data":{"FilteredLogs":[{"x":1}]},"errors":[{"message":"on Block"}]}`, []string{"FilteredLogs"}, 1, true},
		{"query-level error nulls data", `{"data":null,"errors":[{"message":"boom"}]}`, []string{"FilteredLogs"}, 0, false},
		{"view value is null", `{"data":{"FilteredLogs":null}}`, []string{"FilteredLogs"}, 0, false},
		{"view key absent", `{"data":{"Block":[{"x":1}]}}`, []string{"FilteredLogs"}, 0, false},
		{"non-array value", `{"data":{"FilteredLogs":{"count":5}}}`, []string{"FilteredLogs"}, 0, false},
		{"malformed json", `{"data":`, []string{"FilteredLogs"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, served := viewRows([]byte(tc.body), tc.keys)
			require.Equal(t, tc.wantRows, rows)
			require.Equal(t, tc.wantServed, served)
		})
	}
}
