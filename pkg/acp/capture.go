package acp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// captureWriter records the status and a copy of the response body while writing
// it through to the client, so the served rows can be counted. SSE streams are
// passed through unbuffered.
type captureWriter struct {
	http.ResponseWriter

	status    int
	streaming bool
	body      bytes.Buffer
}

func newCaptureWriter(w http.ResponseWriter) *captureWriter {
	return &captureWriter{ResponseWriter: w, status: http.StatusOK}
}

func (c *captureWriter) WriteHeader(status int) {
	c.status = status
	// An SSE stream (defradb serves subscriptions on this URL) would grow the
	// buffer without bound, so don't buffer it.
	if strings.HasPrefix(c.Header().Get("Content-Type"), "text/event-stream") {
		c.streaming = true
	}
	c.ResponseWriter.WriteHeader(status)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if !c.streaming {
		c.body.Write(b)
	}
	return c.ResponseWriter.Write(b)
}

// Flush forwards to the wrapped writer; defradb's SSE subscription handler
// requires an http.Flusher.
func (c *captureWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// viewRows sums the rows under viewKeys (the billed view's response key(s)) in a
// GraphQL response body, and reports whether the view served. A value that is
// null, absent, or not an array contributes nothing and is not served; an empty
// array is served with zero rows.
func viewRows(body []byte, viewKeys []string) (rows uint64, served bool) {
	var resp struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, false
	}
	for _, key := range viewKeys {
		raw, ok := resp.Data[key]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			continue // absent or null: not served
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			continue // not an array
		}
		served = true
		rows += uint64(len(arr))
	}
	return rows, served
}
