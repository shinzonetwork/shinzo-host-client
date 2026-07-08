package accounting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClient_Submit_PostsRecord(t *testing.T) {
	var got ServiceRecord
	var gotPath, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"abc"}`))
	}))
	defer srv.Close()

	rec := ServiceRecord{
		Nonce:             "0x01",
		PoolID:            "0xpool",
		QueryHash:         "0xhash",
		HostAddress:       "0xhost",
		RequestTimestamp:  1735689600,
		RequestSignature:  "0xreqsig",
		RowsQueried:       5,
		RespondedAt:       1735690000,
		ResponseCIDs:      []string{},
		ResponseSignature: "0xrespsig",
		AttestedIndexers:  []string{"idx1"},
	}
	c := NewClient(srv.URL, time.Second)
	require.NoError(t, c.Submit(context.Background(), rec))

	require.Equal(t, "/v1/service-records", gotPath)
	require.Equal(t, "application/json", gotContentType)
	require.Equal(t, rec, got)
}

func TestClient_Submit_Non2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid request body"))
	}))
	defer srv.Close()

	err := NewClient(srv.URL, time.Second).Submit(context.Background(), ServiceRecord{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
	require.Contains(t, err.Error(), "invalid request body")
}

func TestClient_Submit_UnreachableIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // unreachable

	err := NewClient(srv.URL, time.Second).Submit(context.Background(), ServiceRecord{})
	require.Error(t, err)
}
