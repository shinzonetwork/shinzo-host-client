package snapshot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	c := NewClient("http://example.com")

	require.Equal(t, "http://example.com", c.baseURL)
	require.NotNil(t, c.httpClient)
	require.Equal(t, 5*time.Minute, c.httpClient.Timeout)
}

func TestListSnapshots(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErr    bool
		errContain string
		wantLen    int
		checkFirst func(t *testing.T, snap SnapshotInfo)
	}{
		{
			name: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/snapshots", r.URL.Path)
				require.Equal(t, http.MethodGet, r.Method)

				resp := struct {
					Snapshots []SnapshotInfo `json:"snapshots"`
				}{
					Snapshots: []SnapshotInfo{
						{
							Filename:   "snapshot-0-100.gz",
							StartBlock: 0,
							EndBlock:   100,
							SizeBytes:  1024,
							CreatedAt:  now,
							Signed:     true,
							Signature: &SnapshotSignatureData{
								Version:           1,
								SnapshotFile:      "snapshot-0-100.gz",
								StartBlock:        0,
								EndBlock:          100,
								MerkleRoot:        "abcd1234",
								BlockCount:        101,
								SignatureType:      "Ed25519",
								SignatureIdentity: "pubkey123",
								SignatureValue:    "sig456",
								CreatedAt:         now.Format(time.RFC3339),
							},
						},
						{
							Filename:   "snapshot-101-200.gz",
							StartBlock: 101,
							EndBlock:   200,
							SizeBytes:  2048,
							CreatedAt:  now,
							Signed:     false,
						},
					},
				}

				w.Header().Set("Content-Type", "application/json")
				err := json.NewEncoder(w).Encode(resp)
				require.NoError(t, err)
			},
			wantLen: 2,
			checkFirst: func(t *testing.T, snap SnapshotInfo) {
				require.Equal(t, "snapshot-0-100.gz", snap.Filename)
				require.Equal(t, int64(0), snap.StartBlock)
				require.Equal(t, int64(100), snap.EndBlock)
				require.Equal(t, int64(1024), snap.SizeBytes)
				require.True(t, snap.Signed)
				require.NotNil(t, snap.Signature)
				require.Equal(t, "Ed25519", snap.Signature.SignatureType)
				require.Equal(t, "abcd1234", snap.Signature.MerkleRoot)
				require.Equal(t, 1, snap.Signature.Version)
				require.Equal(t, "pubkey123", snap.Signature.SignatureIdentity)
				require.Equal(t, "sig456", snap.Signature.SignatureValue)
				require.Equal(t, 101, snap.Signature.BlockCount)
			},
		},
		{
			name: "empty list",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"snapshots":[]}`))
			},
			wantLen: 0,
		},
		{
			name: "HTTP error 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr:    true,
			errContain: "status 500",
		},
		{
			name: "HTTP error 503",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			wantErr:    true,
			errContain: "status 503",
		},
		{
			name: "JSON decode error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{not valid json`))
			},
			wantErr:    true,
			errContain: "decode snapshots list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			c := NewClient(srv.URL)
			snapshots, err := c.ListSnapshots()

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContain)
				return
			}

			require.NoError(t, err)
			require.Len(t, snapshots, tt.wantLen)

			if tt.checkFirst != nil && len(snapshots) > 0 {
				tt.checkFirst(t, snapshots[0])
			}
		})
	}
}

func TestListSnapshots_ConnectionRefused(t *testing.T) {
	// Use a URL that will fail to connect.
	c := NewClient("http://127.0.0.1:1") // port 1 is almost certainly not listening
	_, err := c.ListSnapshots()
	require.Error(t, err)
	require.Contains(t, err.Error(), "list snapshots")
}

func TestDownloadSnapshot(t *testing.T) {
	fileContent := []byte("snapshot-binary-data-here-for-testing")

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		destPath   func(t *testing.T) string
		wantErr    bool
		errContain string
		checkFile  bool
	}{
		{
			name: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/snapshots/snap-0-100.gz", r.URL.Path)
				require.Equal(t, http.MethodGet, r.Method)
				w.Header().Set("Content-Type", "application/octet-stream")
				_, err := w.Write(fileContent)
				require.NoError(t, err)
			},
			destPath: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "snap-0-100.gz")
			},
			checkFile: true,
		},
		{
			name: "HTTP error 404",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			destPath: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "snap-missing.gz")
			},
			wantErr:    true,
			errContain: "status 404",
		},
		{
			name: "HTTP error 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			destPath: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "snap.gz")
			},
			wantErr:    true,
			errContain: "status 500",
		},
		{
			name: "file creation error invalid path",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write(fileContent)
			},
			destPath: func(t *testing.T) string {
				// Path with a nonexistent parent directory.
				return "/nonexistent-dir-abc123/nested/snap.gz"
			},
			wantErr:    true,
			errContain: "create file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			c := NewClient(srv.URL)
			dest := tt.destPath(t)
			err := c.DownloadSnapshot("snap-0-100.gz", dest)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContain)
				return
			}

			require.NoError(t, err)

			if tt.checkFile {
				data, readErr := os.ReadFile(dest)
				require.NoError(t, readErr)
				require.Equal(t, fileContent, data)
			}
		})
	}
}

func TestDownloadSnapshot_ConnectionRefused(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	dest := filepath.Join(t.TempDir(), "snap.gz")
	err := c.DownloadSnapshot("snap.gz", dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "download snapshot")
}

func TestDownloadSnapshot_WriteFail(t *testing.T) {
	// Trigger the io.Copy error path (line 94-96 in client.go) by advertising a
	// Content-Length larger than the body actually sent, causing io.Copy to return
	// an "unexpected EOF" error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Tell the client to expect 1 MB, but only send 10 bytes.
		w.Header().Set("Content-Length", fmt.Sprintf("%d", 1024*1024))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("short-body"))
		// The handler returns, closing the connection before the full body is sent.
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	dest := filepath.Join(t.TempDir(), "snap-truncated.gz")

	err := c.DownloadSnapshot("snap.gz", dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "write snapshot")

	// Verify the partially written file was cleaned up.
	_, statErr := os.Stat(dest)
	require.True(t, os.IsNotExist(statErr), "partially written file should be removed")
}

func TestDownloadSnapshot_LargeFile(t *testing.T) {
	// Generate a moderately large payload to exercise io.Copy path.
	largeContent := make([]byte, 256*1024) // 256 KB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(largeContent)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	dest := filepath.Join(t.TempDir(), "large-snap.gz")

	err := c.DownloadSnapshot("large.gz", dest)
	require.NoError(t, err)

	data, err := os.ReadFile(dest)
	require.NoError(t, err)
	require.Equal(t, largeContent, data)
}
