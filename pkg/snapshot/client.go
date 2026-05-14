package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Info describes a snapshot file available on the indexer.
type Info struct {
	Filename   string         `json:"filename"`
	StartBlock int64          `json:"start_block"`
	EndBlock   int64          `json:"end_block"`
	SizeBytes  int64          `json:"size_bytes"`
	CreatedAt  time.Time      `json:"created_at"`
	Signed     bool           `json:"signed"`
	Signature  *SignatureData `json:"signature,omitempty"`
}

// SignatureData holds the cryptographic signature for a snapshot file.
type SignatureData struct {
	Version             int      `json:"version"`
	SnapshotFile        string   `json:"snapshot_file"`
	StartBlock          int64    `json:"start_block"`
	EndBlock            int64    `json:"end_block"`
	MerkleRoot          string   `json:"merkle_root"`
	BlockCount          int      `json:"block_count"`
	SignatureType       string   `json:"signature_type"`
	SignatureIdentity   string   `json:"signature_identity"`
	SignatureValue      string   `json:"signature_value"`
	CreatedAt           string   `json:"created_at"`
	BlockSigMerkleRoots []string `json:"block_sig_merkle_roots,omitempty"`
}

// Client is an HTTP client for interacting with an indexer's snapshot API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new snapshot client for the given indexer URL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: httpClientTimeoutMins * time.Minute,
		},
	}
}

// ListSnapshots queries the indexer for available snapshots with inline signature data.
func (c *Client) ListSnapshots() ([]Info, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/snapshots")
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list snapshots status %d: %w", resp.StatusCode, ErrListSnapshotsStatus)
	}

	var result struct {
		Snapshots []Info `json:"snapshots"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode snapshots list: %w", err)
	}

	return result.Snapshots, nil
}

// DownloadSnapshot downloads a snapshot file to the given destination path.
func (c *Client) DownloadSnapshot(filename, destPath string) error {
	resp, err := c.httpClient.Get(c.baseURL + "/snapshots/" + filename)
	if err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download snapshot status %d: %w", resp.StatusCode, ErrDownloadSnapshotStatus)
	}

	f, err := os.Create(filepath.Clean(destPath))
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = os.Remove(destPath)
		return fmt.Errorf("write snapshot: %w", err)
	}

	return nil
}
