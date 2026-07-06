// Package accounting submits signed service records to the accounting service
// after a host serves a billed query.
package accounting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ServiceRecord is the JSON body the host POSTs to the accounting service. Hash,
// address, and signature fields are 0x-prefixed hex; timestamps are unix seconds.
// The accounting service recovers the payer from RequestSignature and the host
// from ResponseSignature.
type ServiceRecord struct {
	Nonce             string   `json:"nonce"`
	PoolID            string   `json:"pool_id"`
	QueryHash         string   `json:"query_hash"`
	HostAddress       string   `json:"host_address"`
	RequestTimestamp  uint64   `json:"request_timestamp"`
	RequestSignature  string   `json:"request_signature"`
	RowsQueried       uint64   `json:"rows_queried"`
	RespondedAt       uint64   `json:"responded_at"`
	ResponseCIDs      []string `json:"response_cids"`
	ResponseSignature string   `json:"response_signature"`
	AttestedIndexers  []string `json:"attested_indexers"`
}

// Client posts service records to the accounting service.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a Client that POSTs to baseURL with the given request
// timeout. A zero timeout means no timeout, per http.Client.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// Submit POSTs record to /v1/service-records. A non-2xx response is an error,
// carrying the status and the first part of the response body for context.
func (c *Client) Submit(ctx context.Context, record ServiceRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal service record: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/service-records", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post service record: %w", err)
	}
	// Drain before closing so the keep-alive connection can be reused.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("accounting service returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}
