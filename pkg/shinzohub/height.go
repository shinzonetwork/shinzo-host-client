package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// heightPollTimeout bounds a single /status call so a slow hub RPC pod cannot
// stall the poller goroutine.
const heightPollTimeout = 10 * time.Second

// cometStatus is the subset of the CometBFT /status RPC response carrying the
// chain tip height.
type cometStatus struct {
	Result struct {
		SyncInfo struct {
			LatestBlockHeight string `json:"latest_block_height"`
		} `json:"sync_info"`
	} `json:"result"`
}

// HeightPoller tracks the latest ShinzoHub block height by polling the hub's
// CometBFT /status endpoint. The cached height is stamped onto attestation
// records as shinzoHeight so the accounting service can bin them into epochs.
// A cached value of 0 means the height is not yet known.
type HeightPoller struct {
	rpcURL     string
	httpClient *http.Client
	latest     atomic.Int64
}

// NewHeightPoller builds a poller against a CometBFT RPC base URL (for example
// "http://host:26657"). An empty rpcURL yields a poller that always reports 0,
// so a host with no hub configured degrades to shinzoHeight=0 rather than failing.
func NewHeightPoller(rpcURL string) *HeightPoller {
	return &HeightPoller{
		rpcURL:     rpcURL,
		httpClient: &http.Client{Timeout: heightPollTimeout},
	}
}

// Latest returns the most recently observed ShinzoHub height, or 0 if none has
// been observed yet (including a nil poller).
func (p *HeightPoller) Latest() int64 {
	if p == nil {
		return 0
	}
	return p.latest.Load()
}

// Start fetches the height once, then refreshes it on the given interval until
// ctx is cancelled. It is a no-op when rpcURL is empty. Refresh failures keep
// the last good value rather than resetting it to 0.
func (p *HeightPoller) Start(ctx context.Context, interval time.Duration) {
	if p == nil || p.rpcURL == "" {
		return
	}

	if h, err := p.fetch(ctx); err == nil {
		p.latest.Store(h)
	} else {
		log().Warnf("ShinzoHub height: initial fetch failed: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if h, err := p.fetch(ctx); err == nil {
				p.latest.Store(h)
			} else {
				log().Debugf("ShinzoHub height: refresh failed: %v", err)
			}
		}
	}
}

// fetch reads the chain tip height from the CometBFT /status endpoint.
func (p *HeightPoller) fetch(ctx context.Context) (int64, error) {
	endpoint := p.rpcURL + "/status"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("build status request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("status GET %s: %w", endpoint, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("close status response body: %v", cerr)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: %s returned HTTP %d", ErrHTTPErrorResponse, endpoint, resp.StatusCode)
	}

	var status cometStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return 0, fmt.Errorf("decode status response: %w", err)
	}

	heightStr := status.Result.SyncInfo.LatestBlockHeight
	if heightStr == "" {
		return 0, ErrEmptyHeight
	}
	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height %q: %w", heightStr, err)
	}
	return height, nil
}
