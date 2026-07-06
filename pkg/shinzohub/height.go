package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// LatestBlockNumber returns the hub's most recent block height from its Cosmos
// LCD. The host divides it by the epoch length to derive the current settlement
// epoch.
func (c *RPCClient) LatestBlockNumber(ctx context.Context) (uint64, error) {
	if c.lcdURL == "" {
		return 0, ErrLCDNotConfigured
	}
	endpoint := c.lcdURL + "/cosmos/base/tendermint/v1beta1/blocks/latest"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("build LCD request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("LCD GET %s: %w", endpoint, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("close LCD response body: %v", cerr)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read LCD response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: %s returned HTTP %d: %s", ErrLCDHTTPStatus, endpoint, resp.StatusCode, string(body))
	}

	var wrap struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return 0, fmt.Errorf("decode LCD response: %w", err)
	}
	height, err := strconv.ParseUint(wrap.Block.Header.Height, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid block height %q: %w", wrap.Block.Header.Height, err)
	}
	return height, nil
}
