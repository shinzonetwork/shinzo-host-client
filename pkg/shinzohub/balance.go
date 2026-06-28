package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
)

// GetQueryBalance returns the spendable query balance for a bech32 address from
// the hub's x/querybalance module. A never-funded address reports "0", so a zero
// balance is a normal result, not an error.
func (c *RPCClient) GetQueryBalance(ctx context.Context, bech32Address string) (*big.Int, error) {
	if c.lcdURL == "" {
		return nil, ErrLCDNotConfigured
	}
	endpoint := fmt.Sprintf("%s/shinzonetwork/querybalance/v1/balance/%s", c.lcdURL, bech32Address)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build LCD request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LCD GET %s: %w", endpoint, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("close LCD response body: %v", cerr)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read LCD response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned HTTP %d: %s", ErrLCDHTTPStatus, endpoint, resp.StatusCode, string(body))
	}

	var wrap struct {
		Amount string `json:"amount"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return nil, fmt.Errorf("decode LCD response: %w", err)
	}
	// proto3 may omit a default-zero string, so an absent amount means zero.
	if wrap.Amount == "" {
		return big.NewInt(0), nil
	}
	amount, ok := new(big.Int).SetString(wrap.Amount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid balance amount %q", wrap.Amount)
	}
	return amount, nil
}
