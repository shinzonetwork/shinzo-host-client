package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

// RPCClient queries the Shinzo Hub's Cosmos LCD (REST gateway) for the
// view-registry endpoints used by the host's startup backfill and live
// event hydration.
type RPCClient struct {
	lcdURL     string
	defraNode  *node.Node
	httpClient *http.Client
}

// httpClientTimeout is the per-request timeout for LCD calls. Higher than
// typical Cosmos LCD latency so a single slow page doesn't kill backfill.
const httpClientTimeout = 30 * time.Second

// NewRPCClient creates a client against the hub's Cosmos LCD. An empty
// lcdURL disables backfill and hydration; the caller in that case is
// responsible for not invoking the methods below.
func NewRPCClient(lcdURL string, defraNode *node.Node) *RPCClient {
	return &RPCClient{
		lcdURL:    lcdURL,
		defraNode: defraNode,
		httpClient: &http.Client{
			Timeout: httpClientTimeout,
		},
	}
}

// LCDView mirrors the JSON shape returned by the hub's view-registry REST API.
// `data` carries the base64-encoded view bundle when the request includes
// include_data=true; otherwise it's empty.
type LCDView struct {
	Name            string `json:"name"`
	Creator         string `json:"creator"`
	ContractAddress string `json:"contract_address"`
	Data            string `json:"data"`
	Height          string `json:"height"`
}

// GetViewBundle fetches the base64-encoded wire bundle for a registered view
// identified by its EVM contract address. Callers decode via
// ProcessViewFromWireFormat to get a view.View.
func (c *RPCClient) GetViewBundle(ctx context.Context, contractAddress string) (string, error) {
	if c.lcdURL == "" {
		return "", ErrLCDNotConfigured
	}
	endpoint := fmt.Sprintf("%s/shinzonetwork/view/v1/views/%s?include_data=true", c.lcdURL, contractAddress)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build LCD request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("LCD GET %s: %w", endpoint, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("close LCD response body: %v", cerr)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read LCD response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: %s returned HTTP %d: %s", ErrLCDHTTPStatus, endpoint, resp.StatusCode, string(body))
	}

	var wrap struct {
		View LCDView `json:"view"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return "", fmt.Errorf("decode LCD response: %w", err)
	}
	if wrap.View.Data == "" {
		return "", fmt.Errorf("%w: contract %s", ErrLCDEmptyData, contractAddress)
	}
	return wrap.View.Data, nil
}

// FetchAllRegisteredViews lists every registered view from the hub and
// decodes each bundle into a view.View. Cursor pagination is followed
// transparently. Entries with malformed or missing bundle bytes are logged
// and skipped so a single bad view doesn't block the rest. The second
// return value duplicates len(views) for caller convenience.
func (c *RPCClient) FetchAllRegisteredViews(ctx context.Context) ([]view.View, int, error) {
	if c.lcdURL == "" {
		return nil, 0, ErrLCDNotConfigured
	}

	const pageLimit = 100
	var views []view.View
	nextKey := ""

	for {
		endpoint := fmt.Sprintf("%s/shinzonetwork/view/v1/views?include_data=true&pagination.limit=%d",
			c.lcdURL, pageLimit)
		if nextKey != "" {
			endpoint += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return views, len(views), fmt.Errorf("build LCD request: %w", err)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return views, len(views), fmt.Errorf("LCD GET %s: %w", endpoint, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("close LCD response body: %v", cerr)
		}
		if readErr != nil {
			return views, len(views), fmt.Errorf("read LCD response: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			return views, len(views), fmt.Errorf("%w: %s returned HTTP %d: %s", ErrLCDHTTPStatus, endpoint, resp.StatusCode, string(body))
		}

		var page struct {
			Views      []LCDView `json:"views"`
			Pagination struct {
				NextKey string `json:"next_key"`
			} `json:"pagination"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return views, len(views), fmt.Errorf("decode LCD response: %w", err)
		}

		for _, lv := range page.Views {
			if lv.Data == "" {
				log().Debugf("📡 view %s (contract %s) has no bundle bytes; skipping", lv.Name, lv.ContractAddress)
				continue
			}
			v, err := ProcessViewFromWireFormat(lv.Data)
			if err != nil {
				log().Warnf("📡 failed to decode bundle for view %s (contract %s): %v", lv.Name, lv.ContractAddress, err)
				continue
			}
			log().Debugf("📡 fetched view %s (contract %s)", lv.Name, lv.ContractAddress)
			views = append(views, v)
		}

		nextKey = page.Pagination.NextKey
		if nextKey == "" {
			break
		}
	}

	log().Infof("📡 ShinzoHub query complete: found %d registered views", len(views))
	return views, len(views), nil
}
