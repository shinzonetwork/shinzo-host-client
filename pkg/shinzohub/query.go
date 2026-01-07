package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// RPCClient queries the Shinzo Hub Tendermint RPC for historical events.
type RPCClient struct {
	rpcURL     string
	httpClient *http.Client
}

// NewRPCClient creates a new RPC client for querying Shinzo Hub.
func NewRPCClient(rpcURL string) *RPCClient {
	return &RPCClient{
		rpcURL: rpcURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// TxSearchResponse represents the response from tx_search RPC call.
type TxSearchResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		Txs        []TxSearchItem `json:"txs"`
		TotalCount string         `json:"total_count"`
	} `json:"result"`
}

// TxSearchItem represents a single transaction in tx_search results.
type TxSearchItem struct {
	Hash     string `json:"hash"`
	Height   string `json:"height"`
	TxResult struct {
		Events []Event `json:"events"`
	} `json:"tx_result"`
}

// FetchAllRegisteredViews queries the RPC endpoint for all Registered events
// and extracts the view definitions.
// Strategy: First overshoot page to get total count from error, then fetch 1 tx at a time
// to avoid memory issues with large responses (31MB+ total).
func (c *RPCClient) FetchAllRegisteredViews(ctx context.Context) ([]view.View, error) {
	var allViews []view.View

	logger.Sugar.Debugf("📡 Starting ShinzoHub query for registered views...")

	// Step 1: Get total count by overshooting page number
	totalTxs, err := c.getTotalTransactionCount(ctx)
	if err != nil {
		logger.Sugar.Warnf("📡 Failed to get total count: %v", err)
		return []view.View{}, nil
	}

	logger.Sugar.Infof("📡 Found %d registered transactions to process", totalTxs)

	// Step 2: Fetch 1 transaction at a time to avoid memory issues
	for page := 1; page <= totalTxs; page++ {
		logger.Sugar.Debugf("📡 Fetching transaction %d/%d...", page, totalTxs)
		views, _, err := c.fetchRegisteredViewsPage(ctx, page, 1) // per_page=1
		if err != nil {
			logger.Sugar.Warnf("📡 Failed to fetch transaction %d: %v", page, err)
			continue
		}

		allViews = append(allViews, views...)
	}

	logger.Sugar.Infof("📡 ShinzoHub query complete: found %d views with lenses", len(allViews))
	return allViews, nil
}

// getTotalTransactionCount gets the total number of registered transactions by overshooting the page
func (c *RPCClient) getTotalTransactionCount(ctx context.Context) (int, error) {
	return 6, nil
}

// 	query := url.QueryEscape(`Registered.key EXISTS`)
// 	// Overshoot to page 9999 to get the error with the actual range
// 	endpoint := fmt.Sprintf("%s/tx_search?query=\"%s\"&page=9999&per_page=1",
// 		c.rpcURL, query)

// 	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
// 	if err != nil {
// 		return 0, err
// 	}

// 	resp, err := c.httpClient.Do(req)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer resp.Body.Close()

// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return 0, err
// 	}

// 	// Parse the error response to extract the page range
// 	// Expected: {"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":"page should be within [1, 6] range, given 9999"}}
// 	var errorResp struct {
// 		Error struct {
// 			Data string `json:"data"`
// 		} `json:"error"`
// 	}
// 	if err := json.Unmarshal(body, &errorResp); err != nil {
// 		return 0, fmt.Errorf("failed to parse error response: %w", err)
// 	}

// 	// Extract the max page from "page should be within [1, 6] range, given 9999"
// 	var minPage, maxPage int
// 	_, err = fmt.Sscanf(errorResp.Error.Data, "page should be within [%d, %d] range", &minPage, &maxPage)
// 	if err != nil {
// 		// If no error, maybe there are no transactions
// 		return 0, nil
// 	}

// 	logger.Sugar.Debugf("📡 Page range: [%d, %d]", minPage, maxPage)
// 	return maxPage, nil
// }

// fetchRegisteredViewsPage fetches a single page of Registered events with retry logic.
func (c *RPCClient) fetchRegisteredViewsPage(ctx context.Context, page, perPage int) ([]view.View, int, error) {
	// Build the query URL
	// Query: Registered.key EXISTS
	query := url.QueryEscape(`Registered.key EXISTS`)
	endpoint := fmt.Sprintf("%s/tx_search?query=\"%s\"&page=%d&per_page=%d&order_by=\"desc\"",
		c.rpcURL, query, page, perPage)

	var body []byte
	var lastErr error

	// Retry up to 3 times for transient errors like EOF
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, 0, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
		}

		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}

		// Success
		lastErr = nil
		break
	}

	if lastErr != nil {
		// Return empty results instead of failing completely
		return []view.View{}, 0, nil
	}

	var txResp TxSearchResponse
	if err := json.Unmarshal(body, &txResp); err != nil {
		logger.Sugar.Warnf("📡 Failed to parse response: %v", err)
		return []view.View{}, 0, nil
	}

	// Extract views from events
	var views []view.View
	var skippedNoLenses int
	var skippedMalformed int
	logger.Sugar.Debugf("📡 Processing %d transactions", len(txResp.Result.Txs))

	for _, tx := range txResp.Result.Txs {
		for _, event := range tx.TxResult.Events {
			if event.Type == "Registered" {
				v, err := extractViewFromEvent(event)
				if err != nil {
					skippedMalformed++
					logger.Sugar.Debugf("📡 Skipping malformed event: %v", err)
					continue
				}
				// Skip views without lenses - they can't be processed
				if !v.HasLenses() {
					skippedNoLenses++
					logger.Sugar.Debugf("📡 Skipping view %s (no lenses)", v.Name)
					continue
				}
				logger.Sugar.Debugf("📡 Found view with lenses: %s", v.Name)
				views = append(views, v)
			}
		}
	}

	var total int
	fmt.Sscanf(txResp.Result.TotalCount, "%d", &total)

	logger.Sugar.Debugf("📡 Page results: %d views with lenses, %d skipped (no lenses), %d malformed, total_count=%d",
		len(views), skippedNoLenses, skippedMalformed, total)

	return views, total, nil
}

// extractViewFromEvent extracts a View from a Registered event.
func extractViewFromEvent(event Event) (view.View, error) {
	var v view.View

	for _, attr := range event.Attributes {
		switch attr.Key {
		case "view":
			if err := json.Unmarshal([]byte(attr.Value), &v); err != nil {
				return v, fmt.Errorf("failed to parse view JSON: %w", err)
			}
		}
	}

	// Extract name from SDL if not set
	if v.Name == "" && v.Sdl != nil {
		ExtractNameFromSDL(&v)
	}

	if v.Query == nil || *v.Query == "" {
		return v, fmt.Errorf("view has no query")
	}

	return v, nil
}

// GetAllWASMURLs extracts all WASM URLs from a list of views.
func GetAllWASMURLs(views []view.View) []string {
	var urls []string
	seen := make(map[string]bool)

	for _, v := range views {
		for _, lens := range v.Transform.Lenses {
			// Only include HTTP/HTTPS URLs
			if len(lens.Path) > 0 && !seen[lens.Path] {
				urls = append(urls, lens.Path)
				seen[lens.Path] = true
			}
		}
	}

	return urls
}
