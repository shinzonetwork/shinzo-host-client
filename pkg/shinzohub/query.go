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
	"github.com/sourcenetwork/defradb/node"
)

// RPCClient queries the Shinzo Hub Tendermint RPC for historical events.
type RPCClient struct {
	rpcURL     string
	defraNode  *node.Node
	httpClient *http.Client
}

// defaultHTTPClientTimeout is the default timeout for HTTP client requests.
const defaultHTTPClientTimeout = 30 * time.Second

// NewRPCClient creates a new RPC client for querying Shinzo Hub.
// defraNode is used to persist the last processed page number.
func NewRPCClient(rpcURL string, defraNode *node.Node) *RPCClient {
	return &RPCClient{
		rpcURL:    rpcURL,
		defraNode: defraNode,
		httpClient: &http.Client{
			Timeout: defaultHTTPClientTimeout,
		},
	}
}

// queryResult wraps the GraphQL response for JSON unmarshaling.
type queryResult struct {
	ConfigLastProcessedPage []struct {
		Page     int `json:"page"`
		PageSize int `json:"pageSize"`
	} `json:"ConfigLastProcessedPage"`
}

// getLastProcessedPage reads the last processed page from DefraDB.
func (c *RPCClient) getLastProcessedPage(ctx context.Context) (int, int) {
	if c.defraNode == nil {
		return 0, defaultPageSize
	}

	query := `query { Config__LastProcessedPage { page pageSize } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	if result.GQL.Errors != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage error: %v", result.GQL.Errors)
		return 0, defaultPageSize
	}

	// Marshal to JSON then unmarshal to struct
	jsonBytes, err := json.Marshal(result.GQL.Data)
	if err != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage marshal error: %v", err)
		return 0, defaultPageSize
	}

	var qr queryResult
	if err := json.Unmarshal(jsonBytes, &qr); err != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage unmarshal error: %v", err)
		return 0, defaultPageSize
	}

	if len(qr.ConfigLastProcessedPage) == 0 {
		logger.Sugar.Debugf("📡 getLastProcessedPage: no records")
		return 0, defaultPageSize
	}

	// Find max page
	maxPage := 0
	for _, rec := range qr.ConfigLastProcessedPage {
		if rec.Page > maxPage {
			maxPage = rec.Page
		}
	}
	maxPageSize := 0
	for _, rec := range qr.ConfigLastProcessedPage {
		if rec.PageSize > maxPageSize {
			maxPageSize = rec.PageSize
		}
	}
	// Guard against persisted records that stored pageSize=0 (an older bug
	// saved totalCount here instead of pageSize; 0 makes tx_search return
	// no rows and the whole query loop finds zero views). Fall back to the
	// default so affected hosts self-heal on next restart.
	if maxPageSize <= 0 {
		maxPageSize = 5
	}
	logger.Sugar.Debugf("📡 getLastProcessedPage: page=%d, pageSize=%d", maxPage, maxPageSize)
	return maxPage, maxPageSize
}

// saveLastProcessedPage writes the last processed page to DefraDB.
// It deletes any existing records first to avoid duplicate ID errors.
func (c *RPCClient) saveLastProcessedPage(ctx context.Context, page, pageSize int) {
	if c.defraNode == nil {
		return
	}

	// Delete all existing records
	query := `query { Config__LastProcessedPage { _docID } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	if result.GQL.Errors == nil {
		if data, ok := result.GQL.Data.(map[string]any); ok {
			if records, ok := data["Config__LastProcessedPage"].([]any); ok {
				for _, record := range records {
					if rec, ok := record.(map[string]any); ok {
						if docID, ok := rec["_docID"].(string); ok {
							deleteMutation := fmt.Sprintf(`mutation { delete_Config__LastProcessedPage(docID: "%s") { _docID } }`, docID)
							c.defraNode.DB.ExecRequest(ctx, deleteMutation)
						}
					}
				}
			}
		}
	}

	// Create new record with final page
	mutation := fmt.Sprintf(`mutation { create_Config__LastProcessedPage(input: {page: %d, pageSize: %d}) { _docID } }`, page, pageSize)
	result = c.defraNode.DB.ExecRequest(ctx, mutation)
	if result.GQL.Errors != nil {
		logger.Sugar.Warnf("📡 Failed to save last processed page: %v", result.GQL.Errors)
	} else {
		logger.Sugar.Infof("📡 Saved last processed page: %d", page)
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

// FetchAllRegisteredViews fetches all Registered events from the RPC endpoint one tx at a time,
// resuming from the last processed page on restart.
func (c *RPCClient) FetchAllRegisteredViews(ctx context.Context) ([]view.View, int, error) {
	var allViews []view.View

	// Start from last processed page + 1 to skip already-downloaded views
	startPage, pageSize := c.getLastProcessedPage(ctx)
	startPage++
	logger.Sugar.Debugf("📡 getLastProcessedPage result: %d", startPage)
	if startPage > 1 {
		logger.Sugar.Infof("📡 Resuming from page %d (skipping %d already processed)", startPage, startPage-1)
	} else {
		logger.Sugar.Debugf("📡 Starting ShinzoHub query for registered views...")
	}

	lastSuccessfulPage := startPage - 1
	totalCount := 0

	// Fetch 1 transaction at a time until we hit an error or empty page
	for page := startPage; ; page++ {
		logger.Sugar.Debugf("📡 Fetching transaction page %d...", page)
		views, total, err := c.fetchRegisteredViewsPage(ctx, page, pageSize) // per_page=5
		if err != nil {
			// Error likely means we've gone past the last page
			logger.Sugar.Debugf("📡 Stopping at page %d: %v", page, err)
			break
		}

		allViews = append(allViews, views...)
		lastSuccessfulPage = page
		totalCount = total

		// Stop if we've processed all transactions
		if page*pageSize >= total {
			logger.Sugar.Debugf("📡 Reached end of transactions (page %d of %d)", page, total)
			break
		}
	}
	// Save progress after each successful page. Persist the actual page size
	// used in the queries, not the server-reported total_count — the latter
	// used to sneak into this slot and land pageSize=0 in the record, which
	// made the next startup query tx_search with per_page=0 and silently
	// observe zero rows.
	c.saveLastProcessedPage(ctx, lastSuccessfulPage, pageSize)
	logger.Sugar.Debugf("📡 Saved progress: page %d (pageSize=%d, total_count=%d)", lastSuccessfulPage, pageSize, totalCount)

	logger.Sugar.Infof("📡 ShinzoHub query complete: found %d new views with lenses", len(allViews))
	return allViews, totalCount, nil
}

// fetchRegisteredViewsPage fetches a single page of Registered events with retry logic.
func (c *RPCClient) fetchRegisteredViewsPage(ctx context.Context, page, perPage int) ([]view.View, int, error) {
	query := url.QueryEscape(`ViewRegistered.view_address EXISTS`)
	endpoint := fmt.Sprintf("%s/tx_search?query=\"%s\"&page=%d&per_page=%d&order_by=\"asc\"",
		c.rpcURL, query, page, perPage)

	body, err := c.fetchWithRetry(ctx, endpoint)
	if err != nil {
		return []view.View{}, 0, err
	}

	var txResp TxSearchResponse
	if err := json.Unmarshal(body, &txResp); err != nil {
		logger.Sugar.Warnf("📡 Failed to parse response: %v", err)
		return []view.View{}, 0, nil
	}

	return parseViewsFromTxResponse(txResp)
}

func (c *RPCClient) fetchWithRetry(ctx context.Context, endpoint string) ([]byte, error) {
	var lastErr error

	for attempt := range 3 {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d %s: %w", resp.StatusCode, string(respBody), ErrHTTPErrorResponse)
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}

		return body, nil
	}

	return nil, lastErr
}

// parseViewsFromTxResponse is self explainatory.
func parseViewsFromTxResponse(txResp TxSearchResponse) ([]view.View, int, error) {
	var views []view.View
	var skippedNoLenses, skippedMalformed int

	logger.Sugar.Debugf("📡 Processing %d transactions", len(txResp.Result.Txs))

	for _, tx := range txResp.Result.Txs {
		for _, event := range tx.TxResult.Events {
			if event.Type != "ViewRegistered" {
				continue
			}
			v, err := extractViewFromEvent(event)
			if err != nil {
				skippedMalformed++
				logger.Sugar.Debugf("📡 Skipping malformed event: %v", err)
				continue
			}
			logger.Sugar.Debugf("📡 Found view with lenses: %s", v.Name)
			views = append(views, v)
		}
	}

	var total int
	if _, err := fmt.Sscanf(txResp.Result.TotalCount, "%d", &total); err != nil {
		return nil, 0, fmt.Errorf("failed to parse total count %q: %w", txResp.Result.TotalCount, err)
	}

	logger.Sugar.Debugf("📡 Page results: %d views with lenses, %d skipped (no lenses), %d malformed, total_count=%d",
		len(views), skippedNoLenses, skippedMalformed, total)

	return views, total, nil
}

// extractViewFromEvent extracts a View from a Registered event using viewbundle.
func extractViewFromEvent(event Event) (view.View, error) {
	var v view.View

	for _, attr := range event.Attributes {
		if attr.Key == "data" {
			newView, err := ProcessViewFromWireFormat(attr.Value)
			if err != nil {
				return v, fmt.Errorf("failed to process view from wire: %w", err)
			}
			v = newView
		}
	}

	// Extract name from SDL if not set (should already be done by ProcessViewFromWire)
	if v.Name == "" && v.Data.Sdl != "" {
		v.ExtractNameFromSDL()
	}

	if v.Data.Query == "" {
		return v, ErrViewHasNoQuery
	}

	return v, nil
}

// GetAllWASMURLs extracts all WASM URLs from a list of views.
func GetAllWASMURLs(views []view.View) []string {
	var urls []string
	seen := make(map[string]bool)

	for _, v := range views {
		for _, lens := range v.Data.Transform.Lenses {
			// Only include HTTP/HTTPS URLs
			if len(lens.Path) > 0 && !seen[lens.Path] {
				urls = append(urls, lens.Path)
				seen[lens.Path] = true
			}
		}
	}

	return urls
}
