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

// NewRPCClient creates a new RPC client for querying Shinzo Hub.
// defraNode is used to persist the last processed page number.
func NewRPCClient(rpcURL string, defraNode *node.Node) *RPCClient {
	return &RPCClient{
		rpcURL:    rpcURL,
		defraNode: defraNode,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// queryResult wraps the GraphQL response for JSON unmarshaling
type queryResult struct {
	Config__LastProcessedPage []struct {
		Page     int `json:"page"`
		PageSize int `json:"pageSize"`
	} `json:"Config__LastProcessedPage"`
}

// getLastProcessedPage reads the last processed page from DefraDB.
func (c *RPCClient) getLastProcessedPage(ctx context.Context) (int, int) {
	if c.defraNode == nil {
		return 0, 5
	}

	query := `query { Config__LastProcessedPage { page pageSize } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	if result.GQL.Errors != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage error: %v", result.GQL.Errors)
		return 0, 5
	}

	// Marshal to JSON then unmarshal to struct
	jsonBytes, err := json.Marshal(result.GQL.Data)
	if err != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage marshal error: %v", err)
		return 0, 5
	}

	var qr queryResult
	if err := json.Unmarshal(jsonBytes, &qr); err != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage unmarshal error: %v", err)
		return 0, 5
	}

	if len(qr.Config__LastProcessedPage) == 0 {
		logger.Sugar.Debugf("📡 getLastProcessedPage: no records")
		return 0, 5
	}

	// Find max page
	maxPage := 0
	for _, rec := range qr.Config__LastProcessedPage {
		if rec.Page > maxPage {
			maxPage = rec.Page
		}
	}
	maxPageSize := 0
	for _, rec := range qr.Config__LastProcessedPage {
		if rec.PageSize > maxPageSize {
			maxPageSize = rec.PageSize
		}
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
	// Save progress after each successful page
	c.saveLastProcessedPage(ctx, lastSuccessfulPage, totalCount)
	logger.Sugar.Debugf("📡 Saved progress: page %d + total_count: %d", lastSuccessfulPage, totalCount)

	logger.Sugar.Infof("📡 ShinzoHub query complete: found %d new views with lenses", len(allViews))
	return allViews, totalCount, nil
}

// fetchRegisteredViewsPage fetches a single page of Registered events with retry logic.
func (c *RPCClient) fetchRegisteredViewsPage(ctx context.Context, page, perPage int) ([]view.View, int, error) {
	// Build the query URL
	// Query: Registered.key EXISTS
	query := url.QueryEscape(`Registered.key EXISTS`)
	endpoint := fmt.Sprintf("%s/tx_search?query=\"%s\"&page=%d&per_page=%d&order_by=\"asc\"",
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

// extractViewFromEvent extracts a View from a Registered event using viewbundle.
func extractViewFromEvent(event Event) (view.View, error) {
	var v view.View

	for _, attr := range event.Attributes {
		switch attr.Key {
		case "view":
			// Process view from wire format
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
		return v, fmt.Errorf("view has no query")
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
