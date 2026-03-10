package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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
	
	// Cache for view count to avoid repeated schema queries
	cachedViewCount int
	viewCountCached bool
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

// getLastProcessedPage reads the last processed page from DefraDB.
func (c *RPCClient) getLastProcessedPage(ctx context.Context) (int, int) {
	if c.defraNode == nil {
		return 0, 1
	}

	query := `query { Config__LastProcessedPage { page pageSize } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	if result.GQL.Errors != nil {
		logger.Sugar.Debugf("📡 getLastProcessedPage error: %v", result.GQL.Errors)
		return 0, 1
	}

	// Direct type assertion instead of marshal/unmarshal for better performance
	data, ok := result.GQL.Data.(map[string]any)
	if !ok {
		logger.Sugar.Debugf("📡 getLastProcessedPage: invalid data format")
		return 0, 1
	}

	records, ok := data["Config__LastProcessedPage"].([]any)
	if !ok || len(records) == 0 {
		logger.Sugar.Debugf("📡 getLastProcessedPage: no records")
		return 0, 1
	}

	// Extract first record
	record, ok := records[0].(map[string]any)
	if !ok {
		logger.Sugar.Debugf("📡 getLastProcessedPage: invalid record format")
		return 0, 1
	}

	// Extract page and pageSize with type safety
	pageFloat, pageOk := record["page"].(float64)
	pageSizeFloat, pageSizeOk := record["pageSize"].(float64)
	
	if !pageOk || !pageSizeOk {
		logger.Sugar.Debugf("📡 getLastProcessedPage: invalid field types")
		return 0, 1
	}

	return int(pageFloat), int(pageSizeFloat)
}

// saveLastProcessedPage writes the last processed page to DefraDB.
// It deletes any existing records first to avoid duplicate ID errors.
func (c *RPCClient) saveLastProcessedPage(ctx context.Context, page int) {
	if c.defraNode == nil {
		return
	}

	// Calculate dynamic pageSize based on current view count
	// Start with 1, grow as more views are registered
	pageSize := c.calculateDynamicPageSize(ctx)

	// Check if a record exists
	query := `query { Config__LastProcessedPage { _docID page pageSize } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	logger.Sugar.Debugf("📡 saveLastProcessedPage: checking existing records")
	if result.GQL.Errors == nil {
		if data, ok := result.GQL.Data.(map[string]any); ok {
			raw := data["Config__LastProcessedPage"]
			var firstRecord map[string]any
			switch list := raw.(type) {
			case []any:
				if len(list) > 0 {
					firstRecord, _ = list[0].(map[string]any)
				}
			case []map[string]any:
				if len(list) > 0 {
					firstRecord = list[0]
				}
			}
			if firstRecord != nil {
				existingPage := int64(0)
				if p, ok := firstRecord["page"].(int64); ok {
					existingPage = p
				}
				logger.Sugar.Debugf("📡 saveLastProcessedPage: found existing page=%d, new page=%d", existingPage, page)

				// Only update if new page is greater
				if int64(page) <= existingPage {
					logger.Sugar.Debugf("📡 saveLastProcessedPage: skipping, page %d <= existing %d", page, existingPage)
					return
				}

					if docID, ok := r["_docID"].(string); ok {
						updateMutation := fmt.Sprintf(`mutation { update_Config__LastProcessedPage(docID: "%s", input: {page: %d, pageSize: %d}) { _docID page pageSize } }`, docID, page, pageSize)
						updateResult := c.defraNode.DB.ExecRequest(ctx, updateMutation)
						if updateResult.GQL.Errors != nil {
							logger.Sugar.Warnf("📡 Failed to update last processed page: %v", updateResult.GQL.Errors)
						} else {
							logger.Sugar.Debugf("📡 Updated progress: page %d, pageSize: %d", page, pageSize)
						}
						return
					}
					return
				}
			}
		}
	}
	logger.Sugar.Debugf("📡 saveLastProcessedPage: no existing record found, creating new")
	mutation := fmt.Sprintf(`mutation { create_Config__LastProcessedPage(input: {page: %d, pageSize: %d}) { _docID } }`, page, pageSize)
	result = c.defraNode.DB.ExecRequest(ctx, mutation)
	if result.GQL.Errors != nil {
		logger.Sugar.Warnf("📡 Failed to save last processed page: %v", result.GQL.Errors)
	} else {
		logger.Sugar.Infof("📡 Saved last processed page: %d, pageSize: %d", page, pageSize)
	}
}

// calculateDynamicPageSize calculates the optimal page size based on current view count
// Uses cached view count to avoid repeated schema queries
func (c *RPCClient) calculateDynamicPageSize(ctx context.Context) int {
	// Return cached value if available
	if c.viewCountCached {
		return c.pageSizeFromViewCount(c.cachedViewCount)
	}

	// Query for current view count (only once)
	query := `query { _schema { collections { name } } }`
	result := c.defraNode.DB.ExecRequest(ctx, query)
	if result.GQL.Errors != nil {
		logger.Sugar.Debugf("📡 Failed to query schema for view count: %v", result.GQL.Errors)
		c.cachedViewCount = 0
		c.viewCountCached = true
		return 1 // fallback to minimum
	}

	// Count view collections (those that start with view names or contain "View")
	viewCount := 0
	if data, ok := result.GQL.Data.(map[string]any); ok {
		if schema, ok := data["_schema"].(map[string]any); ok {
			if collections, ok := schema["collections"].([]any); ok {
				for _, col := range collections {
					if collection, ok := col.(map[string]any); ok {
						if name, ok := collection["name"].(string); ok {
							// Count views (heuristic: contains "View" or looks like a processed view)
							if strings.Contains(name, "View") || strings.HasPrefix(name, "Filtered") || strings.HasPrefix(name, "NewLog") {
								viewCount++
							}
						}
					}
				}
			}
		}
	}

	// Cache the result
	c.cachedViewCount = viewCount
	c.viewCountCached = true
	
	return c.pageSizeFromViewCount(viewCount)
}

// pageSizeFromViewCount converts view count to page size
func (c *RPCClient) pageSizeFromViewCount(viewCount int) int {
	// Dynamic pageSize calculation:
	// 0-2 views: 1
	// 3-5 views: 3
	// 6-10 views: 5
	// 11+ views: 10
	switch {
	case viewCount <= 2:
		return 1
	case viewCount <= 5:
		return 3
	case viewCount <= 10:
		return 5
	default:
		return 10
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
// Strategy: Fetch 1 tx at a time, counting up until we get an error or empty results.
// This avoids memory issues with large responses (31MB+ total).
// On restart, skips already-processed pages using persisted state.
func (c *RPCClient) FetchAllRegisteredViews(ctx context.Context) ([]view.View, error) {
	// Pre-allocate with estimated capacity to reduce slice growths
	// Estimate: ~5 views per page, max 50 pages initially
	allViews := make([]view.View, 0, 250)

	// Start from last processed page + 1 to skip already-downloaded views
	lastPage, pageSize := c.getLastProcessedPage(ctx)
	startPage := lastPage + 1
	logger.Sugar.Debugf("📡 getLastProcessedPage result: %d, pageSize: %d", startPage, pageSize)
	if startPage > 1 {
		logger.Sugar.Infof("📡 Resuming from page %d (skipping %d already processed)", startPage, startPage-1)
	} else {
		logger.Sugar.Debugf("📡 Starting ShinzoHub query for registered views...")
	}

	lastSuccessfulPage := startPage - 1
	saveCounter := 0 // Batch saves every 5 pages

	// Fetch 1 transaction at a time until we hit an error or empty page
	for page := startPage; ; page++ {
		logger.Sugar.Debugf("📡 Fetching transaction page %d with pageSize=%d...", page, pageSize)
		views, total, err := c.fetchRegisteredViewsPage(ctx, page, pageSize) // use dynamic pageSize
		if err != nil {
			// Error likely means we've gone past the last page
			logger.Sugar.Debugf("📡 Stopping at page %d: %v", page, err)
			break
		}

		// Batch append views to reduce slice growth operations
		allViews = append(allViews, views...)
		lastSuccessfulPage = page
		saveCounter++

		// Batch save progress every 5 pages to reduce database writes
		if saveCounter >= 5 {
			c.saveLastProcessedPage(ctx, lastSuccessfulPage)
			logger.Sugar.Debugf("📡 Batch saved progress: page %d", lastSuccessfulPage)
			saveCounter = 0
		}

		// Stop if we've processed all transactions
		if page >= total {
			logger.Sugar.Debugf("📡 Reached end of transactions (page %d of %d)", page, total)
			break
		}
	}

	// Final save if needed
	if saveCounter > 0 {
		c.saveLastProcessedPage(ctx, lastSuccessfulPage)
		logger.Sugar.Debugf("📡 Final saved progress: page %d", lastSuccessfulPage)
	}

	logger.Sugar.Infof("📡 ShinzoHub query complete: found %d new views with lenses", len(allViews))
	return allViews, nil
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
			// Use viewbundle to process the wire format
			processor := NewViewProcessor(nil) // We don't need defraNode for extraction
			newView, err := processor.ProcessViewFromWire(context.Background(), attr.Value)
			if err != nil {
				return v, fmt.Errorf("failed to process view from wire: %w", err)
			}
			v = *newView
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
