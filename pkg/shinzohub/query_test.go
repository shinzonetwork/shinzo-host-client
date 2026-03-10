package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewRPCClient
// ---------------------------------------------------------------------------

func TestNewRPCClient(t *testing.T) {
	t.Run("sets fields correctly with nil defraNode", func(t *testing.T) {
		c := NewRPCClient("http://localhost:26657", nil)
		require.Equal(t, "http://localhost:26657", c.rpcURL)
		require.Nil(t, c.defraNode)
		require.NotNil(t, c.httpClient)
		require.Equal(t, 30*time.Second, c.httpClient.Timeout)
	})

	t.Run("sets fields correctly with real defraNode", func(t *testing.T) {
		ctx := context.Background()
		defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
		require.NoError(t, err)
		defer defraNode.Close(ctx)

		c := NewRPCClient("http://example.com:26657", defraNode)
		require.Equal(t, "http://example.com:26657", c.rpcURL)
		require.NotNil(t, c.defraNode)
		require.NotNil(t, c.httpClient)
	})
}

// ---------------------------------------------------------------------------
// extractViewFromEvent
// ---------------------------------------------------------------------------

func TestExtractViewFromEvent(t *testing.T) {
	tests := []struct {
		name      string
		event     Event
		wantName  string
		wantErr   string
	}{
		{
			name: "valid event with view JSON",
			event: func() Event {
				q := "Ethereum__Mainnet__Log {address topics data transactionHash blockNumber}"
				sdl := "type TestView @materialized(if: false) {transactionHash: String}"
				v := view.View{
					Query: &q,
					Sdl:   &sdl,
					Transform: models.Transform{
						Lenses: []models.Lens{{Label: "l1", Path: "http://example.com/lens.wasm"}},
					},
				}
				viewJSON, _ := json.Marshal(v)
				return Event{
					Type: "Registered",
					Attributes: []EventAttribute{
						{Key: "view", Value: string(viewJSON)},
					},
				}
			}(),
			wantName: "TestView",
		},
		{
			name: "valid event with name extracted from SDL",
			event: func() Event {
				q := "Log {address}"
				sdl := "type ExtractedName @materialized(if: true) {address: String}"
				v := view.View{
					Query: &q,
					Sdl:   &sdl,
				}
				viewJSON, _ := json.Marshal(v)
				return Event{
					Type: "Registered",
					Attributes: []EventAttribute{
						{Key: "view", Value: string(viewJSON)},
					},
				}
			}(),
			wantName: "ExtractedName",
		},
		{
			name: "missing view attribute",
			event: Event{
				Type: "Registered",
				Attributes: []EventAttribute{
					{Key: "key", Value: "some-key"},
					{Key: "creator", Value: "some-creator"},
				},
			},
			wantErr: "view has no query",
		},
		{
			name: "invalid JSON in view attribute",
			event: Event{
				Type: "Registered",
				Attributes: []EventAttribute{
					{Key: "view", Value: "this is not json{{{"},
				},
			},
			wantErr: "failed to parse view JSON",
		},
		{
			name: "view with empty query",
			event: func() Event {
				empty := ""
				v := view.View{
					Query: &empty,
				}
				viewJSON, _ := json.Marshal(v)
				return Event{
					Type: "Registered",
					Attributes: []EventAttribute{
						{Key: "view", Value: string(viewJSON)},
					},
				}
			}(),
			wantErr: "view has no query",
		},
		{
			name: "view with nil query",
			event: func() Event {
				v := view.View{}
				viewJSON, _ := json.Marshal(v)
				return Event{
					Type: "Registered",
					Attributes: []EventAttribute{
						{Key: "view", Value: string(viewJSON)},
					},
				}
			}(),
			wantErr: "view has no query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := extractViewFromEvent(tt.event)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantName, v.Name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetAllWASMURLs
// ---------------------------------------------------------------------------

func TestGetAllWASMURLs(t *testing.T) {
	tests := []struct {
		name     string
		views    []view.View
		expected []string
	}{
		{
			name:     "empty views",
			views:    []view.View{},
			expected: nil,
		},
		{
			name:     "nil views",
			views:    nil,
			expected: nil,
		},
		{
			name: "views with lenses having HTTP URLs",
			views: []view.View{
				{
					Transform: models.Transform{
						Lenses: []models.Lens{
							{Label: "l1", Path: "http://example.com/lens1.wasm"},
							{Label: "l2", Path: "https://example.com/lens2.wasm"},
						},
					},
				},
			},
			expected: []string{
				"http://example.com/lens1.wasm",
				"https://example.com/lens2.wasm",
			},
		},
		{
			name: "deduplication across views",
			views: []view.View{
				{
					Transform: models.Transform{
						Lenses: []models.Lens{
							{Label: "l1", Path: "http://example.com/lens.wasm"},
						},
					},
				},
				{
					Transform: models.Transform{
						Lenses: []models.Lens{
							{Label: "l2", Path: "http://example.com/lens.wasm"},
							{Label: "l3", Path: "http://example.com/lens2.wasm"},
						},
					},
				},
			},
			expected: []string{
				"http://example.com/lens.wasm",
				"http://example.com/lens2.wasm",
			},
		},
		{
			name: "views without lenses",
			views: []view.View{
				{
					Transform: models.Transform{
						Lenses: nil,
					},
				},
				{
					Transform: models.Transform{
						Lenses: []models.Lens{},
					},
				},
			},
			expected: nil,
		},
		{
			name: "lens with empty path is skipped",
			views: []view.View{
				{
					Transform: models.Transform{
						Lenses: []models.Lens{
							{Label: "empty", Path: ""},
							{Label: "valid", Path: "http://example.com/valid.wasm"},
						},
					},
				},
			},
			expected: []string{
				"http://example.com/valid.wasm",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAllWASMURLs(tt.views)
			require.Equal(t, tt.expected, result)
		})
	}
}

// ---------------------------------------------------------------------------
// fetchRegisteredViewsPage – uses httptest to mock the RPC endpoint
// ---------------------------------------------------------------------------

func TestFetchRegisteredViewsPage(t *testing.T) {
	t.Run("success with valid response", func(t *testing.T) {
		q := "Ethereum__Mainnet__Log {address topics data transactionHash blockNumber}"
		sdl := "type TestView @materialized(if: false) {transactionHash: String}"
		v := view.View{
			Query: &q,
			Sdl:   &sdl,
		}
		viewJSON, err := json.Marshal(v)
		require.NoError(t, err)

		resp := TxSearchResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				Txs        []TxSearchItem `json:"txs"`
				TotalCount string         `json:"total_count"`
			}{
				TotalCount: "3",
				Txs: []TxSearchItem{
					{
						Hash:   "AABB",
						Height: "100",
						TxResult: struct {
							Events []Event `json:"events"`
						}{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "view", Value: string(viewJSON)},
									},
								},
							},
						},
					},
				},
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.NoError(t, err)
		require.Equal(t, 3, total)
		require.Len(t, views, 1)
		require.Equal(t, "TestView", views[0].Name)
	})

	t.Run("HTTP error returns error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal server error"))
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "HTTP 500")
		require.Equal(t, 0, total)
		require.Nil(t, views)
	})

	t.Run("invalid JSON response returns empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("this is not valid json{{{"))
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.NoError(t, err)
		require.Equal(t, 0, total)
		require.Empty(t, views)
	})

	t.Run("empty txs returns empty views", func(t *testing.T) {
		resp := TxSearchResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				Txs        []TxSearchItem `json:"txs"`
				TotalCount string         `json:"total_count"`
			}{
				TotalCount: "0",
				Txs:        []TxSearchItem{},
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.NoError(t, err)
		require.Equal(t, 0, total)
		require.Empty(t, views)
	})

	t.Run("malformed view event is skipped", func(t *testing.T) {
		resp := TxSearchResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				Txs        []TxSearchItem `json:"txs"`
				TotalCount string         `json:"total_count"`
			}{
				TotalCount: "1",
				Txs: []TxSearchItem{
					{
						Hash:   "CCDD",
						Height: "200",
						TxResult: struct {
							Events []Event `json:"events"`
						}{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "view", Value: "not-json"},
									},
								},
							},
						},
					},
				},
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.NoError(t, err)
		require.Equal(t, 1, total)
		require.Empty(t, views) // malformed event skipped
	})

	t.Run("non-Registered events are ignored", func(t *testing.T) {
		resp := TxSearchResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result: struct {
				Txs        []TxSearchItem `json:"txs"`
				TotalCount string         `json:"total_count"`
			}{
				TotalCount: "1",
				Txs: []TxSearchItem{
					{
						Hash:   "EEFF",
						Height: "300",
						TxResult: struct {
							Events []Event `json:"events"`
						}{
							Events: []Event{
								{
									Type: "SomeOtherEvent",
									Attributes: []EventAttribute{
										{Key: "key", Value: "value"},
									},
								},
							},
						},
					},
				},
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := &RPCClient{
			rpcURL:     server.URL,
			httpClient: &http.Client{Timeout: 5 * time.Second},
		}

		views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
		require.NoError(t, err)
		require.Equal(t, 1, total)
		require.Empty(t, views)
	})
}

// ---------------------------------------------------------------------------
// getLastProcessedPage & saveLastProcessedPage – require DefraDB
// ---------------------------------------------------------------------------

func TestGetLastProcessedPage_NilDefraNode(t *testing.T) {
	client := &RPCClient{defraNode: nil}
	page := client.getLastProcessedPage(context.Background())
	require.Equal(t, 0, page)
}

func TestSaveLastProcessedPage_NilDefraNode(t *testing.T) {
	client := &RPCClient{defraNode: nil}
	// Should not panic
	client.saveLastProcessedPage(context.Background(), 42)
}

func TestGetAndSaveLastProcessedPage_WithDefraDB(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Add the Config__LastProcessedPage schema
	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	client := &RPCClient{defraNode: defraNode}

	// Initially should be 0 (no records)
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 0, page)

	// Save page 5
	client.saveLastProcessedPage(ctx, 5)

	// Now getLastProcessedPage should return 5
	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 5, page)

	// Save a higher page
	client.saveLastProcessedPage(ctx, 10)

	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 10, page)

	// Saving a lower page should be a no-op
	client.saveLastProcessedPage(ctx, 3)

	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 10, page)
}

func TestGetLastProcessedPage_NoSchema(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Do NOT add the schema - query should fail gracefully and return 0
	client := &RPCClient{defraNode: defraNode}
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 0, page)
}

// ---------------------------------------------------------------------------
// FetchAllRegisteredViews – uses httptest + real DefraDB
// ---------------------------------------------------------------------------

func TestFetchAllRegisteredViews(t *testing.T) {
	t.Run("fetches views across multiple pages", func(t *testing.T) {
		ctx := context.Background()

		defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
		require.NoError(t, err)
		defer defraNode.Close(ctx)

		// Add the config schema for page persistence
		configSchema := `type Config__LastProcessedPage { page: Int }`
		_, err = defraNode.DB.AddSchema(ctx, configSchema)
		require.NoError(t, err)

		// Build view JSON payloads
		q1 := "Ethereum__Mainnet__Log {address}"
		sdl1 := "type View1 @materialized(if: false) {address: String}"
		v1JSON, _ := json.Marshal(view.View{Query: &q1, Sdl: &sdl1})

		q2 := "Ethereum__Mainnet__Log {topics}"
		sdl2 := "type View2 @materialized(if: false) {topics: String}"
		v2JSON, _ := json.Marshal(view.View{Query: &q2, Sdl: &sdl2})

		requestCount := 0

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			pageParam := r.URL.Query().Get("page")

			var resp TxSearchResponse
			switch pageParam {
			case "":
				// page 1 (first call)
				resp = makeTxSearchResponse("2", []Event{
					{Type: "Registered", Attributes: []EventAttribute{{Key: "view", Value: string(v1JSON)}}},
				})
			default:
				// Determine which page based on the request URL
				// The URL format is: /tx_search?query=...&page=N&per_page=5&order_by=...
				// We need to parse the page from the encoded URL
				if requestCount <= 1 {
					resp = makeTxSearchResponse("2", []Event{
						{Type: "Registered", Attributes: []EventAttribute{{Key: "view", Value: string(v1JSON)}}},
					})
				} else if requestCount == 2 {
					resp = makeTxSearchResponse("2", []Event{
						{Type: "Registered", Attributes: []EventAttribute{{Key: "view", Value: string(v2JSON)}}},
					})
				} else {
					// Return empty to stop pagination
					resp = makeTxSearchResponse("2", nil)
				}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := NewRPCClient(server.URL, defraNode)

		views, err := client.FetchAllRegisteredViews(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(views), 1)
	})

	t.Run("fetches with no results", func(t *testing.T) {
		ctx := context.Background()

		defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
		require.NoError(t, err)
		defer defraNode.Close(ctx)

		configSchema := `type Config__LastProcessedPage { page: Int }`
		_, err = defraNode.DB.AddSchema(ctx, configSchema)
		require.NoError(t, err)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := makeTxSearchResponse("0", nil)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := NewRPCClient(server.URL, defraNode)

		views, err := client.FetchAllRegisteredViews(ctx)
		require.NoError(t, err)
		require.Empty(t, views)
	})

	t.Run("resumes from last processed page", func(t *testing.T) {
		ctx := context.Background()

		defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
		require.NoError(t, err)
		defer defraNode.Close(ctx)

		configSchema := `type Config__LastProcessedPage { page: Int }`
		_, err = defraNode.DB.AddSchema(ctx, configSchema)
		require.NoError(t, err)

		// Pre-save page 2 so it resumes from page 3
		rpcClient := &RPCClient{defraNode: defraNode}
		rpcClient.saveLastProcessedPage(ctx, 2)

		var requestedPages []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestedPages = append(requestedPages, r.URL.RawQuery)
			// Return empty to stop immediately
			resp := makeTxSearchResponse("2", nil)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		client := NewRPCClient(server.URL, defraNode)

		views, err := client.FetchAllRegisteredViews(ctx)
		require.NoError(t, err)
		require.Empty(t, views)

		// Verify it started from page 3 (last processed + 1)
		require.GreaterOrEqual(t, len(requestedPages), 1)
		require.Contains(t, requestedPages[0], "page=3")
	})

	t.Run("handles server error gracefully", func(t *testing.T) {
		ctx := context.Background()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("error"))
		}))
		defer server.Close()

		client := NewRPCClient(server.URL, nil)

		views, err := client.FetchAllRegisteredViews(ctx)
		require.NoError(t, err) // FetchAllRegisteredViews doesn't propagate errors, it just stops
		require.Empty(t, views)
	})
}

// makeTxSearchResponse builds a TxSearchResponse with the given total_count and events on a single tx.
func makeTxSearchResponse(totalCount string, events []Event) TxSearchResponse {
	var txs []TxSearchItem
	if len(events) > 0 {
		txs = []TxSearchItem{
			{
				Hash:   "AABB",
				Height: "100",
				TxResult: struct {
					Events []Event `json:"events"`
				}{
					Events: events,
				},
			},
		}
	}
	return TxSearchResponse{
		JSONRPC: "2.0",
		ID:      1,
		Result: struct {
			Txs        []TxSearchItem `json:"txs"`
			TotalCount string         `json:"total_count"`
		}{
			TotalCount: totalCount,
			Txs:        txs,
		},
	}
}

// ---------------------------------------------------------------------------
// ProcessEntityRegisteredEvent – additional coverage
// ---------------------------------------------------------------------------

func TestProcessEntityRegisteredEvent_NewIndexer(t *testing.T) {
	manager := NewMockIndexerManager()
	handler := NewEntityRegistrationHandler(manager)

	event := EntityRegisteredEvent{
		Key:    "test-key",
		Owner:  "test-owner",
		DID:    "test-did",
		Pid:    "test-pid",
		Entity: "\x01",
	}

	err := handler.ProcessEntityRegisteredEvent(context.Background(), event)
	require.NoError(t, err)

	indexer, exists := manager.GetIndexer("test-key")
	require.True(t, exists)
	require.Equal(t, "test-owner", indexer.Owner)
	require.Equal(t, "test-did", indexer.DID)
	require.Equal(t, "test-pid", indexer.Pid)
	require.True(t, indexer.Active)
}

func TestProcessEntityRegisteredEvent_ExistingIndexerUpdate(t *testing.T) {
	manager := NewMockIndexerManager()
	handler := NewEntityRegistrationHandler(manager)

	// Add an existing indexer
	_ = manager.AddIndexer(context.Background(), "existing-key", "old-owner", "old-did", "old-pid")

	event := EntityRegisteredEvent{
		Key:    "existing-key",
		Owner:  "new-owner",
		DID:    "new-did",
		Pid:    "new-pid",
		Entity: "\x02",
	}

	err := handler.ProcessEntityRegisteredEvent(context.Background(), event)
	require.NoError(t, err)

	indexer, exists := manager.GetIndexer("existing-key")
	require.True(t, exists)
	require.Equal(t, "new-owner", indexer.Owner)
	require.Equal(t, "new-did", indexer.DID)
	require.Equal(t, "new-pid", indexer.Pid)
}

// ErrorIndexerManager is a mock that returns errors on AddIndexer.
type ErrorIndexerManager struct {
	indexers map[string]Indexer
}

func NewErrorIndexerManager() *ErrorIndexerManager {
	return &ErrorIndexerManager{indexers: make(map[string]Indexer)}
}

func (m *ErrorIndexerManager) AddIndexer(ctx context.Context, key, owner, did, pid string) error {
	return fmt.Errorf("simulated add error")
}

func (m *ErrorIndexerManager) GetIndexer(key string) (Indexer, bool) {
	idx, ok := m.indexers[key]
	return idx, ok
}

func (m *ErrorIndexerManager) RemoveIndexer(key string) error {
	delete(m.indexers, key)
	return nil
}

func TestProcessEntityRegisteredEvent_AddIndexerError(t *testing.T) {
	manager := NewErrorIndexerManager()
	handler := NewEntityRegistrationHandler(manager)

	event := EntityRegisteredEvent{
		Key:    "fail-key",
		Owner:  "owner",
		DID:    "did",
		Pid:    "pid",
		Entity: "\x01",
	}

	err := handler.ProcessEntityRegisteredEvent(context.Background(), event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to add indexer")
	require.Contains(t, err.Error(), "simulated add error")
}

// ---------------------------------------------------------------------------
// fetchRegisteredViewsPage – retry logic: server closes connection on first
// two attempts then succeeds on the third
// ---------------------------------------------------------------------------

func TestFetchRegisteredViewsPage_RetryOnTransientError(t *testing.T) {
	attempt := 0
	q := "Ethereum__Mainnet__Log {address}"
	sdl := "type RetryView @materialized(if: false) {address: String}"
	v := view.View{Query: &q, Sdl: &sdl}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt < 3 {
			// Simulate transient failure by hijacking the connection and closing it
			hj, ok := w.(http.Hijacker)
			if !ok {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}
		// Third attempt succeeds
		resp := makeTxSearchResponse("1", []Event{
			{Type: "Registered", Attributes: []EventAttribute{{Key: "view", Value: string(viewJSON)}}},
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := &RPCClient{
		rpcURL:     server.URL,
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}

	views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, views, 1)
	require.Equal(t, "RetryView", views[0].Name)
}

func TestFetchRegisteredViewsPage_AllRetriesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always fail by hijacking and closing
		hj, ok := w.(http.Hijacker)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer server.Close()

	client := &RPCClient{
		rpcURL:     server.URL,
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}

	views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
	// After all retries fail, returns empty results without error
	require.NoError(t, err)
	require.Equal(t, 0, total)
	require.Empty(t, views)
}

func TestFetchRegisteredViewsPage_InvalidURL(t *testing.T) {
	client := &RPCClient{
		rpcURL:     "://invalid-url",
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}

	views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
	require.Error(t, err)
	require.Equal(t, 0, total)
	require.Nil(t, views)
}

// ---------------------------------------------------------------------------
// saveLastProcessedPage – saving equal page is a no-op
// ---------------------------------------------------------------------------

func TestSaveLastProcessedPage_EqualPage(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	client := &RPCClient{defraNode: defraNode}

	// Save page 5
	client.saveLastProcessedPage(ctx, 5)
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 5, page)

	// Saving equal page should be a no-op (page <= existing)
	client.saveLastProcessedPage(ctx, 5)
	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 5, page)
}

// ---------------------------------------------------------------------------
// saveLastProcessedPage – no schema means create falls back
// ---------------------------------------------------------------------------

func TestSaveLastProcessedPage_NoSchema(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Do NOT add the schema - save should not panic, just log warnings
	client := &RPCClient{defraNode: defraNode}
	client.saveLastProcessedPage(ctx, 42) // should not panic
}

// ---------------------------------------------------------------------------
// getLastProcessedPage – multiple records returns max
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// extractViewFromEvent - view with Name already set (ExtractNameFromSDL skipped)
// ---------------------------------------------------------------------------

func TestExtractViewFromEvent_NameAlreadySet(t *testing.T) {
	q := "Ethereum__Mainnet__Log {address}"
	sdl := "type SomeOtherName @materialized(if: true) {address: String}"
	v := view.View{
		Name:  "PresetName",
		Query: &q,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	event := Event{
		Type: "Registered",
		Attributes: []EventAttribute{
			{Key: "view", Value: string(viewJSON)},
		},
	}

	result, err := extractViewFromEvent(event)
	require.NoError(t, err)
	// Name was already set, so ExtractNameFromSDL should NOT be called
	require.Equal(t, "PresetName", result.Name)
}

// ---------------------------------------------------------------------------
// extractViewFromEvent - view with no name and no SDL
// ---------------------------------------------------------------------------

func TestExtractViewFromEvent_NoNameNoSDL(t *testing.T) {
	q := "Log {address}"
	v := view.View{
		Query: &q,
		Sdl:   nil,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	event := Event{
		Type: "Registered",
		Attributes: []EventAttribute{
			{Key: "view", Value: string(viewJSON)},
		},
	}

	result, err := extractViewFromEvent(event)
	require.NoError(t, err)
	// Name should be empty since no SDL to extract from
	require.Equal(t, "", result.Name)
}

// ---------------------------------------------------------------------------
// extractViewFromEvent - view with no name but has SDL (name extracted)
// ---------------------------------------------------------------------------

func TestExtractViewFromEvent_NoNameWithSDL(t *testing.T) {
	q := "Log {address}"
	sdl := "type ExtractedViewName { address: String }"
	v := view.View{
		Query: &q,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	event := Event{
		Type: "Registered",
		Attributes: []EventAttribute{
			{Key: "view", Value: string(viewJSON)},
		},
	}

	result, err := extractViewFromEvent(event)
	require.NoError(t, err)
	// Name should be extracted from SDL
	require.Equal(t, "ExtractedViewName", result.Name)
}

// ---------------------------------------------------------------------------
// extractViewFromEvent - non-view attributes are ignored
// ---------------------------------------------------------------------------

func TestExtractViewFromEvent_OtherAttributes(t *testing.T) {
	q := "Log {address}"
	sdl := "type OtherAttrView { address: String }"
	v := view.View{
		Query: &q,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	event := Event{
		Type: "Registered",
		Attributes: []EventAttribute{
			{Key: "key", Value: "some-key"},
			{Key: "creator", Value: "some-creator"},
			{Key: "view", Value: string(viewJSON)},
			{Key: "extra", Value: "ignored"},
		},
	}

	result, err := extractViewFromEvent(event)
	require.NoError(t, err)
	require.Equal(t, "OtherAttrView", result.Name)
}

// ---------------------------------------------------------------------------
// GetAllWASMURLs - views with base64 paths included
// ---------------------------------------------------------------------------

func TestGetAllWASMURLs_IncludesBase64Paths(t *testing.T) {
	views := []view.View{
		{
			Transform: models.Transform{
				Lenses: []models.Lens{
					{Label: "l1", Path: "AGFzbQEAAAA="},
					{Label: "l2", Path: "file:///local/path.wasm"},
				},
			},
		},
	}

	urls := GetAllWASMURLs(views)
	// GetAllWASMURLs includes ALL non-empty paths (not just HTTP)
	require.Len(t, urls, 2)
	require.Contains(t, urls, "AGFzbQEAAAA=")
	require.Contains(t, urls, "file:///local/path.wasm")
}

// ---------------------------------------------------------------------------
// FetchAllRegisteredViews - with connection error (unreachable server)
// ---------------------------------------------------------------------------

func TestFetchAllRegisteredViews_ConnectionError(t *testing.T) {
	client := NewRPCClient("http://127.0.0.1:1", nil) // unreachable port
	client.httpClient.Timeout = 1 * time.Second

	views, err := client.FetchAllRegisteredViews(context.Background())
	require.NoError(t, err) // FetchAllRegisteredViews doesn't propagate individual page errors
	require.Empty(t, views)
}

// ---------------------------------------------------------------------------
// fetchRegisteredViewsPage - with cancelled context
// ---------------------------------------------------------------------------

func TestFetchRegisteredViewsPage_CancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow response to ensure context cancellation kicks in
		time.Sleep(5 * time.Second)
	}))
	defer server.Close()

	client := &RPCClient{
		rpcURL:     server.URL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	views, total, err := client.fetchRegisteredViewsPage(ctx, 1, 5)
	// Should get empty results (retries exhaust with cancelled context)
	if err != nil {
		require.Contains(t, err.Error(), "context canceled")
	} else {
		require.Equal(t, 0, total)
		require.Empty(t, views)
	}
}

// ---------------------------------------------------------------------------
// fetchRegisteredViewsPage - view with non-Registered event types mixed
// ---------------------------------------------------------------------------

func TestFetchRegisteredViewsPage_MixedEventTypes(t *testing.T) {
	q := "Log {address}"
	sdl := "type MixedEventView @materialized(if: false) {address: String}"
	v := view.View{Query: &q, Sdl: &sdl}
	viewJSON, _ := json.Marshal(v)

	resp := TxSearchResponse{
		JSONRPC: "2.0",
		ID:      1,
		Result: struct {
			Txs        []TxSearchItem `json:"txs"`
			TotalCount string         `json:"total_count"`
		}{
			TotalCount: "1",
			Txs: []TxSearchItem{
				{
					Hash:   "AABB",
					Height: "100",
					TxResult: struct {
						Events []Event `json:"events"`
					}{
						Events: []Event{
							{
								Type: "EntityRegistered",
								Attributes: []EventAttribute{
									{Key: "key", Value: "k"},
									{Key: "owner", Value: "o"},
								},
							},
							{
								Type: "Registered",
								Attributes: []EventAttribute{
									{Key: "view", Value: string(viewJSON)},
								},
							},
							{
								Type: "Transfer",
								Attributes: []EventAttribute{
									{Key: "amount", Value: "100"},
								},
							},
						},
					},
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := &RPCClient{
		rpcURL:     server.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	// Only the Registered event should produce a view
	require.Len(t, views, 1)
	require.Equal(t, "MixedEventView", views[0].Name)
}

// ---------------------------------------------------------------------------
// fetchRegisteredViewsPage - multiple txs with multiple events
// ---------------------------------------------------------------------------

func TestFetchRegisteredViewsPage_MultipleTxs(t *testing.T) {
	q1 := "Log {address}"
	sdl1 := "type View1 @materialized(if: false) {address: String}"
	v1 := view.View{Query: &q1, Sdl: &sdl1}
	v1JSON, _ := json.Marshal(v1)

	q2 := "Block {number}"
	sdl2 := "type View2 @materialized(if: false) {number: Int}"
	v2 := view.View{Query: &q2, Sdl: &sdl2}
	v2JSON, _ := json.Marshal(v2)

	resp := TxSearchResponse{
		JSONRPC: "2.0",
		ID:      1,
		Result: struct {
			Txs        []TxSearchItem `json:"txs"`
			TotalCount string         `json:"total_count"`
		}{
			TotalCount: "2",
			Txs: []TxSearchItem{
				{
					Hash:   "AA",
					Height: "100",
					TxResult: struct {
						Events []Event `json:"events"`
					}{
						Events: []Event{
							{
								Type: "Registered",
								Attributes: []EventAttribute{
									{Key: "view", Value: string(v1JSON)},
								},
							},
						},
					},
				},
				{
					Hash:   "BB",
					Height: "101",
					TxResult: struct {
						Events []Event `json:"events"`
					}{
						Events: []Event{
							{
								Type: "Registered",
								Attributes: []EventAttribute{
									{Key: "view", Value: string(v2JSON)},
								},
							},
						},
					},
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := &RPCClient{
		rpcURL:     server.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	views, total, err := client.fetchRegisteredViewsPage(context.Background(), 1, 5)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, views, 2)
}

// ---------------------------------------------------------------------------
// NewEntityRegistrationHandler
// ---------------------------------------------------------------------------

func TestNewEntityRegistrationHandler(t *testing.T) {
	manager := NewMockIndexerManager()
	handler := NewEntityRegistrationHandler(manager)
	require.NotNil(t, handler)
	require.NotNil(t, handler.indexerManager)
}

// ---------------------------------------------------------------------------
// MockIndexerManager - comprehensive tests
// ---------------------------------------------------------------------------

func TestMockIndexerManager_AddAndGet(t *testing.T) {
	manager := NewMockIndexerManager()
	require.Equal(t, 0, manager.GetIndexerCount())

	err := manager.AddIndexer(context.Background(), "key1", "owner1", "did1", "pid1")
	require.NoError(t, err)
	require.Equal(t, 1, manager.GetIndexerCount())

	indexer, exists := manager.GetIndexer("key1")
	require.True(t, exists)
	require.Equal(t, "key1", indexer.Key)
	require.Equal(t, "owner1", indexer.Owner)
	require.Equal(t, "did1", indexer.DID)
	require.Equal(t, "pid1", indexer.Pid)
	require.True(t, indexer.Active)
}

func TestMockIndexerManager_GetNonExistent(t *testing.T) {
	manager := NewMockIndexerManager()
	_, exists := manager.GetIndexer("nonexistent")
	require.False(t, exists)
}

func TestMockIndexerManager_RemoveIndexer(t *testing.T) {
	manager := NewMockIndexerManager()
	err := manager.AddIndexer(context.Background(), "key1", "owner1", "did1", "pid1")
	require.NoError(t, err)
	require.Equal(t, 1, manager.GetIndexerCount())

	err = manager.RemoveIndexer("key1")
	require.NoError(t, err)
	require.Equal(t, 0, manager.GetIndexerCount())

	_, exists := manager.GetIndexer("key1")
	require.False(t, exists)
}

func TestMockIndexerManager_RemoveNonExistent(t *testing.T) {
	manager := NewMockIndexerManager()
	err := manager.RemoveIndexer("nonexistent")
	require.NoError(t, err)
}

func TestMockIndexerManager_ListIndexers(t *testing.T) {
	manager := NewMockIndexerManager()

	err := manager.AddIndexer(context.Background(), "key1", "owner1", "did1", "pid1")
	require.NoError(t, err)
	err = manager.AddIndexer(context.Background(), "key2", "owner2", "did2", "pid2")
	require.NoError(t, err)

	indexers := manager.ListIndexers()
	require.Len(t, indexers, 2)
}

func TestMockIndexerManager_ListIndexers_Empty(t *testing.T) {
	manager := NewMockIndexerManager()
	indexers := manager.ListIndexers()
	require.Empty(t, indexers)
}

func TestMockIndexerManager_GetIndexerCount(t *testing.T) {
	manager := NewMockIndexerManager()
	require.Equal(t, 0, manager.GetIndexerCount())

	_ = manager.AddIndexer(context.Background(), "a", "o", "d", "p")
	require.Equal(t, 1, manager.GetIndexerCount())

	_ = manager.AddIndexer(context.Background(), "b", "o", "d", "p")
	require.Equal(t, 2, manager.GetIndexerCount())

	_ = manager.RemoveIndexer("a")
	require.Equal(t, 1, manager.GetIndexerCount())
}

// ---------------------------------------------------------------------------
// FetchAllRegisteredViews - startPage > 1 log message
// ---------------------------------------------------------------------------

func TestFetchAllRegisteredViews_ResumesFromHighPage(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	// Pre-save page 100 to exercise the "Resuming from page" log path
	rpcClient := &RPCClient{defraNode: defraNode}
	rpcClient.saveLastProcessedPage(ctx, 100)

	var requestedPages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPages = append(requestedPages, r.URL.RawQuery)
		resp := makeTxSearchResponse("100", nil)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewRPCClient(server.URL, defraNode)

	views, err := client.FetchAllRegisteredViews(ctx)
	require.NoError(t, err)
	require.Empty(t, views)

	// Should start from page 101
	require.GreaterOrEqual(t, len(requestedPages), 1)
	require.Contains(t, requestedPages[0], "page=101")
}

// ---------------------------------------------------------------------------
// saveLastProcessedPage - saving to new record when no existing records
// ---------------------------------------------------------------------------

func TestSaveLastProcessedPage_CreatesNewRecord(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	client := &RPCClient{defraNode: defraNode}

	// Save when there are no existing records - should create new
	client.saveLastProcessedPage(ctx, 1)

	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 1, page)
}

// ---------------------------------------------------------------------------
// saveLastProcessedPage - updating to higher page
// ---------------------------------------------------------------------------

func TestSaveLastProcessedPage_UpdatesHigherPage(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	client := &RPCClient{defraNode: defraNode}

	// Create initial record
	client.saveLastProcessedPage(ctx, 5)
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 5, page)

	// Update to higher page
	client.saveLastProcessedPage(ctx, 15)
	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 15, page)
}

func TestSaveLastProcessedPage_SkipsLowerPage(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	client := &RPCClient{defraNode: defraNode}

	// Create initial record at page 10
	client.saveLastProcessedPage(ctx, 10)
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 10, page)

	// Try to save a lower page, should be skipped
	client.saveLastProcessedPage(ctx, 5)
	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 10, page, "should not downgrade to lower page")

	// Try to save the same page, should also be skipped
	client.saveLastProcessedPage(ctx, 10)
	page = client.getLastProcessedPage(ctx)
	require.Equal(t, 10, page, "saving same page should be a no-op")
}

func TestGetLastProcessedPage_MultipleRecords(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	configSchema := `type Config__LastProcessedPage { page: Int }`
	_, err = defraNode.DB.AddSchema(ctx, configSchema)
	require.NoError(t, err)

	// Create multiple records directly
	for _, p := range []int{3, 7, 5} {
		mutation := fmt.Sprintf(`mutation { create_Config__LastProcessedPage(input: {page: %d}) { _docID } }`, p)
		defraNode.DB.ExecRequest(ctx, mutation)
	}

	client := &RPCClient{defraNode: defraNode}
	page := client.getLastProcessedPage(ctx)
	require.Equal(t, 7, page, "should return max page across all records")
}
