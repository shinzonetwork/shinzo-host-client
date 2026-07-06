package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

const (
	testSchemaNetwork = "ethereum-mainnet"
	testSchemaBlock   = `type Ethereum__Mainnet__Block {
    hash: String @index(unique: true)
}`
)

func TestResolveSchema(t *testing.T) {
	tests := []struct {
		name           string
		indexerURL     string
		schemaEndpoint string
		ctx            context.Context
		setupServer    func() *httptest.Server
		wantEmbedded   bool
	}{
		{
			name:           "valid URL, successful fetch",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					resp := localschema.Response{
						Network: testSchemaNetwork,
						Schema:  testSchemaBlock,
					}
					err := json.NewEncoder(w).Encode(resp)
					require.NoError(t, err)
				}))
			},
			wantEmbedded: false,
		},
		{
			name:           "empty indexer URL falls back to embedded",
			indexerURL:     "",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer:    func() *httptest.Server { return nil },
			wantEmbedded:   true,
		},
		{
			name:           "garbage URL (no scheme/host) falls back to embedded",
			indexerURL:     "not-a-url",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer:    func() *httptest.Server { return nil },
			wantEmbedded:   true,
		},
		{
			name:           "valid URL, network error falls back to embedded",
			indexerURL:     "http://127.0.0.1:1",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer:    func() *httptest.Server { return nil },
			wantEmbedded:   true,
		},
		{
			name:           "valid URL, HTTP 500 falls back to embedded",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			wantEmbedded: true,
		},
		{
			name:           "valid URL, malformed JSON falls back to embedded",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{not valid json`))
				}))
			},
			wantEmbedded: true,
		},
		{
			name:           "valid URL, missing block type falls back to embedded",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					resp := localschema.Response{
						Network: testSchemaNetwork,
						Schema:  "type SomeOtherType { id: String }",
					}
					err := json.NewEncoder(w).Encode(resp)
					require.NoError(t, err)
				}))
			},
			wantEmbedded: true,
		},
		{
			name:           "context cancellation falls back to embedded",
			schemaEndpoint: config.DefaultIndexerSchemaEndpoint,
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				time.Sleep(10 * time.Millisecond)
				cancel()
				return ctx
			}(),
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
					time.Sleep(10 * time.Second)
				}))
			},
			wantEmbedded: true,
		},
	}

	embeddedSchema := localschema.GetSchema()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var srvURL string
			if tt.setupServer != nil {
				srv := tt.setupServer()
				if srv != nil {
					defer srv.Close()
					srvURL = srv.URL
				}
			}

			indexerURL := tt.indexerURL
			if indexerURL == "" && srvURL != "" {
				indexerURL = srvURL
			}

			cfg := &config.Config{
				HostConfig: config.HostConfig{
					Snapshot: config.SnapshotConfig{
						IndexerURL: indexerURL,
					},
				},
				Schema: config.SchemaConfig{
					IndexerSchemaEndpoint: tt.schemaEndpoint,
					HTTPClientTimeoutSecs: 5,
				},
			}

			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			result := resolveSchema(ctx, cfg)

			if tt.wantEmbedded {
				require.Equal(t, embeddedSchema, result)
			} else {
				require.Contains(t, result, "Ethereum__Mainnet__Block")
				require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
				require.NotEqual(t, embeddedSchema, result)
			}
		})
	}
}
