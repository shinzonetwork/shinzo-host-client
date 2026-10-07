package schema

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// generatorSchema is a schema as a generator serves it for Ethereum mainnet.
//
//go:embed testdata/generator_schema.graphql
var generatorSchema string

var (
	ethereum      = chain.EVM(chain.EthereumMainnet)
	validResponse = Response{Network: chain.EthereumMainnet, Schema: generatorSchema}
)

var testSchemaConfig = config.SchemaConfig{HTTPClientTimeoutSecs: 30}

func testIndexerSchemaURL(srv *httptest.Server) string {
	return srv.URL + config.DefaultIndexerSchemaEndpoint
}

// serveSchema returns a server that answers every request with resp.
func serveSchema(t *testing.T, resp Response) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchSchema_Success(t *testing.T) {
	srv := serveSchema(t, validResponse)

	result, err := FetchSchema(context.Background(), NewSchemaHTTPClient(testSchemaConfig), testIndexerSchemaURL(srv), ethereum)
	require.NoError(t, err)

	// Applied to DefraDB, the fetched schema has the built-in schema's indexes.
	require.Equal(t, appliedIndexes(t, SchemaGraphQL, chain.EthereumMainnet), appliedIndexes(t, result, chain.EthereumMainnet))
}

func TestFetchSchema_Checks(t *testing.T) {
	cases := []struct {
		desc    string
		resp    Response
		wantErr error
	}{
		{
			desc:    "another chain's network",
			resp:    Response{Network: "Ethereum__Sepolia", Schema: generatorSchema},
			wantErr: ErrSchemaWrongNetwork,
		},
		{
			desc:    "type outside the prefix",
			resp:    Response{Network: chain.EthereumMainnet, Schema: generatorSchema + "\ntype Other__Chain__Block { number: Int }\n"},
			wantErr: ErrSchemaForeignType,
		},
		{
			desc:    "host's AttestationRecord type",
			resp:    Response{Network: chain.EthereumMainnet, Schema: generatorSchema + "\n" + attestationRecordSchema(ethereum)},
			wantErr: ErrSchemaHostOwnedType,
		},
		{
			desc: "missing collection",
			resp: Response{
				Network: chain.EthereumMainnet,
				Schema:  regexp.MustCompile(`(?s)type `+ethereum.SnapshotSignature.Name+` \{.*?\}`).ReplaceAllString(generatorSchema, ""),
			},
			wantErr: ErrSchemaMissingType,
		},
		{
			desc:    "missing indexed field",
			resp:    Response{Network: chain.EthereumMainnet, Schema: strings.Replace(generatorSchema, "number: Int @index", "", 1)},
			wantErr: ErrSchemaMissingIndexedField,
		},
		{
			desc:    "schema that does not parse",
			resp:    Response{Network: chain.EthereumMainnet, Schema: "type {"},
			wantErr: ErrSchemaMalformedResponse,
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			srv := serveSchema(t, c.resp)

			_, err := FetchSchema(context.Background(), NewSchemaHTTPClient(testSchemaConfig), testIndexerSchemaURL(srv), ethereum)
			require.ErrorIs(t, err, c.wantErr)
		})
	}
}

func TestFetchSchema_NetworkError(t *testing.T) {
	t.Parallel()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, "http://127.0.0.1:1"+config.DefaultIndexerSchemaEndpoint, ethereum)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaFetchNetwork)
}

func TestFetchSchema_HttpError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "status 500")
	require.ErrorIs(t, err, ErrSchemaFetchStatus)
}

func TestFetchSchema_MalformedJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode schema response")
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestFetchSchema_EmptySchemaField(t *testing.T) {
	t.Parallel()

	emptySchema := Response{
		Network: chain.EthereumMainnet,
		Schema:  "",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(emptySchema)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaEmptyResponse)
}

func TestFetchSchema_OversizedPayload(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		largePayload := make([]byte, maxSchemaBodyBytes+1) // Exceeding the max schema payload size by 1 byte
		for i := range largePayload {
			largePayload[i] = 'a'
		}
		_, _ = w.Write(largePayload)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestNewSchemaHTTPClient(t *testing.T) {
	t.Parallel()

	client := NewSchemaHTTPClient(testSchemaConfig)
	require.NotNil(t, client)
	require.Equal(t, 30*time.Second, client.Timeout)
}

func TestNewSchemaHTTPClient_CustomTimeout(t *testing.T) {
	t.Parallel()

	cfg := config.SchemaConfig{HTTPClientTimeoutSecs: 60}
	client := NewSchemaHTTPClient(cfg)
	require.Equal(t, 60*time.Second, client.Timeout)
}

func TestFetchSchema_StrictContentNegotiation(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/json" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(validResponse)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__Block")
}

func TestNewSchemaHTTPClient_AuthHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authToken  string
		wantHeader string
		wantSet    bool
	}{
		{
			name:       "token present sets Authorization header",
			authToken:  "test-token",
			wantHeader: "Bearer test-token",
			wantSet:    true,
		},
		{
			name:       "token absent does not set Authorization header",
			authToken:  "",
			wantHeader: "",
			wantSet:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotHeader string
			var headerSet bool

			srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				gotHeader = r.Header.Get("Authorization")
				headerSet = r.Header.Get("Authorization") != ""
			}))
			defer srv.Close()

			cfg := config.SchemaConfig{
				HTTPClientTimeoutSecs: 30,
				AuthToken:             tt.authToken,
			}
			client := NewSchemaHTTPClient(cfg)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			require.Equal(t, tt.wantSet, headerSet)
			require.Equal(t, tt.wantHeader, gotHeader)
		})
	}
}

func TestFetchSchema_AuthToken(t *testing.T) {
	t.Parallel()

	const serverToken = "correct-token"

	tests := []struct {
		name        string
		clientToken string
		wantErr     bool
		wantErrIs   error
	}{
		{
			name:        "correct token succeeds",
			clientToken: serverToken,
			wantErr:     false,
		},
		{
			name:        "wrong token rejected with 401",
			clientToken: "wrong-token",
			wantErr:     true,
			wantErrIs:   ErrSchemaFetchStatus,
		},
		{
			name:        "missing token rejected with 401",
			clientToken: "",
			wantErr:     true,
			wantErrIs:   ErrSchemaFetchStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+serverToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				err := json.NewEncoder(w).Encode(validResponse)
				require.NoError(t, err)
			}))
			defer srv.Close()

			cfg := config.SchemaConfig{
				HTTPClientTimeoutSecs: 30,
				AuthToken:             tt.clientToken,
			}
			client := NewSchemaHTTPClient(cfg)

			result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv), ethereum)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}
				return
			}

			require.NoError(t, err)
			require.Contains(t, result, "Ethereum__Mainnet__Block")
		})
	}
}
