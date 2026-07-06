package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
)

const (
	testNetwork     = "ethereum-mainnet"
	testSchemaBlock = `type Ethereum__Mainnet__Block {
    hash: String @index(unique: true)
}`
	testSchemaBlockWithAtt = `type Ethereum__Mainnet__Block {
    hash: String @index(unique: true)
}

type Ethereum__Mainnet__AttestationRecord {
    attested_doc: String @index
    source_doc: [String]
    CIDs: [String]
    doc_type: String @index
    vote_count: Int @crdt(type: pcounter)
}`
	testSchemaBlockWithAttShort = `type Ethereum__Mainnet__Block {
    hash: String @index(unique: true)
}

type Ethereum__Mainnet__AttestationRecord {
    attested_doc: String @index
}`
)

var validResponse = Response{
	Network: testNetwork,
	Schema:  testSchemaBlock,
}

var testSchemaConfig = config.SchemaConfig{HTTPClientTimeoutSecs: 30}

func testIndexerSchemaURL(srv *httptest.Server) string {
	return srv.URL + config.DefaultIndexerSchemaEndpoint
}

func TestFetchSchema_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(validResponse)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__Block")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
}

func TestFetchSchema_AppendsAttestationRecord(t *testing.T) {
	t.Parallel()

	schemaWithoutAttestation := Response{
		Network: testNetwork,
		Schema:  testSchemaBlock,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(schemaWithoutAttestation)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
	require.Contains(t, result, "attested_doc: String @index")
}

func TestFetchSchema_DoesNotDuplicateAttestationRecord(t *testing.T) {
	t.Parallel()

	schemaWithAttestation := Response{
		Network: testNetwork,
		Schema:  testSchemaBlockWithAtt,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(schemaWithAttestation)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.NoError(t, err)

	count := strings.Count(result, "Ethereum__Mainnet__AttestationRecord")
	require.Equal(t, 1, count, "AttestationRecord should appear exactly once, got %d", count)
}

func TestFetchSchema_NetworkError(t *testing.T) {
	t.Parallel()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, "http://127.0.0.1:1"+config.DefaultIndexerSchemaEndpoint)
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
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
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
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode schema response")
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestFetchSchema_EmptySchemaField(t *testing.T) {
	t.Parallel()

	emptySchema := Response{
		Network: testNetwork,
		Schema:  "",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(emptySchema)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaEmptyResponse)
}

func TestFetchSchema_MissingRequiredTypes(t *testing.T) {
	t.Parallel()

	minimalSchema := Response{
		Network: testNetwork,
		Schema:  "type SomeOtherType { id: String }",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(minimalSchema)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.Error(t, err)
	require.Contains(t, err.Error(), "validate schema")
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
	_, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestAppendAttestationRecord(t *testing.T) {
	t.Parallel()

	result := AppendAttestationRecord(testSchemaBlock)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
	require.Contains(t, result, "attested_doc: String @index")
	require.Contains(t, result, "Ethereum__Mainnet__Block")
}

func TestAppendAttestationRecord_AlreadyPresent(t *testing.T) {
	t.Parallel()

	result := AppendAttestationRecord(testSchemaBlockWithAttShort)
	count := strings.Count(result, "Ethereum__Mainnet__AttestationRecord")
	require.Equal(t, 1, count, "should not duplicate AttestationRecord")
	require.Equal(t, testSchemaBlockWithAttShort, result, "should return unchanged when already present")
}

func TestValidateSchema_Valid(t *testing.T) {
	t.Parallel()

	err := ValidateSchema(testSchemaBlockWithAttShort)
	require.NoError(t, err)
}

func TestValidateSchema_MissingBlock(t *testing.T) {
	t.Parallel()

	schema := `type Ethereum__Mainnet__AttestationRecord {
    attested_doc: String @index
}`
	err := ValidateSchema(schema)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMissingBlockType)
}

func TestValidateSchema_BlockSignatureOnly(t *testing.T) {
	t.Parallel()

	schema := `type Ethereum__Mainnet__BlockSignature {
    blockHash: String
}`
	err := ValidateSchema(schema)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMissingBlockType)
}

func TestAppendAttestationRecord_DoesNotMatchSimilarType(t *testing.T) {
	t.Parallel()

	schema := `type Ethereum__Mainnet__AttestationRecordFoo { id: String }`
	result := AppendAttestationRecord(schema)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord {", "should append the real type when only a similar-named type exists")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecordFoo", "should preserve existing types")
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
	result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))
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

			result, err := FetchSchema(context.Background(), client, testIndexerSchemaURL(srv))

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}
				return
			}

			require.NoError(t, err)
			require.Contains(t, result, "Ethereum__Mainnet__Block")
			require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
		})
	}
}
