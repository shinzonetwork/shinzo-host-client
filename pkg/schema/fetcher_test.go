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

func TestFetchSchema_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(validResponse)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	result, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__Block")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
}

func TestFetchSchema_AppendsAttestationRecord(t *testing.T) {
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
	result, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
	require.Contains(t, result, "attested_doc: String @index")
}

func TestFetchSchema_DoesNotDuplicateAttestationRecord(t *testing.T) {
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
	result, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.NoError(t, err)

	count := strings.Count(result, "Ethereum__Mainnet__AttestationRecord")
	require.Equal(t, 1, count, "AttestationRecord should appear exactly once, got %d", count)
}

func TestFetchSchema_NetworkError(t *testing.T) {
	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, "http://127.0.0.1:1/api/v1/schema")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaFetchNetwork)
}

func TestFetchSchema_HttpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Error(t, err)
	require.Contains(t, err.Error(), "status 500")
	require.ErrorIs(t, err, ErrSchemaFetchStatus)
}

func TestFetchSchema_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode schema response")
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestFetchSchema_EmptySchemaField(t *testing.T) {
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
	_, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaEmptyResponse)
}

func TestFetchSchema_MissingRequiredTypes(t *testing.T) {
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
	_, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Error(t, err)
	require.Contains(t, err.Error(), "validate schema")
}

func TestFetchSchema_OversizedPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		largePayload := make([]byte, maxSchemaBodyBytes + 1) // Exceeding the max schema payload size by 1 byte
		for i := range largePayload {
			largePayload[i] = 'a'
		}
		_, _ = w.Write(largePayload)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(testSchemaConfig)
	_, err := FetchSchema(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMalformedResponse)
}

func TestAppendAttestationRecord(t *testing.T) {
	result := AppendAttestationRecord(testSchemaBlock)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
	require.Contains(t, result, "attested_doc: String @index")
	require.Contains(t, result, "Ethereum__Mainnet__Block")
}

func TestAppendAttestationRecord_AlreadyPresent(t *testing.T) {
	result := AppendAttestationRecord(testSchemaBlockWithAttShort)
	count := strings.Count(result, "Ethereum__Mainnet__AttestationRecord")
	require.Equal(t, 1, count, "should not duplicate AttestationRecord")
	require.Equal(t, testSchemaBlockWithAttShort, result, "should return unchanged when already present")
}

func TestValidateSchema_Valid(t *testing.T) {
	err := ValidateSchema(testSchemaBlockWithAttShort)
	require.NoError(t, err)
}

func TestValidateSchema_MissingBlock(t *testing.T) {
	schema := `type Ethereum__Mainnet__AttestationRecord {
    attested_doc: String @index
}`
	err := ValidateSchema(schema)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMissingBlockType)
}

func TestValidateSchema_BlockSignatureOnly(t *testing.T) {
	schema := `type Ethereum__Mainnet__BlockSignature {
    blockHash: String
}`
	err := ValidateSchema(schema)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaMissingBlockType)
}

func TestAppendAttestationRecord_DoesNotMatchSimilarType(t *testing.T) {
	schema := `type Ethereum__Mainnet__AttestationRecordFoo { id: String }`
	result := AppendAttestationRecord(schema)
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord {", "should append the real type when only a similar-named type exists")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecordFoo", "should preserve existing types")
}

func TestNewSchemaHTTPClient(t *testing.T) {
	client := NewSchemaHTTPClient(testSchemaConfig)
	require.NotNil(t, client)
	require.Equal(t, 30*time.Second, client.Timeout)
}

func TestNewSchemaHTTPClient_CustomTimeout(t *testing.T) {
	cfg := config.SchemaConfig{HTTPClientTimeoutSecs: 60}
	client := NewSchemaHTTPClient(cfg)
	require.Equal(t, 60*time.Second, client.Timeout)
}
