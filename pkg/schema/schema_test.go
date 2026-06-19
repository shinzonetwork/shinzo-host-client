package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

func init() {
	logger.Init(true, "")
}

func TestGetSchema(t *testing.T) {
	s := GetSchema()
	require.NotEmpty(t, s)
}

func TestGetSchema_ContainsExpectedTypes(t *testing.T) {
	s := GetSchema()
	require.Contains(t, s, "type", "schema should contain GraphQL type definitions")
}

func TestGetSchemaDynamic_EmptyURL_ReturnsEmbedded(t *testing.T) {
	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
	result := GetSchemaDynamic(context.Background(), client, "")
	require.Equal(t, GetSchema(), result)
}

func TestGetSchemaDynamic_EmptyURL_Whitespace_ReturnsEmbedded(t *testing.T) {
	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
	result := GetSchemaDynamic(context.Background(), client, "   ")
	require.Equal(t, GetSchema(), result)
}

func TestGetSchemaDynamic_FetchSuccess(t *testing.T) {
	validResp := Response{
		Network: testNetwork,
		Schema:  testSchemaBlock,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(validResp)
		require.NoError(t, err)
	}))
	defer srv.Close()

	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
	result := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
	require.Contains(t, result, "Ethereum__Mainnet__Block")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
}

func TestGetSchemaDynamic_NetworkError_Fallback(t *testing.T) {
	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 1})
	result := GetSchemaDynamic(context.Background(), client, "http://127.0.0.1:1/api/v1/schema")
	require.Equal(t, GetSchema(), result)
}

func TestGetSchemaDynamic_DataError_Fallback(t *testing.T) {
	t.Run("http_error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Equal(t, GetSchema(), result)
	})

	t.Run("malformed_json", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{not valid json`))
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Equal(t, GetSchema(), result)
	})

	t.Run("empty_schema", func(t *testing.T) {
		emptyResp := Response{Network: testNetwork, Schema: ""}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(emptyResp)
			require.NoError(t, err)
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Equal(t, GetSchema(), result)
	})

	t.Run("missing_required_types", func(t *testing.T) {
		badResp := Response{Network: testNetwork, Schema: "type SomeOtherType { id: String }"}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(badResp)
			require.NoError(t, err)
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Equal(t, GetSchema(), result)
	})
}
