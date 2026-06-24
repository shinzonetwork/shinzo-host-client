package schema

import (
	"context"
	"encoding/json"
	"fmt"
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
	result, err := GetSchemaDynamic(context.Background(), client, "")
	require.NoError(t, err)
	require.Equal(t, GetSchema(), result)
}

func TestGetSchemaDynamic_EmptyURL_Whitespace_ReturnsEmbedded(t *testing.T) {
	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
	result, err := GetSchemaDynamic(context.Background(), client, "   ")
	require.NoError(t, err)
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
	result, err := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
	require.NoError(t, err)
	require.Contains(t, result, "Ethereum__Mainnet__Block")
	require.Contains(t, result, "Ethereum__Mainnet__AttestationRecord")
}

func TestGetSchemaDynamic_NetworkError_Fallback(t *testing.T) {
	client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 1})
	result, err := GetSchemaDynamic(context.Background(), client, "http://127.0.0.1:1/api/v1/schema")
	require.Error(t, err)
	require.Equal(t, GetSchema(), result)
	require.False(t, IsDataLevelError(err))
	require.True(t, IsNetworkLevelError(err))
}

func TestGetSchemaDynamic_DataError_Fallback(t *testing.T) {
	t.Run("http_error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result, err := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Error(t, err)
		require.Equal(t, GetSchema(), result)
		require.True(t, IsNetworkLevelError(err))
		require.False(t, IsDataLevelError(err))
	})

	t.Run("malformed_json", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{not valid json`))
		}))
		defer srv.Close()

		client := NewSchemaHTTPClient(config.SchemaConfig{HTTPClientTimeoutSecs: 30})
		result, err := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Error(t, err)
		require.Equal(t, GetSchema(), result)
		require.True(t, IsDataLevelError(err))
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
		result, err := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Error(t, err)
		require.Equal(t, GetSchema(), result)
		require.True(t, IsDataLevelError(err))
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
		result, err := GetSchemaDynamic(context.Background(), client, srv.URL+"/api/v1/schema")
		require.Error(t, err)
		require.Equal(t, GetSchema(), result)
		require.True(t, IsDataLevelError(err))
	})
}

func TestIsDataLevelError(t *testing.T) {
	t.Run("data_level_errors", func(t *testing.T) {
		require.True(t, IsDataLevelError(ErrSchemaMalformedResponse))
		require.True(t, IsDataLevelError(ErrSchemaEmptyResponse))
		require.True(t, IsDataLevelError(ErrSchemaMissingBlockType))
	})

	t.Run("wrapped_data_level_errors", func(t *testing.T) {
		err := fmt.Errorf("some context: %w", ErrSchemaMalformedResponse)
		require.True(t, IsDataLevelError(err))
	})

	t.Run("network_level_errors_are_not_data_level", func(t *testing.T) {
		require.False(t, IsDataLevelError(ErrSchemaFetchNetwork))
		require.False(t, IsDataLevelError(ErrSchemaFetchStatus))
	})

	t.Run("nil_error", func(t *testing.T) {
		require.False(t, IsDataLevelError(nil))
	})
}

func TestIsNetworkLevelError(t *testing.T) {
	t.Run("network_level_errors", func(t *testing.T) {
		require.True(t, IsNetworkLevelError(ErrSchemaFetchNetwork))
		require.True(t, IsNetworkLevelError(ErrSchemaFetchStatus))
	})

	t.Run("wrapped_network_level_errors", func(t *testing.T) {
		err := fmt.Errorf("some context: %w", ErrSchemaFetchStatus)
		require.True(t, IsNetworkLevelError(err))
	})

	t.Run("data_level_errors_are_not_network_level", func(t *testing.T) {
		require.False(t, IsNetworkLevelError(ErrSchemaMalformedResponse))
		require.False(t, IsNetworkLevelError(ErrSchemaEmptyResponse))
		require.False(t, IsNetworkLevelError(ErrSchemaMissingBlockType))
	})

	t.Run("nil_error", func(t *testing.T) {
		require.False(t, IsNetworkLevelError(nil))
	})
}
