package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "embed"

	"github.com/shinzonetwork/shinzo-host-client/config"
)

// Sentinel errors for schema fetch and validation failures.
var (
	ErrSchemaFetchStatus      = fmt.Errorf("schema fetch non-OK status")
	ErrSchemaEmptyResponse    = fmt.Errorf("schema field is empty in indexer response")
	ErrSchemaMissingBlockType = fmt.Errorf("schema missing required type Ethereum__Mainnet__Block")
	ErrSchemaMissingAttType   = fmt.Errorf("schema missing required type Ethereum__Mainnet__AttestationRecord")
)

// AttestationRecordTypeDef is the GraphQL type definition for Ethereum__Mainnet__AttestationRecord.
// This type is NOT included in the indexer's schema response and must be appended by the host.
//
//go:embed attestationRecord.graphql
var AttestationRecordTypeDef string

// Response represents the JSON response from the indexer's schema endpoint.
type Response struct {
	Network string `json:"network"`
	Schema  string `json:"schema"`
}

// FetchSchema fetches the GraphQL schema from the given full URL (base URL + endpoint path),
// appends the AttestationRecord type definition, and validates the result.
// Returns an error on HTTP errors, malformed responses, or validation failures (fail-closed).
func FetchSchema(ctx context.Context, httpClient *http.Client, fullURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", fmt.Errorf("create schema request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch schema: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch schema status %d: %w", resp.StatusCode, ErrSchemaFetchStatus)
	}

	var schemaResp Response
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return "", fmt.Errorf("decode schema response: %w", err)
	}

	if strings.TrimSpace(schemaResp.Schema) == "" {
		return "", ErrSchemaEmptyResponse
	}

	result := AppendAttestationRecord(schemaResp.Schema)
	if err := ValidateSchema(result); err != nil {
		return "", fmt.Errorf("validate schema: %w", err)
	}

	return result, nil
}

// AppendAttestationRecord appends the Ethereum__Mainnet__AttestationRecord type definition
// to the base schema. If the type is already present, the schema is returned unchanged.
func AppendAttestationRecord(baseSchema string) string {
	if strings.Contains(baseSchema, "Ethereum__Mainnet__AttestationRecord") {
		return baseSchema
	}
	return strings.TrimSpace(baseSchema) + "\n\n" + AttestationRecordTypeDef + "\n"
}

// ValidateSchema checks that the schema contains the required type definitions.
// Returns an error if Ethereum__Mainnet__Block or Ethereum__Mainnet__AttestationRecord is missing.
func ValidateSchema(schemaStr string) error {
	if !strings.Contains(schemaStr, "Ethereum__Mainnet__Block") {
		return ErrSchemaMissingBlockType
	}
	if !strings.Contains(schemaStr, "Ethereum__Mainnet__AttestationRecord") {
		return ErrSchemaMissingAttType
	}
	return nil
}

// NewSchemaHTTPClient creates an HTTP client suitable for schema fetching,
// using the timeout from the provided SchemaConfig.
func NewSchemaHTTPClient(cfg config.SchemaConfig) *http.Client {
	return &http.Client{
		Timeout: time.Duration(cfg.HTTPClientTimeoutSecs) * time.Second,
	}
}
