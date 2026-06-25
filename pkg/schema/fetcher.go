package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	_ "embed"

	"github.com/shinzonetwork/shinzo-host-client/config"
)

// Sentinel errors for schema fetch and validation failures.
var (
	ErrSchemaFetchNetwork      = fmt.Errorf("schema fetch network error")
	ErrSchemaFetchStatus       = fmt.Errorf("schema fetch non-OK status")
	ErrSchemaEmptyResponse     = fmt.Errorf("schema field is empty in indexer response")
	ErrSchemaMalformedResponse = fmt.Errorf("schema response is malformed or invalid JSON")
	ErrSchemaMissingBlockType  = fmt.Errorf("schema missing required type Ethereum__Mainnet__Block")
)

var (
	blockTypeRegEx       = regexp.MustCompile(`type\s+Ethereum__Mainnet__Block\s*\{`)
	attestationTypeRegEx = regexp.MustCompile(`type\s+Ethereum__Mainnet__AttestationRecord\s*\{`)
)

// AttestationRecordTypeDef is the GraphQL type definition for Ethereum__Mainnet__AttestationRecord.
// This type is NOT included in the indexer's schema response and must be appended by the host.
//
//go:embed attestationRecord.graphql
var AttestationRecordTypeDef string

// Specifying max schema body size to prevent attacker from sending a large payload.
const maxSchemaBodyBytes = 5 << 10 // 5 KB

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
		return "", fmt.Errorf("create schema request: %w: %w", ErrSchemaFetchNetwork, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch schema: %w: %w", ErrSchemaFetchNetwork, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch schema status %d: %w", resp.StatusCode, ErrSchemaFetchStatus)
	}

	var schemaResp Response
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSchemaBodyBytes)).Decode(&schemaResp); err != nil {
		return "", fmt.Errorf("decode schema response: %w: %w", ErrSchemaMalformedResponse, err)
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
	if attestationTypeRegEx.MatchString(baseSchema) {
		return baseSchema
	}
	return strings.TrimSpace(baseSchema) + "\n\n" + AttestationRecordTypeDef + "\n"
}

// ValidateSchema checks that the schema contains the required type definitions.
// Returns an error if Ethereum__Mainnet__Block is missing.
func ValidateSchema(schemaStr string) error {
	// TODO: Update this function to perform more comprehensive validation check beyond Block schema verification
	if !blockTypeRegEx.MatchString(schemaStr) {
		return ErrSchemaMissingBlockType
	}

	return nil
}

// IsDataLevelError reports whether the given error is a data-level schema fetch error
// (e.g. malformed JSON, empty schema, missing required types).
// Network-level errors like DNS failure, connection refused, or HTTP non-200 return false.
//
// This classifier assumes single-level sentinel wrapping: FetchSchema wraps each error
// with exactly one sentinel error via %w. Transitive wrapping of multiple sentinels
// (e.g., fmt.Errorf("...: %w: %w", ErrSchemaFetchNetwork, ErrSchemaMalformedResponse))
// could cause misclassification and must not be introduced without updating these classifiers.
func IsDataLevelError(err error) bool {
	return errors.Is(err, ErrSchemaMalformedResponse) ||
		errors.Is(err, ErrSchemaEmptyResponse) ||
		errors.Is(err, ErrSchemaMissingBlockType)
}

// IsNetworkLevelError reports whether the given error is a network-level schema fetch error
// (e.g. DNS failure, connection refused, timeout, or HTTP non-200 status).
//
// This classifier assumes single-level sentinel wrapping: FetchSchema wraps each error
// with exactly one sentinel error via %w. Transitive wrapping of multiple sentinels
// (e.g., fmt.Errorf("...: %w: %w", ErrSchemaFetchNetwork, ErrSchemaMalformedResponse))
// could cause misclassification and must not be introduced without updating these classifiers.
func IsNetworkLevelError(err error) bool {
	return errors.Is(err, ErrSchemaFetchNetwork) ||
		errors.Is(err, ErrSchemaFetchStatus)
}

// authTransport wraps an http.RoundTripper to inject a Bearer token
// into the Authorization header of every outgoing request.
type authTransport struct {
	base  http.RoundTripper
	token string
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(req)
}

// NewSchemaHTTPClient creates an HTTP client suitable for schema fetching,
// using the timeout from the provided SchemaConfig. When AuthToken is non-empty,
// the client's transport injects an Authorization: Bearer <token> header on
// every outgoing request.
func NewSchemaHTTPClient(cfg config.SchemaConfig) *http.Client {
	var transport http.RoundTripper = http.DefaultTransport.(*http.Transport).Clone()
	if cfg.AuthToken != "" {
		transport = &authTransport{base: transport, token: cfg.AuthToken}
	}
	return &http.Client{
		Timeout:   time.Duration(cfg.HTTPClientTimeoutSecs) * time.Second,
		Transport: transport,
	}
}
