package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	_ "embed"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// Sentinel errors for schema fetch and validation failures.
var (
	ErrSchemaFetchNetwork        = fmt.Errorf("schema fetch network error")
	ErrSchemaFetchStatus         = fmt.Errorf("schema fetch non-OK status")
	ErrSchemaEmptyResponse       = fmt.Errorf("schema field is empty in indexer response")
	ErrSchemaMalformedResponse   = fmt.Errorf("schema response is malformed or invalid JSON")
	ErrSchemaWrongNetwork        = fmt.Errorf("schema is for another chain")
	ErrSchemaForeignType         = fmt.Errorf("schema has a type outside the chain's prefix")
	ErrSchemaHostOwnedType       = fmt.Errorf("schema declares a type the host owns")
	ErrSchemaMissingType         = fmt.Errorf("schema is missing a collection the generator writes")
	ErrSchemaMissingIndexedField = fmt.Errorf("schema is missing a field the host indexes")
)

// AttestationRecordTypeDef is the GraphQL type definition of the attestation records the host
// writes. Generators do not serve it, so the host appends it to a fetched schema.
//
//go:embed attestationRecord.graphql
var AttestationRecordTypeDef string

// maxSchemaBodyBytes caps the schema response size to mitigate DoS via oversized payloads.
// 64 KB provides ~20x headroom over the current ~3.2 KB schema while keeping a tight
// anomaly ceiling for unauthenticated fetches — NewSchemaHTTPClient does not enforce
// a non-empty AuthToken, so the untrusted path is reachable today.
// TODO: Once AuthToken is made required (remove the empty-token fallback in NewSchemaHTTPClient), bump to 512 << 10 for effectively permanent headroom.
const maxSchemaBodyBytes = 64 << 10 // 64 KB

// Response represents the JSON response from the indexer's schema endpoint.
type Response struct {
	Network string `json:"network"`
	Schema  string `json:"schema"`
}

// FetchSchema fetches a chain's schema from fullURL, a generator's schema endpoint, and returns
// it ready to apply: checked against the chain's collections, with the host's indexes and the
// host's AttestationRecord type. It returns an error on an HTTP error, a malformed response or a
// failed check.
func FetchSchema(ctx context.Context, httpClient *http.Client, fullURL string, collections chain.Collections) (string, error) {
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

	if err := checkSchema(schemaResp, collections); err != nil {
		return "", fmt.Errorf("check schema: %w", err)
	}
	withIndexes, err := applyHostIndexes(schemaResp.Schema, collections.Prefix, SchemaGraphQL)
	if err != nil {
		return "", fmt.Errorf("apply host indexes: %w", err)
	}
	return AppendAttestationRecord(withIndexes), nil
}

// checkSchema checks a generator's schema response against the chain's collections: it is for
// this chain, every type is under the chain's prefix, the host's AttestationRecord type is left
// out, and every collection the generator writes is there.
func checkSchema(resp Response, c chain.Collections) error {
	if resp.Network != c.Prefix {
		return fmt.Errorf("network %q, want %q: %w", resp.Network, c.Prefix, ErrSchemaWrongNetwork)
	}
	doc, err := parser.ParseSchema(&ast.Source{Input: resp.Schema})
	if err != nil {
		return fmt.Errorf("parse schema: %w: %w", ErrSchemaMalformedResponse, err)
	}
	for _, def := range doc.Definitions {
		if !strings.HasPrefix(def.Name, c.Prefix+"__") {
			return fmt.Errorf("type %s: %w", def.Name, ErrSchemaForeignType)
		}
		if def.Name == c.AttestationRecord.Name {
			return fmt.Errorf("type %s: %w", def.Name, ErrSchemaHostOwnedType)
		}
	}
	for _, col := range c.Generated() {
		if doc.Definitions.ForName(col.Name) == nil {
			return fmt.Errorf("type %s: %w", col.Name, ErrSchemaMissingType)
		}
	}
	return nil
}

// AppendAttestationRecord appends the host's AttestationRecord type to a fetched schema.
func AppendAttestationRecord(baseSchema string) string {
	return strings.TrimSpace(baseSchema) + "\n\n" + AttestationRecordTypeDef + "\n"
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
