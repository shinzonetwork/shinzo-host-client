package schema

import (
	"context"
	_ "embed"
	"net/http"
)

// SchemaGraphQL holds the contents of the GraphQL schema defined in `schema.graphql`.
//
//go:embed schema.graphql
var SchemaGraphQL string

// GetSchema returns the GraphQL schema found in `schema.graphql` as a string.
func GetSchema() string {
	return SchemaGraphQL
}

// GetSchemaDynamic attempts to fetch the schema from the indexer URL.
//
// fullURL must be non-empty. The production caller in host.go
// validates the URL before calling this function. If the contract is
// violated (empty URL), the underlying HTTP request fails with a
// network-level error and the embedded schema is returned as fallback —
// the function never returns an empty string.
//
// On fetch failure, it returns the embedded schema as fallback along with
// the error so the caller can inspect it and decide on logging policy.
func GetSchemaDynamic(ctx context.Context, httpClient *http.Client, fullURL string) (string, error) {
	schema, err := FetchSchema(ctx, httpClient, fullURL)
	if err != nil {
		return GetSchema(), err
	}

	return schema, nil
}
