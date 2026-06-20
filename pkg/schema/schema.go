package schema

import (
	"context"
	_ "embed"
	"net/http"
	"strings"
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
// If the URL is empty or whitespace-only, it returns the embedded schema with no error.
// On fetch failure, it returns the embedded schema as fallback along with the error
// so the caller can inspect it and decide on logging policy.
func GetSchemaDynamic(ctx context.Context, httpClient *http.Client, fullURL string) (string, error) {
	if strings.TrimSpace(fullURL) == "" {
		return GetSchema(), nil
	}

	schema, err := FetchSchema(ctx, httpClient, fullURL)
	if err != nil {
		return GetSchema(), err
	}

	return schema, nil
}
