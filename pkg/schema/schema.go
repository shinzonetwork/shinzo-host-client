package schema

import (
	"context"
	_ "embed"
	"net/http"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
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
// If the URL is empty, returns the embedded schema.
// On any error, logs a warning and falls back to the embedded schema.
// Always returns a usable schema string.
func GetSchemaDynamic(ctx context.Context, httpClient *http.Client, fullURL string) string {
	if strings.TrimSpace(fullURL) == "" {
		logger.Sugar.Warnf("Empty schema URL, using embedded schema")
		return GetSchema()
	}

	schema, err := FetchSchema(ctx, httpClient, fullURL)
	if err != nil {
		logger.Sugar.Warnf("Schema fetch failed, using embedded schema: %v", err)
		return GetSchema()
	}

	return schema
}
