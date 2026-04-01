package schema

import (
	_ "embed"
)

//go:embed schema.graphql
var SchemaGraphQL string

// GetSchema returns the GraphQL schema found in `schema.graphql` as a string.
func GetSchema() string {
	return SchemaGraphQL
}

// GetSchemaForBuild returns the appropriate schema based on build tags.
func GetSchemaForBuild() string {
	return GetSchema()
}
