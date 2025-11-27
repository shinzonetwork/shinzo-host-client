package schema

import (
	_ "embed"
)

//go:embed viewSchema.graphql
var ViewSchemaGraphQL string

func GetViewSchema() string {
	return ViewSchemaGraphQL
}
