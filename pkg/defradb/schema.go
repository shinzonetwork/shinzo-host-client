package defradb

import (
	"context"

	"github.com/sourcenetwork/defradb/node"
)

// SchemaApplier is implemented by types that know how to install a GraphQL
// schema on a freshly-started DefraDB node. StartDefraInstance invokes
// ApplySchema once during boot, after the node has started.
type SchemaApplier interface {
	ApplySchema(ctx context.Context, defraNode *node.Node) error
}

// MockSchemaApplierThatSucceeds is a test helper that satisfies SchemaApplier
// without touching the database. Use it in tests that don't care about schema.
type MockSchemaApplierThatSucceeds struct{}

// ApplySchema is a mock implementation that always succeeds without performing any operations.
func (schema *MockSchemaApplierThatSucceeds) ApplySchema(ctx context.Context, defraNode *node.Node) error { // nolint:revive
	return nil
}

// SchemaApplierFromProvidedSchema applies a schema string supplied by the caller.
type SchemaApplierFromProvidedSchema struct {
	ProvidedSchema string
}

// NewSchemaApplierFromProvidedSchema creates a new SchemaApplierFromProvidedSchema with the given schema string.
func NewSchemaApplierFromProvidedSchema(schema string) *SchemaApplierFromProvidedSchema {
	return &SchemaApplierFromProvidedSchema{
		ProvidedSchema: schema,
	}
}

// ApplySchema applies the provided schema string to the DefraDB node.
func (schema *SchemaApplierFromProvidedSchema) ApplySchema(ctx context.Context, defraNode *node.Node) error {
	_, err := defraNode.DB.AddSchema(ctx, schema.ProvidedSchema)
	return err
}
