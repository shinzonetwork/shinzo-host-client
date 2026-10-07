package schema

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// ErrSchemaPartiallyStored is returned when the database holds only some of a chain's tables.
var ErrSchemaPartiallyStored = fmt.Errorf("database holds only some of the chain's tables")

// attestationRecordTemplate declares the attestation record collection under the type name
// AttestationRecord, which attestationRecordSchema replaces with the chain's collection name.
//
//go:embed attestationRecord.graphql
var attestationRecordTemplate string

// attestationRecordSchema returns the SDL of the chain's attestation record collection. The host
// writes these records, so generators do not serve the type.
func attestationRecordSchema(c chain.Collections) string {
	return strings.Replace(attestationRecordTemplate, "type AttestationRecord ", "type "+c.AttestationRecord.Name+" ", 1)
}

// ChainApplier creates a chain's collections in DefraDB. It implements defradb.SchemaApplier.
type ChainApplier struct {
	// Tables is the SDL of the chain's tables, from its generators or the built-in schema.
	Tables      string
	Collections chain.Collections
}

// ApplySchema creates the chain's tables unless the database already holds them, then the chain's
// attestation record collection if it is missing. It returns ErrSchemaPartiallyStored when the
// database holds only some of the tables.
func (a ChainApplier) ApplySchema(ctx context.Context, n *node.Node) error {
	cols, err := n.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("read stored collections: %w", err)
	}
	stored := make(map[string]bool, len(cols))
	for _, col := range cols {
		stored[col.Name()] = true
	}

	tables := a.Collections.Generated()
	var missing []string
	for _, t := range tables {
		if !stored[t.Name] {
			missing = append(missing, t.Name)
		}
	}
	switch len(missing) {
	case 0:
	case len(tables):
		if _, err := n.DB.AddCollection(ctx, a.Tables); err != nil {
			return fmt.Errorf("add %s tables: %w", a.Collections.Prefix, err)
		}
	default:
		return fmt.Errorf("%s is missing %s: %w", a.Collections.Prefix, strings.Join(missing, ", "), ErrSchemaPartiallyStored)
	}

	if !stored[a.Collections.AttestationRecord.Name] {
		if _, err := n.DB.AddCollection(ctx, attestationRecordSchema(a.Collections)); err != nil {
			return fmt.Errorf("add %s: %w", a.Collections.AttestationRecord.Name, err)
		}
	}
	return nil
}
