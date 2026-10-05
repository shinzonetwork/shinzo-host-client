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

// ErrSchemaOtherChainStored is returned when the database holds the tables of a chain other than
// the one being applied.
var ErrSchemaOtherChainStored = fmt.Errorf("database holds another chain's tables")

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
	// Tables returns the SDL of the chain's tables. ApplySchema calls it only when the database
	// holds none of them.
	Tables      func(ctx context.Context) (string, error)
	Collections chain.Collections
}

// ApplySchema creates the chain's tables from Tables unless the database already holds them, then
// the chain's attestation record collection if it is missing. It returns ErrSchemaOtherChainStored
// when the database holds another chain's tables, and ErrSchemaPartiallyStored when it holds only
// some of this chain's tables.
func (a ChainApplier) ApplySchema(ctx context.Context, n *node.Node) error {
	cols, err := n.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("read stored collections: %w", err)
	}
	// Every chain has a "<prefix>__BlockSignature" table, so one under another prefix means the
	// database holds another chain.
	signatureSuffix := strings.TrimPrefix(a.Collections.BlockSignature.Name, a.Collections.Prefix)
	stored := make(map[string]bool, len(cols))
	for _, col := range cols {
		if prefix, ok := strings.CutSuffix(col.Name(), signatureSuffix); ok && prefix != a.Collections.Prefix {
			return fmt.Errorf("%s: %w", prefix, ErrSchemaOtherChainStored)
		}
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
		sdl, err := a.Tables(ctx)
		if err != nil {
			return fmt.Errorf("%s tables: %w", a.Collections.Prefix, err)
		}
		if _, err := n.DB.AddCollection(ctx, sdl); err != nil {
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
