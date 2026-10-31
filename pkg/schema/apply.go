package schema

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
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

// verifiedBlockTemplate declares the verified block collection under the type name VerifiedBlock,
// which verifiedBlockSchema replaces with the chain's collection name.
//
//go:embed verifiedBlock.graphql
var verifiedBlockTemplate string

// verifiedBlockSchema returns the SDL of a tokens-mode chain's verified block collection. The host
// writes these records, so generators do not serve the type.
func verifiedBlockSchema(c chain.Collections) string {
	return strings.Replace(verifiedBlockTemplate, "type VerifiedBlock ", "type "+c.VerifiedBlock.Name+" ", 1)
}

// ChainApplier creates a chain's collections in DefraDB. It implements defradb.SchemaApplier.
type ChainApplier struct {
	// Tables returns the SDL of the chain's tables. ApplySchema calls it only when the database
	// holds none of them.
	Tables func(ctx context.Context) (string, error)
	// Served yields the URL of each answering generator and its response, as FetchResponse returns
	// it, whatever chain the response's network names. ApplySchema calls it only when the database
	// holds the chain's tables, and stops at the first served schema that matches them.
	Served      func(ctx context.Context) iter.Seq2[string, Response]
	Collections chain.Collections
}

// ApplySchema creates the chain's tables from Tables unless the database already holds them, then
// the chain's attestation record collection, and a tokens-mode chain's verified block collection,
// if missing. It returns ErrSchemaPartiallyStored when the database holds only some of the chain's
// tables, and ErrSchemaDrift when it holds them and no schema from Served matches them. A schema
// FetchSchema would refuse is logged and skipped, since wiping the store would not let the host take
// it. If Served yields no other schema, the tables are not compared.
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
		var errs []error
		for generator, resp := range a.Served(ctx) {
			if _, err := prepareSchema(resp, a.Collections); err != nil {
				logger.Sugar.Warnf("Schema from generator %s not compared with the stored %s tables: %v", generator, a.Collections.Prefix, err)
				continue
			}
			err := checkStored(resp.document, cols, a.Collections)
			if err == nil {
				errs = nil
				break
			}
			errs = append(errs, fmt.Errorf("generator %s: %w", generator, err))
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
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
	if a.Collections.TokensMode() && !stored[a.Collections.VerifiedBlock.Name] {
		if _, err := n.DB.AddCollection(ctx, verifiedBlockSchema(a.Collections)); err != nil {
			return fmt.Errorf("add %s: %w", a.Collections.VerifiedBlock.Name, err)
		}
	}
	return nil
}
