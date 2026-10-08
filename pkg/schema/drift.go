package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sourcenetwork/defradb/client"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// ErrSchemaDrift is returned when the stored tables differ from the tables a generator serves.
var ErrSchemaDrift = fmt.Errorf("stored tables differ from the served schema")

// checkStored compares the tables sdl declares with the chain's stored tables: their names, their
// fields' names and their fields' kinds. Stored views, the host's attestation record table and
// the stored fields DefraDB adds, whose names start with "_", are left out. It returns
// ErrSchemaDrift listing every difference.
func checkStored(sdl string, stored []client.Collection, c chain.Collections) error {
	served, err := servedTables(sdl)
	if err != nil {
		return err
	}
	tables := storedTables(stored, c)

	var diffs []string
	for _, name := range unionKeys(tables, served) {
		storedFields, inStore := tables[name]
		servedFields, inServed := served[name]
		switch {
		case !inStore:
			diffs = append(diffs, name+" is served but not stored")
		case !inServed:
			diffs = append(diffs, name+" is stored but not served")
		default:
			diffs = append(diffs, fieldDiffs(name, storedFields, servedFields)...)
		}
	}
	if len(diffs) > 0 {
		return fmt.Errorf("%s: %w", strings.Join(diffs, "; "), ErrSchemaDrift)
	}
	return nil
}

// fieldDiffs describes how a table's stored fields differ from its served ones.
func fieldDiffs(table string, stored, served map[string]string) []string {
	var diffs []string
	for _, f := range unionKeys(stored, served) {
		s, inStore := stored[f]
		v, inServed := served[f]
		switch {
		case !inStore:
			diffs = append(diffs, fmt.Sprintf("%s.%s is served but not stored", table, f))
		case !inServed:
			diffs = append(diffs, fmt.Sprintf("%s.%s is stored but not served", table, f))
		case s != v:
			diffs = append(diffs, fmt.Sprintf("%s.%s is stored as %s and served as %s", table, f, s, v))
		}
	}
	return diffs
}

// servedTables returns the fields of each type sdl declares, with each field's kind named the way
// DefraDB names the kind it stores.
func servedTables(sdl string) (map[string]map[string]string, error) {
	doc, err := parser.ParseSchema(&ast.Source{Input: sdl})
	if err != nil {
		return nil, fmt.Errorf("parse served schema: %w: %w", ErrSchemaMalformedResponse, err)
	}
	tables := make(map[string]map[string]string, len(doc.Definitions))
	for _, def := range doc.Definitions {
		fields := make(map[string]string, len(def.Fields))
		for _, f := range def.Fields {
			kind := f.Type.String()
			// DefraDB accepts some kinds under two names, such as Float and Float64.
			if k, ok := client.FieldKindStringToEnumMapping[kind]; ok {
				kind = k.String()
			}
			fields[f.Name] = kind
		}
		tables[def.Name] = fields
	}
	return tables, nil
}

// storedTables returns the fields of each stored table under the chain's prefix, with each field's
// kind as DefraDB names it, except that a relation names the related table.
func storedTables(stored []client.Collection, c chain.Collections) map[string]map[string]string {
	names := make(map[string]string, len(stored))
	for _, col := range stored {
		names[col.CollectionID()] = col.Name()
	}
	tables := make(map[string]map[string]string)
	for _, col := range stored {
		if !strings.HasPrefix(col.Name(), c.Prefix+"__") || col.Name() == c.AttestationRecord.Name ||
			col.Version().Query.HasValue() {
			continue
		}
		fields := make(map[string]string)
		for _, f := range col.Version().Fields {
			if strings.HasPrefix(f.Name, "_") {
				continue
			}
			fields[f.Name] = kindName(f.Kind, col.Name(), names)
		}
		tables[col.Name()] = fields
	}
	return tables
}

// kindName names a stored field's kind. DefraDB stores a relation by the related table's
// collection ID, or as Self when the table relates to itself, so a relation is named by the
// related table's name, as an SDL names it.
func kindName(k client.FieldKind, table string, names map[string]string) string {
	var related string
	var many bool
	switch k := k.(type) {
	case *client.CollectionKind:
		related, many = names[k.CollectionID], k.Array
	case *client.SelfKind:
		if k.RelativeID != "" {
			return k.String()
		}
		related, many = table, k.Array
	default:
		return k.String()
	}
	if many {
		return "[" + related + "]"
	}
	return related
}

// unionKeys returns the keys of a and b, sorted.
func unionKeys[V any](a, b map[string]V) []string {
	keys := slices.Collect(maps.Keys(a))
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}
