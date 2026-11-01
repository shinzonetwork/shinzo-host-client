package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sourcenetwork/defradb/client"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// ErrSchemaDrift is returned when the stored tables differ from the tables a generator serves.
var ErrSchemaDrift = fmt.Errorf("stored tables differ from the served schema")

// checkStored compares the tables doc declares with the chain's stored tables: their names, their
// fields' names, their fields' kinds and their scalar fields' CRDT types. Stored views, the host's
// attestation record and verified block tables, served tables the host does not use (see
// unusedTable), the stored fields DefraDB adds, whose names start with "_", and the many side of a
// relation, which does not change a table's collection ID, are left out. The secondary side of a
// one-to-one relation is compared, although it does not change the collection ID either, so a
// change to that side alone refuses a restart. It returns ErrSchemaDrift listing every difference
// and naming the store wipe as the recovery: the host creates a chain's tables only in a store that
// holds none of them and never changes stored ones.
func checkStored(doc *ast.SchemaDocument, stored []client.Collection, c chain.Collections) error {
	served := servedTables(doc)
	tables := storedTables(stored, c)
	maps.DeleteFunc(served, func(name string, _ map[string]string) bool { return unusedTable(c, name) })

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
		return fmt.Errorf("%s: %w; wipe the store for the host to take the served schema", strings.Join(diffs, "; "), ErrSchemaDrift)
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

// servedTables returns the fields of each type doc declares, with each field's kind named the way
// DefraDB names the kind it stores, and a scalar field's CRDT type. The many side of a relation is
// left out.
func servedTables(doc *ast.SchemaDocument) map[string]map[string]string {
	tables := make(map[string]map[string]string, len(doc.Definitions))
	for _, def := range doc.Definitions {
		fields := make(map[string]string, len(def.Fields))
		for _, f := range def.Fields {
			// DefraDB accepts some kinds under two names, such as Float and Float64.
			k, scalar := client.FieldKindStringToEnumMapping[f.Type.String()]
			if scalar {
				crdt := client.LWW_REGISTER.String()
				if d := f.Directives.ForName("crdt"); d != nil && d.Arguments.ForName("type") != nil {
					crdt = d.Arguments.ForName("type").Value.Raw
				}
				fields[f.Name] = withCRDT(k.String(), crdt)
			} else if f.Type.Elem == nil {
				// A relation names the related table. Its many side, a list, is left out.
				fields[f.Name] = f.Type.String()
			}
		}
		tables[def.Name] = fields
	}
	return tables
}

// storedTables returns the fields of each stored table under the chain's prefix, with each field's
// kind as DefraDB names it, except that a relation names the related table, and a scalar field's
// CRDT type. The many side of a relation is left out.
func storedTables(stored []client.Collection, c chain.Collections) map[string]map[string]string {
	names := make(map[string]string, len(stored))
	for _, col := range stored {
		names[col.CollectionID()] = col.Name()
	}
	tables := make(map[string]map[string]string)
	for _, col := range stored {
		if !strings.HasPrefix(col.Name(), c.Prefix+"__") || col.Name() == c.AttestationRecord.Name ||
			col.Name() == c.VerifiedBlock.Name || col.Version().Query.HasValue() {
			continue
		}
		fields := make(map[string]string)
		for _, f := range col.Version().Fields {
			if strings.HasPrefix(f.Name, "_") || (f.RelationName.HasValue() && f.Kind.IsArray()) {
				continue
			}
			if f.RelationName.HasValue() {
				fields[f.Name] = kindName(f.Kind, col.Name(), names)
			} else {
				fields[f.Name] = withCRDT(f.Kind.String(), f.Typ.String())
			}
		}
		tables[col.Name()] = fields
	}
	return tables
}

// kindName names a stored relation's kind. DefraDB stores a relation by the related table's
// collection ID, or as Self when the table relates to itself, so a relation is named by the
// related table's name, as an SDL names it.
func kindName(k client.FieldKind, table string, names map[string]string) string {
	switch k := k.(type) {
	case *client.CollectionKind:
		return names[k.CollectionID]
	case *client.SelfKind:
		if k.RelativeID != "" {
			return k.String()
		}
		return table
	default:
		return k.String()
	}
}

// withCRDT names a scalar kind with its CRDT type, which is left out when it is the default, LWW.
func withCRDT(kind, crdt string) string {
	if crdt == client.LWW_REGISTER.String() {
		return kind
	}
	return kind + " @crdt(type: " + crdt + ")"
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
