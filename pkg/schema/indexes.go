package schema

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
)

// indexDirective is the DefraDB directive that declares an index.
const indexDirective = "index"

// applyHostIndexes sets the indexes of sdl, a schema whose types are named "<prefix>__<table>",
// from hostSchema, whose types use Ethereum mainnet's prefix; the host passes its built-in schema.
// A table that hostSchema also declares gets exactly hostSchema's indexes, matched by field. Any
// other table keeps the indexes sdl declares. It returns an error if sdl lacks a field that carries
// an index in hostSchema.
func applyHostIndexes(sdl, prefix, hostSchema string) (string, error) {
	host, err := parser.ParseSchema(&ast.Source{Input: hostSchema})
	if err != nil {
		return "", fmt.Errorf("parse host schema: %w", err)
	}
	doc, err := parser.ParseSchema(&ast.Source{Input: sdl})
	if err != nil {
		return "", fmt.Errorf("parse schema: %w", err)
	}

	hostTypes := make(map[string]*ast.Definition, len(host.Definitions))
	for _, def := range host.Definitions {
		if table, ok := strings.CutPrefix(def.Name, chain.EthereumMainnet+"__"); ok {
			hostTypes[table] = def
		}
	}

	for _, def := range doc.Definitions {
		table, ok := strings.CutPrefix(def.Name, prefix+"__")
		if !ok {
			continue
		}
		hostDef, ok := hostTypes[table]
		if !ok {
			continue
		}

		def.Directives = append(withoutIndexes(def.Directives), hostDef.Directives.ForNames(indexDirective)...)
		for _, f := range def.Fields {
			f.Directives = withoutIndexes(f.Directives)
		}
		for _, hostField := range hostDef.Fields {
			indexes := hostField.Directives.ForNames(indexDirective)
			if len(indexes) == 0 {
				continue
			}
			f := def.Fields.ForName(hostField.Name)
			if f == nil {
				return "", fmt.Errorf("%s.%s: %w", def.Name, hostField.Name, ErrSchemaMissingIndexedField)
			}
			f.Directives = append(f.Directives, indexes...)
		}
	}

	var out bytes.Buffer
	formatter.NewFormatter(&out).FormatSchemaDocument(doc)
	return out.String(), nil
}

// withoutIndexes removes the index directives from directives, reusing its backing array.
func withoutIndexes(directives ast.DirectiveList) ast.DirectiveList {
	kept := directives[:0]
	for _, d := range directives {
		if d.Name != indexDirective {
			kept = append(kept, d)
		}
	}
	return kept
}
