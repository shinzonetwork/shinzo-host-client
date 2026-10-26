package schema

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
)

const testPrefix = "Testchain__Devnet"

var indexDirectivePattern = regexp.MustCompile(`\s*@index(\([^)]*\))?`)

func TestApplyHostIndexes(t *testing.T) {
	// A generator's schema for another chain: the built-in types under its prefix, without the
	// built-in indexes, with indexes the host does not declare (on a Block field, on the Block type
	// and on the Transaction type), and with a table named without the prefix, which the host's
	// indexes do not match.
	generatorSchema := indexDirectivePattern.ReplaceAllString(SchemaGraphQL, "")
	generatorSchema = strings.ReplaceAll(generatorSchema, chain.EthereumMainnet, testPrefix)
	generatorSchema = strings.Replace(generatorSchema, "miner: String", "miner: String @index", 1)
	generatorSchema = strings.Replace(generatorSchema, "type "+testPrefix+"__Block {",
		"type "+testPrefix+`__Block @index(includes: [{field: "miner"}, {field: "nonce"}]) {`, 1)
	generatorSchema = strings.Replace(generatorSchema, "type "+testPrefix+"__Transaction {",
		"type "+testPrefix+`__Transaction @index(includes: [{field: "hash"}]) {`, 1)
	generatorSchema += "\ntype Block { number: Int hash: String }\n"

	cases := []struct {
		desc       string
		hostSchema string
		// kept are the generator's indexes on tables hostSchema does not have.
		kept []string
	}{
		{desc: "built-in schema", hostSchema: SchemaGraphQL},
		{
			desc:       "host index on a type",
			hostSchema: `type Ethereum__Mainnet__Block @index(includes: [{field: "number"}, {field: "hash"}]) { number: Int hash: String }`,
			kept:       []string{"Transaction.hash(descending=false) unique=false"},
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			got, err := applyHostIndexes(generatorSchema, testPrefix, c.hostSchema)
			require.NoError(t, err)

			// Applied to DefraDB, the result has the host schema's indexes and the kept ones.
			want := appliedIndexes(t, c.hostSchema, chain.EthereumMainnet)
			require.NotEmpty(t, want)
			want = append(want, c.kept...)
			sort.Strings(want)
			require.Equal(t, want, appliedIndexes(t, got, testPrefix))
		})
	}
}

func TestApplyHostIndexes_MissingIndexedField(t *testing.T) {
	sdl := strings.Replace(SchemaGraphQL, "number: Int @index", "", 1)

	_, err := applyHostIndexes(sdl, chain.EthereumMainnet, SchemaGraphQL)
	require.ErrorIs(t, err, ErrSchemaMissingIndexedField)
}

// appliedIndexes applies sdl to a new DefraDB node and returns its indexes as collectionIndexes
// lists them.
func appliedIndexes(t *testing.T, sdl, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, defradb.NewSchemaApplierFromProvidedSchema(sdl))
	require.NoError(t, err)
	defer func() { _ = node.Close(ctx) }()

	cols, err := node.DB.GetCollections(ctx)
	require.NoError(t, err)
	return collectionIndexes(cols, prefix)
}

// collectionIndexes returns the indexes of cols as "<table>.<fields> unique=<bool>", sorted, with
// prefix removed from the collection names.
func collectionIndexes(cols []client.Collection, prefix string) []string {
	var indexes []string
	for _, col := range cols {
		table := strings.TrimPrefix(col.Name(), prefix+"__")
		for _, index := range col.Version().Indexes {
			var fields []string
			for _, f := range index.Fields {
				fields = append(fields, fmt.Sprintf("%s(descending=%v)", f.Name, f.Descending))
			}
			indexes = append(indexes, fmt.Sprintf("%s.%s unique=%v", table, strings.Join(fields, ","), index.Unique))
		}
	}
	sort.Strings(indexes)
	return indexes
}
