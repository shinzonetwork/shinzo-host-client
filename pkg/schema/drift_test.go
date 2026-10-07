package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
)

func TestCheckStored(t *testing.T) {
	ctx := context.Background()
	collections := chain.EVM(testPrefix)
	// The built-in tables under testPrefix, with a float field, and a table that relates to itself.
	builtIn := strings.ReplaceAll(SchemaGraphQL, chain.EthereumMainnet, testPrefix)
	builtIn = strings.Replace(builtIn, "miner: String", "miner: String\n    gasRatio: Float64", 1)
	node := testPrefix + "__Node"
	tables := builtIn + "\ntype " + node + " { name: String parent: " + node + ` @relation(name: "tree") children: [` + node + `] @relation(name: "tree") }`

	// The store also holds the host's attestation records and a view named under the prefix.
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig,
		defradb.NewSchemaApplierFromProvidedSchema(tables+"\n"+attestationRecordSchema(collections)))
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()
	_, err = defraNode.DB.AddView(ctx, collections.Block.Name+" { number }", "type "+testPrefix+"__Summary { number: Int }")
	require.NoError(t, err)
	stored, err := defraNode.DB.GetCollections(ctx)
	require.NoError(t, err)

	cases := []struct {
		desc     string
		served   string
		wantDiff string
	}{
		{desc: "same tables", served: tables},
		{
			desc:   "same tables, written without indexes and with Float for Float64",
			served: strings.Replace(indexDirectivePattern.ReplaceAllString(tables, ""), "Float64", "Float", 1),
		},
		{
			desc:     "a field added",
			served:   strings.Replace(tables, "miner: String", "miner: String extra: String", 1),
			wantDiff: collections.Block.Name + ".extra is served but not stored",
		},
		{
			desc:     "a field removed",
			served:   strings.Replace(tables, "miner: String", "", 1),
			wantDiff: collections.Block.Name + ".miner is stored but not served",
		},
		{
			desc:     "a field's kind changed",
			served:   strings.Replace(tables, "number: Int @index", "number: String @index", 1),
			wantDiff: collections.Block.Name + ".number is stored as Int and served as String",
		},
		{
			desc:     "a relation to another table",
			served:   strings.Replace(tables, "block: "+collections.Block.Name+` @index @relation(name: "block_logs")`, "block: "+collections.Transaction.Name, 1),
			wantDiff: collections.Log.Name + ".block is stored as " + collections.Block.Name + " and served as " + collections.Transaction.Name,
		},
		{
			desc:     "a table added",
			served:   tables + "\ntype " + testPrefix + "__Extra { a: Int }",
			wantDiff: testPrefix + "__Extra is served but not stored",
		},
		{
			desc:     "a table removed",
			served:   builtIn,
			wantDiff: node + " is stored but not served",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			err := checkStored(c.served, stored, collections)
			if c.wantDiff == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrSchemaDrift)
			require.ErrorContains(t, err, c.wantDiff)
		})
	}
}
