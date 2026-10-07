package schema

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
)

func TestChainApplier(t *testing.T) {
	collections := chain.EVM(testPrefix)
	tables := strings.ReplaceAll(SchemaGraphQL, chain.EthereumMainnet, testPrefix)
	applier := ChainApplier{Tables: tables, Collections: collections}

	want := []string{collections.AttestationRecord.Name}
	for _, c := range collections.Generated() {
		want = append(want, c.Name)
	}
	sort.Strings(want)

	cases := []struct {
		desc    string
		stored  defradb.SchemaApplier
		wantErr error
	}{
		{desc: "empty database", stored: &defradb.MockSchemaApplierThatSucceeds{}},
		{desc: "tables stored", stored: defradb.NewSchemaApplierFromProvidedSchema(tables)},
		{
			desc:   "tables and attestation records stored",
			stored: defradb.NewSchemaApplierFromProvidedSchema(tables + "\n" + attestationRecordSchema(collections)),
		},
		{
			desc:    "only some tables stored",
			stored:  defradb.NewSchemaApplierFromProvidedSchema("type " + collections.Block.Name + " { number: Int }"),
			wantErr: ErrSchemaPartiallyStored,
		},
		{
			desc:    "another chain's tables stored",
			stored:  defradb.NewSchemaApplierFromProvidedSchema(SchemaGraphQL),
			wantErr: ErrSchemaOtherChainStored,
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ctx := context.Background()
			node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, c.stored)
			require.NoError(t, err)
			defer func() { _ = node.Close(ctx) }()

			err = applier.ApplySchema(ctx, node)
			if c.wantErr != nil {
				require.ErrorIs(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)

			cols, err := node.DB.GetCollections(ctx)
			require.NoError(t, err)
			var got []string
			for _, col := range cols {
				got = append(got, col.Name())
			}
			sort.Strings(got)
			require.Equal(t, want, got)

			// The pruner orders attestation records on blockNumber, so it has to be indexed.
			attestations, err := node.DB.GetCollectionByName(ctx, collections.AttestationRecord.Name)
			require.NoError(t, err)
			require.NotEmpty(t, attestations.Version().GetIndexesOnField("blockNumber"))
		})
	}
}
