package schema

import (
	"context"
	"errors"
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
	errUnavailable := errors.New("tables unavailable")

	want := []string{collections.AttestationRecord.Name}
	for _, c := range collections.Generated() {
		want = append(want, c.Name)
	}
	sort.Strings(want)

	cases := []struct {
		desc      string
		stored    defradb.SchemaApplier
		tablesErr error
		// served is the SDL a generator serves; empty means no generator answers.
		served    string
		wantFetch bool
		wantErr   error
	}{
		{desc: "empty database", stored: &defradb.MockSchemaApplierThatSucceeds{}, wantFetch: true},
		{
			desc:      "empty database, tables unavailable",
			stored:    &defradb.MockSchemaApplierThatSucceeds{},
			tablesErr: errUnavailable,
			wantFetch: true,
			wantErr:   errUnavailable,
		},
		{desc: "tables stored, no generator answers", stored: defradb.NewSchemaApplierFromProvidedSchema(tables)},
		{
			desc:   "tables stored, a generator serves the same tables",
			stored: defradb.NewSchemaApplierFromProvidedSchema(tables),
			served: tables,
		},
		{
			desc:    "tables stored, a generator serves other tables",
			stored:  defradb.NewSchemaApplierFromProvidedSchema(tables),
			served:  tables + "\ntype " + testPrefix + "__Extra { a: Int }",
			wantErr: ErrSchemaDrift,
		},
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

			var fetched bool
			applier := ChainApplier{
				Tables: func(context.Context) (string, error) {
					fetched = true
					return tables, c.tablesErr
				},
				Served:      func(context.Context) (string, bool) { return c.served, c.served != "" },
				Collections: collections,
			}

			err = applier.ApplySchema(ctx, node)
			require.Equal(t, c.wantFetch, fetched)
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

func TestChainApplier_MatchesV070(t *testing.T) {
	// The collections of a v0.7.0 host, read from /api/v0/collections on a production host running
	// it. A collection's ID hashes its name and its fields' names, kinds and CRDT types, so equal IDs
	// mean the same tables. DefraDB also names a subscribed collection's pubsub topic after its ID,
	// so a host with other IDs listens on topics v0.7.0 generators do not publish on. IDs do not
	// cover indexes, so those are compared too.
	wantIDs := map[string]string{
		"Ethereum__Mainnet__AccessListEntry":   "bafyreihipupkpou65ots2fmkdzadqta46qatc4gojnwiaz6qhpca2cfmba",
		"Ethereum__Mainnet__AttestationRecord": "bafyreicmnsr7utuszbl7qgbw7b7gukrx2cate4xxldnjvkequyvvq6vtlu",
		"Ethereum__Mainnet__Block":             "bafyreiftz6gvxpt5kfd5iao3ssro5kt3a4yoaiskzbleygu3ir5kep4va4",
		"Ethereum__Mainnet__BlockSignature":    "bafyreihfwcqxc46ev2vffmkhwou73npv5n5wwt54527no7wvr26u4wujdq",
		"Ethereum__Mainnet__Log":               "bafyreiaomf72pp3gpwxwgs57c25y533igglr25eng3v5bfn2zmeqdotgsu",
		"Ethereum__Mainnet__SnapshotSignature": "bafyreidqkow4wga5lc4gjpjwlith6teccdtmrpdauenws7vbko6higdd5q",
		"Ethereum__Mainnet__Transaction":       "bafyreigtkycjzgns3aczf264nadoj5td4rzjvpbq56ni33nm7v7q3lls6i",
	}
	wantIndexes := []string{
		"AccessListEntry._transactionID(descending=false) unique=false",
		"AccessListEntry.blockNumber(descending=false) unique=false",
		"AttestationRecord.attested_doc(descending=false) unique=false",
		"AttestationRecord.blockNumber(descending=false) unique=false",
		"AttestationRecord.doc_type(descending=false) unique=false",
		"Block.hash(descending=false) unique=true",
		"Block.number(descending=false) unique=false",
		"BlockSignature.blockNumber(descending=false) unique=false",
		"Log._blockID(descending=false) unique=false",
		"Log._transactionID(descending=false) unique=false",
		"Log.address(descending=false) unique=false",
		"Log.blockNumber(descending=false) unique=false",
		"SnapshotSignature.endBlock(descending=false) unique=false",
		"Transaction._blockID(descending=false) unique=false",
		"Transaction.blockNumber(descending=false) unique=false",
		"Transaction.hash(descending=false) unique=true",
	}

	srv := serveSchema(t, validResponse)
	cases := []struct {
		desc   string
		tables func(ctx context.Context) (string, error)
	}{
		{desc: "built-in schema", tables: func(context.Context) (string, error) { return GetSchema(), nil }},
		{
			desc: "generator schema",
			tables: func(ctx context.Context) (string, error) {
				return FetchSchema(ctx, NewSchemaHTTPClient(testSchemaConfig), testIndexerSchemaURL(srv), ethereum)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ctx := context.Background()
			applier := ChainApplier{Tables: c.tables, Collections: ethereum}
			node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, applier)
			require.NoError(t, err)
			defer func() { _ = node.Close(ctx) }()

			cols, err := node.DB.GetCollections(ctx)
			require.NoError(t, err)
			gotIDs := make(map[string]string, len(cols))
			for _, col := range cols {
				gotIDs[col.Name()] = col.CollectionID()
			}
			require.Equal(t, wantIDs, gotIDs)
			require.Equal(t, wantIndexes, collectionIndexes(cols, chain.EthereumMainnet))
		})
	}
}
