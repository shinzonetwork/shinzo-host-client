package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestReadViewsInAppAfterProcessingIndexerPrimitivesWithHost(t *testing.T) {
	// Create a bigPeer to serve as the entrypoint to the network
	bigPeer, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	peerInfo, err := bigPeer.DB.PeerInfo()
	require.NoError(t, err)
	indexerDefra, testIndexer := startIndexer(t, peerInfo)
	defer indexerDefra.Close(t.Context())
	defer testIndexer.StopIndexing()

	logs, err := defra.QueryArray[viewResult](t.Context(), indexerDefra, "Log { transactionHash }")
	require.NoError(t, err)
	require.Greater(t, len(logs), 0)
	testHost := host.CreateHostWithTwoViews(t, peerInfo...)
	defer testHost.Close(t.Context())
	require.Len(t, testHost.HostedViews, 2)

	appConfig := defra.DefaultConfig
	appConfig.DefraDB.P2P.BootstrapPeers = append(appConfig.DefraDB.P2P.BootstrapPeers, peerInfo...)
	appDefra, err := defra.StartDefraInstanceWithTestConfig(t, appConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer appDefra.Close(t.Context())

	for _, view := range testHost.HostedViews {
		err = view.SubscribeTo(t.Context(), appDefra)
		require.NoError(t, err)
		err = attestation.AddAttestationRecordCollection(t.Context(), appDefra, view.Name)
		require.NoError(t, err)
	}

	// Wait until host has received logs
	time.Sleep(10 * time.Second)
	logs, err = defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, "Log { transactionHash }")
	require.NoError(t, err)
	for len(logs) == 0 {
		time.Sleep(1 * time.Second)
		logs, err = defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, "Log { transactionHash }")
		require.NoError(t, err)
	}
	require.Greater(t, len(logs), 0)

	// Check host has lens results
	unfilteredQuery := fmt.Sprintf("%s { transactionHash }", testHost.HostedViews[0].Name)
	unfilteredResults, err := defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, unfilteredQuery)
	require.NoError(t, err)
	require.Greater(t, len(unfilteredResults), 0)

	filteredQuery := fmt.Sprintf("%s { transactionHash }", testHost.HostedViews[1].Name)
	filteredResults, err := defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, filteredQuery)
	require.NoError(t, err)
	require.NotNil(t, filteredResults)
	require.Greater(t, len(unfilteredResults), len(filteredResults))

	// Wait for app defra to receive lens results
	unfilteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
	require.NoError(t, err)
	for len(unfilteredResults) == 0 {
		time.Sleep(1 * time.Second)
		unfilteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
		require.NoError(t, err)
	}
	require.Greater(t, len(unfilteredResults), 0)

	filteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, filteredQuery)
	require.NoError(t, err)
	require.NotNil(t, filteredResults)
	require.Greater(t, len(unfilteredResults), len(filteredResults))

	time.Sleep(90 * time.Second) // Allow more blocks to process

	newUnfilteredResults, err := defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
	require.NoError(t, err)
	require.Greater(t, len(newUnfilteredResults), len(unfilteredResults))

	// Now let's also check that we have attestations for a view
	attestationRecordsQuery := fmt.Sprintf(`query {
	AttestationRecord_%s {
		attested_doc
		source_doc
		CIDs
	}
}`, testHost.HostedViews[0].Name)
	attestationRecords, err := defra.QueryArray[attestation.AttestationRecord](t.Context(), appDefra, attestationRecordsQuery)
	require.NoError(t, err)
	require.NotNil(t, attestationRecords)
	require.Greater(t, len(attestationRecords), 0)
	for _, record := range attestationRecords {
		require.NotNil(t, record.CIDs)
		require.Greater(t, len(record.CIDs), 0)
		require.Greater(t, len(record.AttestedDocId), 0)
		require.Greater(t, len(record.SourceDocId), 0)
	}
}

type viewResult struct {
	TransactionHash string `json:"transactionHash"`
}

func startIndexer(t *testing.T, bigPeer []string) (*node.Node, *indexer.ChainIndexer) {
	defraConfig := defra.DefaultConfig
	defraConfig.DefraDB.P2P.BootstrapPeers = append(defraConfig.DefraDB.P2P.BootstrapPeers, bigPeer...)
	indexerDefra, err := defra.StartDefraInstanceWithTestConfig(t, defraConfig, &defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}, "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)
	testConfig := indexer.DefaultConfig
	testConfig.Geth = host.GetGethConfig()
	i, err := indexer.CreateIndexer(testConfig)
	require.NoError(t, err)
	go func() {
		err := i.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()

	for !i.IsStarted() || !i.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	return indexerDefra, i
}
