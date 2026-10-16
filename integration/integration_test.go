//go:build integration

package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

const prefix = "Testchain__Devnet"

// A generator's block reaches the host over P2P, and the host records an attestation for it.
func TestHostAttestsGeneratorBlock(t *testing.T) {
	ctx := context.Background()
	cols := chain.EVM(prefix)
	fixture, err := os.ReadFile("../pkg/schema/testdata/generator_schema.graphql")
	require.NoError(t, err)
	sdl := strings.ReplaceAll(string(fixture), chain.EthereumMainnet, prefix)

	// The stand-in generator: a DefraDB node that publishes the chain's tables, as a generator
	// does, and a server for their schema.
	opts := options.Node().SetDisableAPI(true)
	opts.P2P().SetEnablePubSub(true).SetListenAddresses("/ip4/127.0.0.1/tcp/0")
	opts.Store().SetPath(t.TempDir())
	gen, err := node.New(ctx, opts)
	require.NoError(t, err)
	require.NoError(t, gen.Start(ctx))
	t.Cleanup(func() { _ = gen.Close(context.Background()) })
	_, err = gen.DB.AddCollection(ctx, sdl)
	require.NoError(t, err)
	var generated []string
	for _, col := range cols.Generated() {
		generated = append(generated, col.Name)
	}
	require.NoError(t, gen.DB.AddP2PCollections(ctx, generated))
	genAddrs, err := gen.DB.PeerInfo(ctx)
	require.NoError(t, err)

	schemaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: prefix, Schema: sdl}))
	}))
	t.Cleanup(schemaServer.Close)

	// Pubsub does not replay, so the generator writes only once the host has joined every topic it
	// subscribes to. Listening for join events before the host starts means none is missed.
	joins, err := gen.DB.Events().Subscribe(event.TopicPeerEventName)
	require.NoError(t, err)
	t.Cleanup(func() { defradb.CloseSubscription(gen.DB.Events(), joins) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NoError(t, listener.Close())

	// StartHosting binds the host to the machine's LAN address, so the test needs a default route.
	cfg := *host.DefaultConfig
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.URL = "127.0.0.1:0"
	cfg.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/0"
	cfg.HostConfig.HealthServerPort = healthAddr.Port
	cfg.HostConfig.LensRegistryPath = t.TempDir()
	cfg.Chains = []chain.Config{{
		Prefix:     prefix,
		Generators: []chain.Generator{{URL: schemaServer.URL, Peer: genAddrs[0]}},
	}}
	h, err := host.StartHosting(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, h.Close(ctx))
	})

	hostAddrs, err := h.DefraNode.DB.PeerInfo(ctx)
	require.NoError(t, err)
	hostPeer, err := peer.AddrInfoFromString(hostAddrs[0])
	require.NoError(t, err)
	// A collection's pubsub topic is its collection ID.
	topics := make(map[string]string)
	for _, name := range cols.Subscribed() {
		genCol, err := gen.DB.GetCollectionByName(ctx, name)
		require.NoError(t, err)
		hostCol, err := h.DefraNode.DB.GetCollectionByName(ctx, name)
		require.NoError(t, err)
		require.Equal(t, genCol.CollectionID(), hostCol.CollectionID(), name)
		topics[genCol.CollectionID()] = name
	}
	deadline := time.After(10 * time.Second)
	for len(topics) > 0 {
		select {
		case msg := <-joins.Message():
			joined, ok := msg.Data.(event.TopicPeerEvent)
			if ok && joined.EventType == client.PeerEventTypeJoined && joined.PeerID == hostPeer.ID.String() {
				delete(topics, joined.Topic)
			}
		case <-deadline:
			t.Fatalf("the host did not join the topics of %v", slices.Collect(maps.Values(topics)))
		}
	}

	// The stand-in writes a block's documents, then a BlockSignature over their head CIDs signed with
	// a secp256k1 key, the way the generator's block handler signs a block.
	const blockNumber = 1000
	blockCols := []chain.Collection{cols.Block, cols.Transaction, cols.Log, cols.AccessListEntry}
	collector := node.NewBatchCIDCollector()
	signingCtx := node.ContextWithBatchSigning(ctx, collector)
	for _, col := range blockCols {
		addDocument(signingCtx, t, gen, col.Name, map[string]any{col.HeightField: blockNumber})
	}
	cids := collector.GetCIDs()
	require.Len(t, cids, len(blockCols))

	key, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	root := node.ComputeMerkleRoot(cids)
	signature, err := key.Sign(root)
	require.NoError(t, err)
	signed := make([]string, len(cids))
	for i, id := range cids {
		signed[i] = id.String()
	}
	sort.Strings(signed)
	addDocument(ctx, t, gen, cols.BlockSignature.Name, map[string]any{
		"blockNumber":       blockNumber,
		"merkleRoot":        hex.EncodeToString(root),
		"cidCount":          len(signed),
		"cids":              signed,
		"signatureType":     "ES256K",
		"signatureIdentity": key.GetPublic().String(),
		"signatureValue":    hex.EncodeToString(signature),
	})

	// The host does not compare a record's CIDs with the documents it holds, so the test checks
	// both: the host holds the signed documents, and it records the block as attested by the key.
	type versioned struct {
		Version []struct {
			CID string `json:"cid"`
		} `json:"_version"`
	}
	height := int64(blockNumber)
	want := attestation.Record{
		AttestedDocID: fmt.Sprintf("block:%d:%s", blockNumber, hex.EncodeToString(root)),
		SourceDocIDs:  []string{key.GetPublic().String()},
		CIDs:          signed,
		DocType:       "Block",
		VoteCount:     1,
		BlockNumber:   &height,
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var held []string
		for _, col := range blockCols {
			query := fmt.Sprintf("%s(filter: {%s: {_eq: %d}}) { _version { cid } }", col.Name, col.HeightField, blockNumber)
			docs, err := defradb.QueryArray[versioned](ctx, h.DefraNode, query)
			if !assert.NoError(c, err) {
				return
			}
			for _, doc := range docs {
				for _, v := range doc.Version {
					held = append(held, v.CID)
				}
			}
		}
		assert.ElementsMatch(c, signed, held)

		query := cols.AttestationRecord.Name + " { attested_doc source_doc CIDs doc_type vote_count blockNumber }"
		records, err := defradb.QueryArray[attestation.Record](ctx, h.DefraNode, query)
		if assert.NoError(c, err) {
			assert.Equal(c, []attestation.Record{want}, records)
		}
	}, 10*time.Second, 50*time.Millisecond)
}

// addDocument writes fields as a new document in the collection named collection on n.
func addDocument(ctx context.Context, t *testing.T, n *node.Node, collection string, fields map[string]any) {
	t.Helper()
	col, err := n.DB.GetCollectionByName(ctx, collection)
	require.NoError(t, err)
	doc, err := client.NewDocFromMap(ctx, fields, col.Version())
	require.NoError(t, err)
	require.NoError(t, col.AddDocument(ctx, doc))
}
