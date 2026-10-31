package verifier

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const usd1 = "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"

var tokens = chain.Tokens("Bsc__Mainnet", []string{usd1})

// newVerifierNode starts a DefraDB node holding a tokens-mode chain's tables as a host stores them:
// the tables a generator serves, from the served schema fixture, and the host's own tables.
func newVerifierNode(t *testing.T) *node.Node {
	t.Helper()
	n := newServedNode(t, false)
	addHostTables(t, n)
	return n
}

// newServedNode starts a DefraDB node holding the tables a tokens-mode generator serves, with P2P on
// a local port when p2p is set.
func newServedNode(t *testing.T, p2p bool) *node.Node {
	t.Helper()
	logger.Init(true, "")
	ctx := context.Background()
	nb := options.Node().SetDisableAPI(true).SetDisableP2P(!p2p)
	if p2p {
		nb.P2P().SetListenAddresses("/ip4/127.0.0.1/tcp/0").SetEnablePubSub(true)
	}
	nb.Store().SetPath(t.TempDir())
	n, err := node.New(ctx, nb)
	require.NoError(t, err)
	require.NoError(t, n.Start(ctx))
	t.Cleanup(func() { _ = n.Close(ctx) })

	served, err := os.ReadFile(filepath.Join("..", "schema", "testdata", "generator_tokens_schema.graphql"))
	require.NoError(t, err)
	_, err = n.DB.AddCollection(ctx, string(served))
	require.NoError(t, err)
	return n
}

// addHostTables adds the tables a host keeps for itself, which no generator serves.
func addHostTables(t *testing.T, n *node.Node) {
	t.Helper()
	for file, table := range map[string]string{"attestationRecord.graphql": "AttestationRecord", "verifiedBlock.graphql": "VerifiedBlock"} {
		sdl, err := os.ReadFile(filepath.Join("..", "schema", file))
		require.NoError(t, err)
		_, err = n.DB.AddCollection(context.Background(), strings.Replace(string(sdl), "type "+table+" ", "type "+tokens.Prefix+"__"+table+" ", 1))
		require.NoError(t, err)
	}
}

// countNothing is the count function of verifiers whose tests attest every signature themselves.
func countNothing(context.Context, []string) {}

// listNothing is the documents function of verifiers whose tests have no generators.
func listNothing(context.Context, int64, int64) (map[string][]string, error) {
	return map[string][]string{}, nil
}

// add writes a document and returns its ID and CID.
func add(t *testing.T, n *node.Node, collection string, fields map[string]any) (client.DocID, string) {
	t.Helper()
	ctx := context.Background()
	col, err := n.DB.GetCollectionByName(ctx, collection)
	require.NoError(t, err)
	doc, err := client.NewDocFromMap(ctx, fields, col.Version())
	require.NoError(t, err)
	require.NoError(t, col.AddDocument(ctx, doc))
	return doc.ID(), currentCID(t, n, collection, doc.ID())
}

// currentCID returns the CID of a document's current version.
func currentCID(t *testing.T, n *node.Node, collection string, id client.DocID) string {
	t.Helper()
	res := n.DB.ExecRequest(context.Background(),
		fmt.Sprintf(`query { %s(docID: %q) { _version { cid } } }`, collection, id.String()))
	require.Empty(t, res.GQL.Errors)
	docs, ok := res.GQL.Data.(map[string]any)[collection].([]map[string]any)
	require.True(t, ok)
	require.Len(t, docs, 1)
	versions, ok := docs[0]["_version"].([]map[string]any)
	require.True(t, ok)
	require.Len(t, versions, 1)
	return versions[0]["cid"].(string)
}

// addBlock writes a Header and two USD1 logs at height and returns their CIDs and the log IDs.
func addBlock(t *testing.T, n *node.Node, height int64) (cids []string, logs []client.DocID) {
	t.Helper()
	_, header := add(t, n, tokens.Header.Name, map[string]any{"number": height, "hash": fmt.Sprintf("0x%064x", height)})
	cids = append(cids, header)
	for i := range 2 {
		id, cid := add(t, n, tokens.ContractLogs[0].Name, map[string]any{"blockNumber": height, "logIndex": i, "address": usd1})
		cids = append(cids, cid)
		logs = append(logs, id)
	}
	return cids, logs
}

// attest records each of signers signing cids at height through the host's attestation write, which
// merges the signers of the same signed documents into one record.
func attest(t *testing.T, n *node.Node, height int64, signers, cids []string) {
	t.Helper()
	for _, signer := range signers {
		require.NoError(t, postAttestation(context.Background(), n, height, signer, cids))
	}
}

// postAttestation records signer signing cids at height.
func postAttestation(ctx context.Context, n *node.Node, height int64, signer string, cids []string) error {
	_, err := attestation.PostAttestationRecord(ctx, n, tokens.AttestationRecord.Name, &constants.AttestationRecord{
		AttestedDocID: fmt.Sprintf("block:%d:%s", height, strings.Join(cids, ",")),
		SourceDocIDs:  []string{signer},
		CIDs:          cids,
		DocType:       "Block",
		VoteCount:     1,
		BlockNumber:   &height,
	})
	return err
}

// verified returns the heights the VerifiedBlock table holds, in order.
func verified(t *testing.T, n *node.Node) []int64 {
	t.Helper()
	v := New(n, tokens, countNothing, listNothing)
	rows, err := v.query(context.Background(),
		fmt.Sprintf(`query { blocks: %s(order: {blockNumber: ASC}) { height: blockNumber } }`, tokens.VerifiedBlock.Name))
	require.NoError(t, err)
	heights := []int64{}
	for _, r := range rows["blocks"] {
		heights = append(heights, r.Height)
	}
	return heights
}

func TestVerify(t *testing.T) {
	const height = 100
	signers := []string{"02aa", "03bb"}
	cases := []struct {
		name    string
		setup   func(t *testing.T, n *node.Node)
		wantErr error
	}{
		{
			name: "two signers, every signed document held",
			setup: func(t *testing.T, n *node.Node) {
				cids, _ := addBlock(t, n, height)
				attest(t, n, height, signers, cids)
			},
		},
		{
			name: "one signer",
			setup: func(t *testing.T, n *node.Node) {
				cids, _ := addBlock(t, n, height)
				attest(t, n, height, signers[:1], cids)
			},
			wantErr: errTooFewSigners,
		},
		{
			name: "signers agree on two versions of the block",
			setup: func(t *testing.T, n *node.Node) {
				cids, _ := addBlock(t, n, height)
				attest(t, n, height, signers, cids)
				attest(t, n, height, []string{"02cc", "03dd"}, cids[:2])
			},
			wantErr: errSignersDisagree,
		},
		{
			name: "a signed document is missing",
			setup: func(t *testing.T, n *node.Node) {
				cids, _ := addBlock(t, n, height)
				_, elsewhere := add(t, n, tokens.ContractLogs[0].Name, map[string]any{"blockNumber": height + 1, "logIndex": 0})
				attest(t, n, height, signers, append(cids, elsewhere))
			},
			wantErr: errDocumentsDiffer,
		},
		{
			name: "a signed document was edited",
			setup: func(t *testing.T, n *node.Node) {
				ctx := context.Background()
				cids, logs := addBlock(t, n, height)
				attest(t, n, height, signers, cids)
				col, err := n.DB.GetCollectionByName(ctx, tokens.ContractLogs[0].Name)
				require.NoError(t, err)
				doc, err := col.GetDocument(ctx, logs[0])
				require.NoError(t, err)
				require.NoError(t, doc.Set(ctx, "data", "0x5000"))
				require.NoError(t, col.UpdateDocument(ctx, doc))
			},
			wantErr: errDocumentsDiffer,
		},
		{
			name: "the signed documents have no Header",
			setup: func(t *testing.T, n *node.Node) {
				_, log := add(t, n, tokens.ContractLogs[0].Name, map[string]any{"blockNumber": height, "logIndex": 0})
				attest(t, n, height, signers, []string{log})
			},
			wantErr: errDocumentsDiffer,
		},
		{
			name: "an unsigned document is held",
			setup: func(t *testing.T, n *node.Node) {
				cids, _ := addBlock(t, n, height)
				attest(t, n, height, signers, cids)
				add(t, n, tokens.ContractLogs[0].Name, map[string]any{"blockNumber": height, "logIndex": 2})
			},
			wantErr: errDocumentsDiffer,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newVerifierNode(t)
			tc.setup(t, n)

			err := New(n, tokens, countNothing, listNothing).verify(context.Background(), height)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Empty(t, verified(t, n))
				return
			}
			require.NoError(t, err)
			require.Equal(t, []int64{height}, verified(t, n))
		})
	}
}

// Run verifies in height order: a complete height waits behind an incomplete one, both pass once
// it completes, and a restart resumes above the highest verified height.
func TestRun(t *testing.T) {
	n := newVerifierNode(t)
	signers := []string{"02aa", "03bb"}
	cids10, _ := addBlock(t, n, 10)
	attest(t, n, 10, signers[:1], cids10)
	cids11, _ := addBlock(t, n, 11)
	attest(t, n, 11, signers, cids11)

	stop := run(New(n, tokens, countNothing, listNothing))
	require.Never(t, func() bool { return len(verified(t, n)) > 0 }, 2*retryInterval, 50*time.Millisecond)

	attest(t, n, 10, signers[1:], cids10)
	require.Eventually(t, func() bool { return len(verified(t, n)) == 2 }, 5*retryInterval, 50*time.Millisecond)
	require.Equal(t, []int64{10, 11}, verified(t, n))
	stop()

	cids12, _ := addBlock(t, n, 12)
	attest(t, n, 12, signers, cids12)
	stop = run(New(n, tokens, countNothing, listNothing))
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 3 }, 5*retryInterval, 50*time.Millisecond)
	require.Equal(t, []int64{10, 11, 12}, verified(t, n))
}

// A height short of signers hands count the held signatures no attestation record counts yet, each
// document once: here 03bb's signature arrived without being attested, as after a document sync.
func TestRunCountsHeldSignatures(t *testing.T) {
	cases := []struct {
		name         string
		attest       bool
		wantVerified []int64
	}{
		{"counting the signature verifies the block", true, []int64{10}},
		{"a signature still uncounted is not handed again", false, []int64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newVerifierNode(t)
			cids, _ := addBlock(t, n, 10)
			attest(t, n, 10, []string{"02aa"}, cids)
			add(t, n, tokens.BlockSignature.Name, map[string]any{"blockNumber": 10, "signatureIdentity": "02aa"})
			held, _ := add(t, n, tokens.BlockSignature.Name, map[string]any{"blockNumber": 10, "signatureIdentity": "03bb"})

			var handed [][]string
			var attestErr error
			count := func(ctx context.Context, ids []string) {
				handed = append(handed, ids)
				if tc.attest {
					attestErr = postAttestation(ctx, n, 10, "03bb", cids)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				New(n, tokens, count, listNothing).Run(ctx)
			}()
			time.Sleep(3 * retryInterval)
			cancel()
			<-done

			require.NoError(t, attestErr)
			require.Equal(t, [][]string{{held.String()}}, handed)
			require.Equal(t, tc.wantVerified, verified(t, n))
		})
	}
}

// Signatures counted at a height are checked at once, so heights waiting only on counting pass
// without a retryInterval each.
func TestRunChecksAgainAfterCounting(t *testing.T) {
	n := newVerifierNode(t)
	cids := make(map[int64][]string)
	heights := make(map[string]int64)
	for h := int64(10); h < 15; h++ {
		cids[h], _ = addBlock(t, n, h)
		attest(t, n, h, []string{"02aa"}, cids[h])
		id, _ := add(t, n, tokens.BlockSignature.Name, map[string]any{"blockNumber": h, "signatureIdentity": "03bb"})
		heights[id.String()] = h
	}
	var countErr error
	count := func(ctx context.Context, ids []string) {
		for _, id := range ids {
			if err := postAttestation(ctx, n, heights[id], "03bb", cids[heights[id]]); err != nil {
				countErr = err
			}
		}
	}

	stop := run(New(n, tokens, count, listNothing))
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 5 }, 2*retryInterval, 20*time.Millisecond)
	stop()
	require.NoError(t, countErr)
}

// A height is repaired only after it has waited repairAfter while the host holds a later block, and
// the repair covers repairHeights heights from it up.
func TestRunRepairsOnlyStuckHeights(t *testing.T) {
	cases := []struct {
		name        string
		laterBlock  bool
		repairAfter time.Duration
		wantRepair  bool
	}{
		{name: "stuck behind a later block", laterBlock: true, repairAfter: 200 * time.Millisecond, wantRepair: true},
		{name: "no later block held", repairAfter: 200 * time.Millisecond},
		{name: "not stuck for long enough", laterBlock: true, repairAfter: time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newVerifierNode(t)
			cids, _ := addBlock(t, n, 10)
			attest(t, n, 10, []string{"02aa"}, cids)
			if tc.laterBlock {
				addBlock(t, n, 11)
			}
			var mu sync.Mutex
			var repaired [][2]int64
			v := New(n, tokens, countNothing, func(_ context.Context, from, to int64) (map[string][]string, error) {
				mu.Lock()
				defer mu.Unlock()
				repaired = append(repaired, [2]int64{from, to})
				return map[string][]string{}, nil
			})
			v.repairAfter = tc.repairAfter
			stop := run(v)
			defer stop()

			repairs := func() [][2]int64 {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(repaired)
			}
			if !tc.wantRepair {
				require.Never(t, func() bool { return len(repairs()) > 0 }, 2*retryInterval, 50*time.Millisecond)
				return
			}
			require.Eventually(t, func() bool { return len(repairs()) > 0 }, 2*retryInterval, 50*time.Millisecond)
			require.Equal(t, [2]int64{10, 10 + repairHeights - 1}, repairs()[0])
			// A height still stuck after a repair waits repairAfter again before the next one.
			time.Sleep(retryInterval)
			require.LessOrEqual(t, len(repairs()), int(retryInterval/tc.repairAfter)+2)
		})
	}
}

// A signature the host never received is fetched from the generator that holds it, counted, and the
// block verified.
func TestRunRepairsMissingSignature(t *testing.T) {
	ctx := context.Background()
	generator := newServedNode(t, true)
	host := newServedNode(t, true)
	addHostTables(t, host)
	for _, n := range []*node.Node{generator, host} {
		addBlock(t, n, 11)
		add(t, n, tokens.BlockSignature.Name, map[string]any{"blockNumber": 10, "signatureIdentity": "02aa"})
	}
	cids, _ := addBlock(t, generator, 10)
	addBlock(t, host, 10)
	attest(t, host, 10, []string{"02aa"}, cids)
	missing, _ := add(t, generator, tokens.BlockSignature.Name, map[string]any{"blockNumber": 10, "signatureIdentity": "03bb"})

	addresses, err := generator.DB.PeerInfo(ctx)
	require.NoError(t, err)
	require.NoError(t, host.DB.Connect(ctx, addresses))
	headerID := documentIDs(ctx, t, generator, tokens.Header, 10)[0]
	require.Eventually(t, func() bool {
		syncCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return host.DB.SyncDocuments(syncCtx, tokens.Header.Name, []string{headerID}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the generator never answered a document sync")

	var countErr error
	count := func(ctx context.Context, ids []string) {
		if slices.Equal(ids, []string{missing.String()}) {
			countErr = postAttestation(ctx, host, 10, "03bb", cids)
		}
	}
	documents := func(ctx context.Context, from, to int64) (map[string][]string, error) {
		listed := make(map[string][]string)
		for _, col := range tokens.Generated() {
			for h := from; h <= min(to, 11); h++ {
				listed[col.Name] = append(listed[col.Name], documentIDs(ctx, t, generator, col, h)...)
			}
		}
		return listed, nil
	}
	v := New(host, tokens, count, documents)
	v.repairAfter = 200 * time.Millisecond
	stop := run(v)
	defer stop()
	require.Eventually(t, func() bool { return slices.Contains(verified(t, host), 10) }, 10*retryInterval, 50*time.Millisecond)
	stop()
	require.NoError(t, countErr)
}

// documentIDs returns the IDs of the documents n holds in col at height.
func documentIDs(ctx context.Context, t *testing.T, n *node.Node, col chain.Collection, height int64) []string {
	t.Helper()
	res := n.DB.ExecRequest(ctx,
		fmt.Sprintf(`query { %s(filter: {%s: {_eq: %d}}) { _docID } }`, col.Name, col.HeightField, height))
	require.Empty(t, res.GQL.Errors)
	var ids []string
	for _, doc := range res.GQL.Data.(map[string]any)[col.Name].([]map[string]any) {
		ids = append(ids, doc["_docID"].(string))
	}
	return ids
}

// run starts v and returns a function that stops it and waits for it to return.
func run(v *Verifier) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		v.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}
