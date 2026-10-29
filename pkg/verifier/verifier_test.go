package verifier

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	logger.Init(true, "")
	ctx := context.Background()
	nb := options.Node().SetDisableAPI(true).SetDisableP2P(true)
	nb.Store().SetPath(t.TempDir())
	n, err := node.New(ctx, nb)
	require.NoError(t, err)
	require.NoError(t, n.Start(ctx))
	t.Cleanup(func() { _ = n.Close(ctx) })

	served, err := os.ReadFile(filepath.Join("..", "schema", "testdata", "generator_tokens_schema.graphql"))
	require.NoError(t, err)
	_, err = n.DB.AddCollection(ctx, string(served))
	require.NoError(t, err)
	for file, table := range map[string]string{"attestationRecord.graphql": "AttestationRecord", "verifiedBlock.graphql": "VerifiedBlock"} {
		sdl, err := os.ReadFile(filepath.Join("..", "schema", file))
		require.NoError(t, err)
		_, err = n.DB.AddCollection(ctx, strings.Replace(string(sdl), "type "+table+" ", "type "+tokens.Prefix+"__"+table+" ", 1))
		require.NoError(t, err)
	}
	return n
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
		_, err := attestation.PostAttestationRecord(context.Background(), n, tokens.AttestationRecord.Name, &constants.AttestationRecord{
			AttestedDocID: fmt.Sprintf("block:%d:%s", height, strings.Join(cids, ",")),
			SourceDocIDs:  []string{signer},
			CIDs:          cids,
			DocType:       "Block",
			VoteCount:     1,
			BlockNumber:   &height,
		})
		require.NoError(t, err)
	}
}

// verified returns the heights the VerifiedBlock table holds, in order.
func verified(t *testing.T, n *node.Node) []int64 {
	t.Helper()
	v := New(n, tokens)
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

			err := New(n, tokens).verify(context.Background(), height)

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

	stop := run(n)
	require.Never(t, func() bool { return len(verified(t, n)) > 0 }, 2*retryInterval, 50*time.Millisecond)

	attest(t, n, 10, signers[1:], cids10)
	require.Eventually(t, func() bool { return len(verified(t, n)) == 2 }, 5*retryInterval, 50*time.Millisecond)
	require.Equal(t, []int64{10, 11}, verified(t, n))
	stop()

	cids12, _ := addBlock(t, n, 12)
	attest(t, n, 12, signers, cids12)
	stop = run(n)
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 3 }, 5*retryInterval, 50*time.Millisecond)
	require.Equal(t, []int64{10, 11, 12}, verified(t, n))
}

// run starts a verifier on n and returns a function that stops it and waits for it to return.
func run(n *node.Node) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		New(n, tokens).Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}
