package verifier

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/shinzonetwork/shinzo-evidence-codec/merkle"
	"github.com/shinzonetwork/shinzo-evidence-codec/rule"
	"github.com/shinzonetwork/shinzo-evidence-codec/sig"
	"github.com/shinzonetwork/shinzo-evidence-codec/statement"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const usd1 = "8d0d000ee44948fc98c9b98a4fa4921476f08b0d"

var tokens = chain.Tokens("Bsc__Mainnet")

// keys are the test signers: the first two are listed in tokensConfig.
var keys = func() []*secp256k1.PrivateKey {
	var ks []*secp256k1.PrivateKey
	for i := range 3 {
		seed := sha256.Sum256([]byte{byte(i)})
		ks = append(ks, secp256k1.PrivKeyFromBytes(seed[:]))
	}
	return ks
}()

func signer(i int) string {
	return hex.EncodeToString(keys[i].PubKey().SerializeCompressed())
}

// listedSigners are the listed signers in the order a verdict reports them.
func listedSigners() []string {
	return slices.Sorted(slices.Values([]string{signer(0), signer(1)}))
}

// tokensConfig returns a tokens-mode chain on Ethereum with two of two listed signers and USD1,
// whose verification starts at start.
func tokensConfig(start chain.Start) chain.Config {
	return chain.Config{
		Prefix: tokens.Prefix, Mode: chain.ModeTokens, CAIP2: "eip155:1",
		Signers: []string{signer(0), signer(1)}, Required: 2,
		Assets: []chain.Asset{{ID: usd1, SupplySlot: "0x35"}},
		Start:  start,
	}
}

// newVerifierNode starts a DefraDB node holding a tokens-mode chain's tables as a host stores them:
// the tables a generator serves and the host's VerifiedBlock.
func newVerifierNode(t *testing.T) *node.Node {
	t.Helper()
	n := newServedNode(t, false)
	addVerifiedBlockTable(t, n)
	return n
}

// addVerifiedBlockTable adds the host's VerifiedBlock table, which no generator serves.
func addVerifiedBlockTable(t *testing.T, n *node.Node) {
	t.Helper()
	sdl, err := os.ReadFile(filepath.Join("..", "schema", "verifiedBlock.graphql"))
	require.NoError(t, err)
	_, err = n.DB.AddCollection(context.Background(), strings.Replace(string(sdl), "type VerifiedBlock ", "type "+tokens.VerifiedBlock.Name+" ", 1))
	require.NoError(t, err)
}

// newServedNode starts a DefraDB node holding the tables a tokens-mode generator serves, from the
// served schema fixture, with P2P on a local port when p2p is set.
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

func newVerifier(t *testing.T, n *node.Node, start chain.Start) *Verifier {
	t.Helper()
	v, err := New(n, tokensConfig(start), tokens, Hooks{})
	require.NoError(t, err)
	return v
}

// add writes a document and returns its ID.
func add(t *testing.T, n *node.Node, collection string, fields map[string]any) client.DocID {
	t.Helper()
	ctx := context.Background()
	col, err := n.DB.GetCollectionByName(ctx, collection)
	require.NoError(t, err)
	doc, err := client.NewDocFromMap(ctx, fields, col.Version())
	require.NoError(t, err)
	require.NoError(t, col.AddDocument(ctx, doc))
	return doc.ID()
}

// block is a synthetic Ethereum block: its raw header and the statement an honest generator signs
// over it, which claims no USD1 records, so no Evidence document is needed.
type block struct {
	raw       []byte
	statement statement.Statement
}

// newBlock returns a block at number whose parent has hash parent. time sets the header's time, so
// two blocks at one height can differ.
func newBlock(t *testing.T, number uint64, parent string, time uint64) block {
	t.Helper()
	h := types.Header{
		ParentHash:  common.HexToHash(parent),
		UncleHash:   types.EmptyUncleHash,
		TxHash:      types.EmptyTxsHash,
		ReceiptHash: types.EmptyReceiptsHash,
		Difficulty:  big.NewInt(0),
		Number:      new(big.Int).SetUint64(number),
		Time:        time,
	}
	raw, err := rlp.EncodeToBytes(&h)
	require.NoError(t, err)
	return block{raw: raw, statement: statement.Statement{
		Chain: "eip155:1", Number: number, Hash: crypto.Keccak256Hash(raw).Hex(), ParentHash: h.ParentHash.Hex(),
		ParentNumber: number - 1, Timestamp: time * 1000, Status: statement.StatusFinalized, Extractor: "test",
		Filter: statement.FilterContractLog, HeaderHash: sha256.Sum256(raw),
		Claims: []statement.Claim{{Asset: usd1, Root: merkle.Root(nil)}},
	}}
}

// addHeader writes b's Header document.
func addHeader(t *testing.T, n *node.Node, b block) {
	t.Helper()
	s := b.statement
	add(t, n, tokens.Header.Name, map[string]any{
		"number": int64(s.Number), "hash": s.Hash, "parentHash": s.ParentHash, "timestamp": int64(s.Timestamp),
		"raw": hex.EncodeToString(b.raw),
	})
}

// attest writes key i's BlockAttestation over s, signed as defradb signs: DER over the SHA-256 of
// the statement.
func attest(t *testing.T, n *node.Node, i int, s statement.Statement) client.DocID {
	t.Helper()
	encoded, err := s.Encode()
	require.NoError(t, err)
	hash := sha256.Sum256(encoded)
	return add(t, n, tokens.BlockAttestation.Name, map[string]any{
		"blockNumber": int64(s.Number), "blockHash": s.Hash, "statement": hex.EncodeToString(encoded),
		"signatureType": sig.TypeES256K, "signer": signer(i), "signature": hex.EncodeToString(ecdsa.Sign(keys[i], hash[:]).Serialize()),
	})
}

// addAttestedBlock writes b's Header and both listed signers' attestations over it.
func addAttestedBlock(t *testing.T, n *node.Node, b block) {
	t.Helper()
	addHeader(t, n, b)
	attest(t, n, 0, b.statement)
	attest(t, n, 1, b.statement)
}

// verifiedBlock is a VerifiedBlock document.
type verifiedBlock struct {
	Height  int64    `json:"blockNumber"`
	Hash    string   `json:"blockHash"`
	Signers []string `json:"signers"`
	Assets  []string `json:"assets"`
}

// verified returns the VerifiedBlock documents, in height order.
func verified(t *testing.T, n *node.Node) []verifiedBlock {
	t.Helper()
	res := n.DB.ExecRequest(context.Background(),
		fmt.Sprintf(`query { %s(order: {blockNumber: ASC}) { blockNumber blockHash signers assets } }`, tokens.VerifiedBlock.Name))
	require.Empty(t, res.GQL.Errors)
	data, err := json.Marshal(res.GQL.Data.(map[string]any)[tokens.VerifiedBlock.Name])
	require.NoError(t, err)
	blocks := []verifiedBlock{}
	require.NoError(t, json.Unmarshal(data, &blocks))
	return blocks
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

// vector is the codec's Ethereum vector for block 26378186: the documents a host holds for it, as
// hex, and the statement both signers sign.
type vector struct {
	RawHeader  hexutil.Bytes `json:"rawHeader"`
	Records    hexutil.Bytes `json:"records"`
	Proof      hexutil.Bytes `json:"proof"`
	StateProof hexutil.Bytes `json:"stateProof"`
	Supply     string        `json:"supply"`
	Statement  hexutil.Bytes `json:"statement"`
}

// The height verifies: the codec's Ethereum vector, with a USD1 Evidence document and a
// supply StateCapture, is verified at its start height and recorded with its signers.
func TestRunVerifiesTheStartHeight(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "ethereum-26378186.json"))
	require.NoError(t, err)
	var vec vector
	require.NoError(t, json.Unmarshal(b, &vec))
	s, err := statement.Decode(vec.Statement)
	require.NoError(t, err)

	n := newVerifierNode(t)
	add(t, n, tokens.Header.Name, map[string]any{
		"number": int64(s.Number), "hash": s.Hash, "parentHash": s.ParentHash, "timestamp": int64(s.Timestamp),
		"raw": hex.EncodeToString(vec.RawHeader),
	})
	add(t, n, tokens.Evidence.Name, map[string]any{
		"blockNumber": int64(s.Number), "blockHash": s.Hash, "asset": usd1,
		"records": hex.EncodeToString(vec.Records), "proof": hex.EncodeToString(vec.Proof),
	})
	add(t, n, tokens.StateCapture.Name, map[string]any{
		"blockNumber": int64(s.Number), "blockHash": s.Hash, "asset": usd1, "value": vec.Supply,
		"proof": hex.EncodeToString(vec.StateProof),
	})
	attest(t, n, 0, s)
	attest(t, n, 1, s)

	stop := run(newVerifier(t, n, chain.Start{Block: int64(s.Number), Hash: s.Hash}))
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 1 }, 5*retryInterval, 50*time.Millisecond)
	require.Equal(t, []verifiedBlock{{Height: int64(s.Number), Hash: s.Hash, Signers: listedSigners(), Assets: []string{usd1}}}, verified(t, n))
}

func TestVerify(t *testing.T) {
	const height = 100
	b := newBlock(t, height, "0x99", 1_700_000_000)
	start := chain.Start{Block: height, Hash: b.statement.Hash}
	cases := []struct {
		name    string
		setup   func(t *testing.T, n *node.Node)
		start   chain.Start
		parent  *rule.Block
		wantErr error
		// wantReason is the rule's reason when wantErr is errNotAccepted.
		wantReason string
	}{
		{
			// Two Header documents at one height: the agreed one is picked.
			name: "another Header at the height beside the agreed one",
			setup: func(t *testing.T, n *node.Node) {
				addHeader(t, n, newBlock(t, height, "0x99", 1_700_000_001))
				addAttestedBlock(t, n, b)
			},
			start: start,
		},
		{
			// The start.hash anchor.
			name:    "the agreed block at the start height is not start.hash",
			setup:   func(t *testing.T, n *node.Node) { addAttestedBlock(t, n, b) },
			start:   chain.Start{Block: height, Hash: "0x" + strings.Repeat("0", 64)},
			wantErr: errStartHash,
		},
		{
			// A parent hash mismatch holds the height.
			name:       "the agreed parent is not the previous verified block",
			setup:      func(t *testing.T, n *node.Node) { addAttestedBlock(t, n, b) },
			start:      start,
			parent:     &rule.Block{Number: height - 1, Hash: "0x" + strings.Repeat("1", 64)},
			wantErr:    errNotAccepted,
			wantReason: rule.ReasonParent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newVerifierNode(t)
			tc.setup(t, n)

			got, signers, err := newVerifier(t, n, tc.start).verify(t.Context(), height, tc.parent)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.ErrorContains(t, err, tc.wantReason)
				return
			}
			require.NoError(t, err)
			require.Equal(t, b.statement.Hash, got.Hash)
			require.Equal(t, listedSigners(), signers)
		})
	}
}

// A restart resumes above the newest stored VerifiedBlock, which the next height's parent must be,
// rather than at start.block, and each block it verifies is the parent of the height after it.
func TestRunResumesAboveTheStoredVerifiedBlock(t *testing.T) {
	n := newVerifierNode(t)
	b100 := newBlock(t, 100, "0x99", 1_700_000_000)
	b101 := newBlock(t, 101, b100.statement.Hash, 1_700_000_012)
	addAttestedBlock(t, n, b100)
	addAttestedBlock(t, n, b101)
	add(t, n, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(98), "blockHash": "0x98"})
	add(t, n, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(99), "blockHash": b100.statement.ParentHash})

	// start.block and height 99 hold no documents, so only resuming from the newest stored block
	// reaches 100.
	// The hook runs on the verifier's goroutine; stop waits for it before the test reads these.
	var hooked, hookedSigners [][]string
	var unverified []uint64
	v, err := New(n, tokensConfig(chain.Start{Block: 50, Hash: "0x50"}), tokens, Hooks{
		Verified: func(b rule.Block, signers []string) {
			hooked = append(hooked, []string{b.Hash})
			hookedSigners = append(hookedSigners, signers)
		},
		Unverified: func(height uint64) { unverified = append(unverified, height) },
	})
	require.NoError(t, err)
	stop := run(v)
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 4 }, 5*retryInterval, 50*time.Millisecond)
	stop()
	got := verified(t, n)
	require.Equal(t, []string{b100.statement.Hash, b101.statement.Hash}, []string{got[2].Hash, got[3].Hash})
	// The Verified hook gets each block the verifier records, with its signers.
	require.Equal(t, [][]string{{b100.statement.Hash}, {b101.statement.Hash}}, hooked)
	require.Equal(t, [][]string{listedSigners(), listedSigners()}, hookedSigners)
	// The Unverified hook gets the lowest unverified height when Run starts and after each block.
	require.Equal(t, []uint64{100, 101, 102}, unverified)
}

// New refuses a chain whose rule configuration the evidence codec refuses.
func TestNewRefusesTheRuleConfig(t *testing.T) {
	c := tokensConfig(chain.Start{})
	c.Required = 3

	_, err := New(nil, c, tokens, Hooks{})
	require.ErrorIs(t, err, rule.ErrRequired)
}

// A second write at the same height does nothing.
func TestRecordTwice(t *testing.T) {
	ctx := context.Background()
	n := newVerifierNode(t)
	v := newVerifier(t, n, chain.Start{})
	b := rule.Block{Number: 100, Hash: "0x" + strings.Repeat("a", 64)}

	require.NoError(t, v.record(ctx, b, []string{signer(0), signer(1)}))
	require.NoError(t, v.record(ctx, b, []string{signer(0), signer(1)}))
	require.Len(t, verified(t, n), 1)
}

// A failed write is retried without re-running the checks: the first write fails after
// removing the attestations, so checking the height again would hold it.
func TestRunRetriesAFailedWriteAlone(t *testing.T) {
	ctx := context.Background()
	n := newVerifierNode(t)
	b := newBlock(t, 100, "0x99", 1_700_000_000)
	addHeader(t, n, b)
	attestations := []client.DocID{attest(t, n, 0, b.statement), attest(t, n, 1, b.statement)}

	v := newVerifier(t, n, chain.Start{Block: 100, Hash: b.statement.Hash})
	write := v.write
	var calls int
	v.write = func(ctx context.Context, b rule.Block, signers []string) error {
		calls++
		if calls > 1 {
			return write(ctx, b, signers)
		}
		col, err := n.DB.GetCollectionByName(ctx, tokens.BlockAttestation.Name)
		if err != nil {
			return err
		}
		for _, id := range attestations {
			if _, err := col.DeleteDocument(ctx, id); err != nil {
				return err
			}
		}
		return errors.New("write failed")
	}
	stop := run(v)
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, n)) == 1 }, 5*retryInterval, 50*time.Millisecond)
	stop()
	require.Equal(t, 2, calls)
	docs, err := v.documents(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, docs.Attestations)
}

// A Blob written and read back through ExecRequest comes back as the stored hex, and a
// Blob that does not decode leaves its document out, in each table.
func TestDocumentsDecodeBlobs(t *testing.T) {
	const height = 7
	ctx := context.Background()
	n := newVerifierNode(t)
	// A query returns each stored value as it is. "abc" is hex of odd length, "xyz" is not hex, and
	// the empty value decodes to no bytes; none of them is held.
	for _, raw := range []string{"abcd", "abc", "xyz", ""} {
		add(t, n, tokens.Header.Name, map[string]any{"number": height, "hash": "0x" + raw, "raw": raw})
	}
	for _, blobs := range [][2]string{{"abcd", "ef01"}, {"abc", "ef01"}, {"abcd", "ef0"}} {
		add(t, n, tokens.Evidence.Name, map[string]any{"blockNumber": height, "asset": usd1, "records": blobs[0], "proof": blobs[1]})
	}
	for _, proof := range []string{"ef01", "ef0"} {
		add(t, n, tokens.StateCapture.Name, map[string]any{"blockNumber": height, "asset": usd1, "value": proof, "proof": proof})
	}
	for _, s := range []string{"0102", "010"} {
		add(t, n, tokens.BlockAttestation.Name, map[string]any{"blockNumber": height, "signer": s, "statement": s})
	}

	docs, err := newVerifier(t, n, chain.Start{}).documents(ctx, height)

	require.NoError(t, err)
	require.Equal(t, rule.Documents{
		Headers:      []rule.Header{{Hash: "0xabcd", Raw: []byte{0xab, 0xcd}}},
		Evidence:     []rule.Evidence{{Asset: usd1, Records: []byte{0xab, 0xcd}, Proof: []byte{0xef, 0x01}}},
		Captures:     []rule.Capture{{Asset: usd1, Value: "ef01", Proof: []byte{0xef, 0x01}}},
		Attestations: []rule.Attestation{{Signer: "0102", Statement: []byte{0x01, 0x02}}},
	}, docs)
}

// A height is repaired only after it has waited repairAfter while the host holds a later Header,
// and the repair covers repairHeights heights from it up.
func TestRunRepairsOnlyStuckHeights(t *testing.T) {
	cases := []struct {
		name        string
		laterBlock  bool
		repairAfter time.Duration
		noLister    bool
		wantRepair  bool
	}{
		{name: "stuck behind a later block", laterBlock: true, repairAfter: 200 * time.Millisecond, wantRepair: true},
		{name: "no later block held", repairAfter: 200 * time.Millisecond},
		{name: "not stuck for long enough", laterBlock: true, repairAfter: time.Hour},
		{name: "no documents lister", laterBlock: true, repairAfter: 200 * time.Millisecond, noLister: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newVerifierNode(t)
			b10 := newBlock(t, 10, "0x09", 1_700_000_000)
			addHeader(t, n, b10)
			attest(t, n, 0, b10.statement)
			if tc.laterBlock {
				addHeader(t, n, newBlock(t, 11, b10.statement.Hash, 1_700_000_012))
			}
			var mu sync.Mutex
			var repaired [][2]uint64
			hooks := Hooks{
				Documents: func(_ context.Context, from, to uint64) (map[string][]string, error) {
					mu.Lock()
					defer mu.Unlock()
					repaired = append(repaired, [2]uint64{from, to})
					return map[string][]string{}, nil
				},
			}
			if tc.noLister {
				hooks = Hooks{}
			}
			v, err := New(n, tokensConfig(chain.Start{Block: 10, Hash: b10.statement.Hash}), tokens, hooks)
			require.NoError(t, err)
			v.repairAfter = tc.repairAfter
			stop := run(v)
			defer stop()

			repairs := func() [][2]uint64 {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(repaired)
			}
			if !tc.wantRepair {
				require.Never(t, func() bool { return len(repairs()) > 0 }, 2*retryInterval, 50*time.Millisecond)
				return
			}
			require.Eventually(t, func() bool { return len(repairs()) > 0 }, 2*retryInterval, 50*time.Millisecond)
			require.Equal(t, [2]uint64{10, 10 + repairHeights - 1}, repairs()[0])
			// A height still stuck after a repair waits repairAfter again before the next one.
			time.Sleep(retryInterval)
			require.LessOrEqual(t, len(repairs()), int(retryInterval/tc.repairAfter)+2)
		})
	}
}

// An attestation the host never received is fetched from the generator that holds it, and the
// height verifies.
func TestRunRepairsMissingAttestation(t *testing.T) {
	ctx := context.Background()
	generator := newServedNode(t, true)
	host := newServedNode(t, true)
	addVerifiedBlockTable(t, host)
	b10 := newBlock(t, 10, "0x09", 1_700_000_000)
	b11 := newBlock(t, 11, b10.statement.Hash, 1_700_000_012)
	for _, n := range []*node.Node{generator, host} {
		addHeader(t, n, b10)
		addHeader(t, n, b11)
		attest(t, n, 0, b10.statement)
	}
	attest(t, generator, 1, b10.statement)
	connect(ctx, t, host, generator)

	v, err := New(host, tokensConfig(chain.Start{Block: 10, Hash: b10.statement.Hash}), tokens, Hooks{
		Documents: func(ctx context.Context, from, to uint64) (map[string][]string, error) {
			listed := make(map[string][]string)
			for _, col := range tokens.Generated() {
				for h := from; h <= min(to, 11); h++ {
					listed[col.Name] = append(listed[col.Name], documentIDs(ctx, t, generator, col, h)...)
				}
			}
			return listed, nil
		},
	})
	require.NoError(t, err)
	v.repairAfter = 200 * time.Millisecond
	stop := run(v)
	defer stop()
	require.Eventually(t, func() bool { return len(verified(t, host)) > 0 }, 10*retryInterval, 50*time.Millisecond)
	require.Equal(t, b10.statement.Hash, verified(t, host)[0].Hash)
}

// A repair fetches every missing document, including those past the first batch.
func TestRepairFetchesEveryBatch(t *testing.T) {
	ctx := context.Background()
	generator := newServedNode(t, true)
	host := newServedNode(t, true)
	addVerifiedBlockTable(t, host)
	addHeader(t, generator, newBlock(t, 10, "0x09", 1_700_000_000))
	for i := range 2*syncBatch + 1 {
		add(t, generator, tokens.Evidence.Name, map[string]any{"blockNumber": 10, "asset": usd1, "records": fmt.Sprintf("%04x", i), "proof": "01"})
	}
	connect(ctx, t, host, generator)

	v, err := New(host, tokensConfig(chain.Start{}), tokens, Hooks{
		Documents: func(ctx context.Context, _, _ uint64) (map[string][]string, error) {
			return map[string][]string{tokens.Evidence.Name: documentIDs(ctx, t, generator, tokens.Evidence, 10)}, nil
		},
	})
	require.NoError(t, err)
	v.repair(ctx, 10)

	want := documentIDs(ctx, t, generator, tokens.Evidence, 10)
	require.Len(t, want, 2*syncBatch+1)
	require.ElementsMatch(t, want, documentIDs(ctx, t, host, tokens.Evidence, 10))
}

// connect connects host to generator and waits until generator answers a document sync for its
// Header at height 10.
func connect(ctx context.Context, t *testing.T, host, generator *node.Node) {
	t.Helper()
	addresses, err := generator.DB.PeerInfo(ctx)
	require.NoError(t, err)
	require.NoError(t, host.DB.Connect(ctx, addresses))
	headerID := documentIDs(ctx, t, generator, tokens.Header, 10)[0]
	require.Eventually(t, func() bool {
		syncCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return host.DB.SyncDocuments(syncCtx, tokens.Header.Name, []string{headerID}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the generator never answered a document sync")
}

// documentIDs returns the IDs of the documents n holds in col at height.
func documentIDs(ctx context.Context, t *testing.T, n *node.Node, col chain.Collection, height uint64) []string {
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

// A held height is logged when it first holds and when its reason changes, at info level while the
// rule waits for documents and at error level when they refuse it. The next height's first hold is
// logged even with the same reason.
func TestRunLogsAHoldOncePerReason(t *testing.T) {
	n := newVerifierNode(t)
	core, logs := observer.New(zap.InfoLevel)
	original := logger.Sugar
	logger.Sugar = zap.New(core).Sugar()
	t.Cleanup(func() { logger.Sugar = original })
	messages := func() []string {
		var m []string
		for _, entry := range logs.All() {
			m = append(m, entry.Message)
		}
		return m
	}
	level := func(i int) zapcore.Level { return logs.All()[i].Level }

	b100 := newBlock(t, 100, "0x99", 1_700_000_000)
	stop := run(newVerifier(t, n, chain.Start{Block: 100, Hash: b100.statement.Hash}))
	defer stop()
	// No document is held, so no group agrees, on each of the checks in this window.
	require.Never(t, func() bool { return logs.Len() > 1 }, 2*retryInterval+retryInterval/2, 50*time.Millisecond)
	require.Len(t, messages(), 1)
	require.Contains(t, messages()[0], "Block 100 is held")
	require.Contains(t, messages()[0], rule.ReasonNoAgreement)
	require.Equal(t, zapcore.InfoLevel, level(0))

	// 100 verifies, and 101 holds for the same reason.
	addAttestedBlock(t, n, b100)
	require.Eventually(t, func() bool { return logs.Len() == 2 }, 3*retryInterval, 50*time.Millisecond)
	require.Contains(t, messages()[1], "Block 101 is held")
	require.Contains(t, messages()[1], rule.ReasonNoAgreement)
	require.Equal(t, zapcore.InfoLevel, level(1))

	// 101's signers agree on a block whose parent is not 100: a new reason.
	addAttestedBlock(t, n, newBlock(t, 101, "0x77", 1_700_000_012))
	require.Eventually(t, func() bool { return logs.Len() == 3 }, 3*retryInterval, 50*time.Millisecond)
	require.Contains(t, messages()[2], rule.ReasonParent)
	require.Equal(t, zapcore.ErrorLevel, level(2))
	require.Len(t, verified(t, n), 1)
}

// A height the rule did not accept waits for documents only while every listed signer's attestation
// is short of a document or in a group below k, with those groups naming one block and holding
// fewer than required signers.
func TestAwaitsDocuments(t *testing.T) {
	a, b, c := signer(0), signer(1), signer(2)
	cases := []struct {
		name    string
		verdict rule.Verdict
		want    bool
	}{
		{"no document held", rule.Verdict{Reason: rule.ReasonNoAgreement}, true},
		{"one listed signer so far", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK},
		}}, true},
		{"attestations before their header", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonHeader}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonHeader},
		}}, true},
		{"attestations before their evidence", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonEvidence}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonEvidence},
		}}, true},
		{"one signer's state capture not held yet", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonCapture},
		}}, true},
		{"a held header that does not match the statements", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBadHeader}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonBadHeader},
		}}, false},
		{"a held state capture that does not prove one signer's value", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonBadCapture},
		}}, false},
		{"an unlisted signer only", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: c, Reason: rule.ReasonUnlisted},
		}}, true},
		{"one listed signer on two blocks", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK}, {Signer: a, Hash: "0xbb", Reason: rule.ReasonBelowK},
		}}, false},
		{"listed signers on one block with different claims", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK}, {Signer: b, Hash: "0xaa", Reason: rule.ReasonBelowK},
		}}, false},
		{"a listed signer's signature fails", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: rule.ReasonBelowK}, {Signer: b, Reason: rule.ReasonSignature},
		}}, false},
		{"a reason from a newer codec", rule.Verdict{Reason: rule.ReasonNoAgreement, Results: []rule.Result{
			{Signer: a, Hash: "0xaa", Reason: "a reason from a newer codec"},
		}}, false},
		{"two groups have the required signers", rule.Verdict{Reason: rule.ReasonSplit}, false},
		{"the agreed parent is not the previous verified block", rule.Verdict{Reason: rule.ReasonParent}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, awaitsDocuments(tc.verdict, 2))
		})
	}
}
