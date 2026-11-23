package host

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/shinzonetwork/shinzo-evidence-codec/evm"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

// usd1StateProof is the codec's eth_getProof fixture for USD1 on Ethereum at block 26307825: its
// supply slot and the owner's balance slot, and what eth_call returned for each.
type usd1StateProof struct {
	BlockNumber      uint64           `json:"blockNumber"`
	StateRoot        common.Hash      `json:"stateRoot"`
	Contract         string           `json:"contract"`
	SupplySlot       string           `json:"supplySlot"`
	OwnerBalanceSlot string           `json:"ownerBalanceSlot"`
	TotalSupply      hexutil.Bytes    `json:"ethCallTotalSupply"`
	OwnerBalance     hexutil.Bytes    `json:"ethCallOwnerBalance"`
	Proof            evm.AccountProof `json:"proof"`
}

func TestStateHandler(t *testing.T) {
	const prefix = "Bsc__Mainnet"
	ctx := context.Background()
	b, err := os.ReadFile(filepath.Join("testdata", "ethereum-usd1-proof.json"))
	require.NoError(t, err)
	var fixture usd1StateProof
	require.NoError(t, json.Unmarshal(b, &fixture))
	proof, err := evm.EncodeStateProof(fixture.Proof)
	require.NoError(t, err)
	// The zero address at Ethereum block 26377355, where eth_getBalance answered 0x2ff7101b77259c1d7e1.
	b, err = os.ReadFile(filepath.Join("testdata", "ethereum-empty-storage-proof.json"))
	require.NoError(t, err)
	var account struct {
		StateRoot common.Hash      `json:"stateRoot"`
		Account   string           `json:"account"`
		Proof     evm.AccountProof `json:"proof"`
	}
	require.NoError(t, json.Unmarshal(b, &account))
	accountProof, err := evm.EncodeStateProof(account.Proof)
	require.NoError(t, err)
	// An address with no balance, nonce or code at Ethereum block 26479097, so not in the state.
	b, err = os.ReadFile(filepath.Join("testdata", "ethereum-absent-account-proof.json"))
	require.NoError(t, err)
	var absent struct {
		StateRoot common.Hash      `json:"stateRoot"`
		Account   string           `json:"account"`
		Slot      string           `json:"slot"`
		Proof     evm.AccountProof `json:"proof"`
	}
	require.NoError(t, json.Unmarshal(b, &absent))
	absentProof, err := evm.EncodeStateProof(absent.Proof)
	require.NoError(t, err)

	schemaServer := httptest.NewServer(tokensSchemaHandler(t, prefix))
	t.Cleanup(schemaServer.Close)
	tokensChain := chain.Config{Prefix: prefix, Mode: chain.ModeTokens, CAIP2: "eip155:1", Generators: []chain.Generator{{URL: schemaServer.URL}}}
	applier := &chainsApplier{schemaCfg: config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}, chains: []chain.Config{tokensChain}}
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, applier)
	require.NoError(t, err)
	t.Cleanup(func() { _ = defraNode.Close(ctx) })
	tokens := tokensChain.Collections()

	// The fixture's block holds the proof's state root. A decoy Header at the same height and the
	// block before it hold other roots, so reading either fails every proof.
	header := func(number uint64, root common.Hash, time uint64) (raw []byte, hash string) {
		raw, err := rlp.EncodeToBytes(&types.Header{
			Root: root, UncleHash: types.EmptyUncleHash, TxHash: types.EmptyTxsHash, ReceiptHash: types.EmptyReceiptsHash,
			Difficulty: big.NewInt(0), Number: new(big.Int).SetUint64(number), Time: time,
		})
		require.NoError(t, err)
		return raw, crypto.Keccak256Hash(raw).Hex()
	}
	height := fixture.BlockNumber
	raw, hash := header(height, fixture.StateRoot, 1_700_000_000)
	earlierRaw, earlierHash := header(height-1, common.Hash{}, 1_699_999_988)
	decoyRaw, _ := header(height, common.Hash{}, 1_700_000_001)
	// The decoy is stored first, so it is the first Header the store returns at the height.
	headers := []struct {
		number uint64
		raw    []byte
		hash   string
	}{{height, decoyRaw, hash}, {height, raw, hash}, {height - 1, earlierRaw, earlierHash}}
	for _, h := range headers {
		addTokensDoc(ctx, t, defraNode, tokens.Header.Name, map[string]any{"number": int64(h.number), "hash": h.hash, "raw": hex.EncodeToString(h.raw)})
	}
	held, err := defradb.QueryArray[map[string]any](ctx, defraNode, fmt.Sprintf(`%s(filter: {%s: {_eq: %d}}) { raw }`, tokens.Header.Name, tokens.Header.HeightField, height))
	require.NoError(t, err)
	require.NotEqual(t, hex.EncodeToString(raw), held[0]["raw"], "the real Header comes first; the hash check would go untested")
	addTokensDoc(ctx, t, defraNode, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(height - 1), "blockHash": earlierHash})
	addTokensDoc(ctx, t, defraNode, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(height), "blockHash": hash})
	// The zero address's state root is stored two blocks below the USD1 block, so that block stays
	// the newest verified one. A proof is checked against the root, not the height.
	accountHeight := height - 2
	accountRaw, accountHash := header(accountHeight, account.StateRoot, 1_699_999_976)
	addTokensDoc(ctx, t, defraNode, tokens.Header.Name, map[string]any{"number": int64(accountHeight), "hash": accountHash, "raw": hex.EncodeToString(accountRaw)})
	addTokensDoc(ctx, t, defraNode, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(accountHeight), "blockHash": accountHash})
	absentHeight := height - 3
	absentRaw, absentHash := header(absentHeight, absent.StateRoot, 1_699_999_964)
	addTokensDoc(ctx, t, defraNode, tokens.Header.Name, map[string]any{"number": int64(absentHeight), "hash": absentHash, "raw": hex.EncodeToString(absentRaw)})
	addTokensDoc(ctx, t, defraNode, tokens.VerifiedBlock.Name, map[string]any{"blockNumber": int64(absentHeight), "blockHash": absentHash})

	// The generators refuse an address that is not written as 0x and 40 hex digits, as a generator's
	// state endpoint does.
	addressPattern := regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	generator := func(answer any, status int) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != stateEndpoint {
				http.NotFound(w, r)
				return
			}
			// Decoded with the generator's field names, not the host's request type, so a renamed
			// field on either side fails here.
			var req struct {
				Address string `json:"address"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !addressPattern.MatchString(req.Address) {
				http.Error(w, "address must be 0x and 40 hex digits", http.StatusBadRequest)
				return
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(answer)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	honest := generator(map[string]any{"proof": hexutil.Bytes(proof)}, http.StatusOK)
	accountGenerator := generator(map[string]any{"proof": hexutil.Bytes(accountProof)}, http.StatusOK)
	absentGenerator := generator(map[string]any{"proof": hexutil.Bytes(absentProof)}, http.StatusOK)
	// A proof of nothing, padded past maxStateAnswerBytes: read whole, it would fail to verify.
	oversized := generator(map[string]any{"proof": "0xc0" + strings.Repeat("00", maxStateAnswerBytes/2)}, http.StatusOK)
	// An empty RLP list: a proof of nothing, which verifies no slot.
	lying := generator(map[string]any{"proof": "0xc0"}, http.StatusOK)
	down := generator(map[string]any{}, http.StatusServiceUnavailable)

	slots := []string{fixture.SupplySlot, fixture.OwnerBalanceSlot}
	wantValues := map[string]string{
		fixture.SupplySlot:       new(big.Int).SetBytes(fixture.TotalSupply).String(),
		fixture.OwnerBalanceSlot: new(big.Int).SetBytes(fixture.OwnerBalance).String(),
	}
	at := func(h uint64) *uint64 { return &h }
	// The USD1 contract holds no ether at its block.
	wantSlots := stateAnswer{Block: height, BlockHash: hash, Proof: proof, Balance: "0", Values: wantValues}
	cases := []struct {
		name         string
		chainID      string
		block        *uint64
		generators   []string
		disabled     bool
		address      string   // the fixture's contract when empty
		slots        []string // the fixture's slots when nil
		padding      int      // spaces before the request's JSON
		wantStatus   int
		wantFailures map[string]int64
		want         *stateAnswer // wantSlots when nil
	}{
		{name: "the newest verified block, after a generator whose proof fails", chainID: "eip155:1", generators: []string{lying, honest}, wantStatus: http.StatusOK, wantFailures: map[string]int64{lying: 1}},
		{name: "a named verified block", chainID: "eip155:1", block: at(height), generators: []string{down, honest}, wantStatus: http.StatusOK},
		{name: "a height with no VerifiedBlock", chainID: "eip155:1", block: at(height + 1), generators: []string{honest}, wantStatus: http.StatusNotFound},
		// The test host serves its tokens-mode chain under this ID too; the codec has no family for it.
		{name: "a chain outside eip155", chainID: "tron:728126428", generators: []string{honest}, wantStatus: http.StatusBadRequest},
		{name: "a tokens-mode chain this host does not serve", chainID: "eip155:1", generators: []string{honest}, disabled: true, wantStatus: http.StatusBadRequest},
		{name: "no generator's proof verifies", chainID: "eip155:1", generators: []string{lying, down}, wantStatus: http.StatusBadGateway, wantFailures: map[string]int64{lying: 1}},
		{name: "an address that is not 20 bytes", chainID: "eip155:1", generators: []string{honest}, address: "0x8d0d", wantStatus: http.StatusBadRequest},
		{name: "an address written without 0x", chainID: "eip155:1", generators: []string{honest}, address: strings.TrimPrefix(fixture.Contract, "0x"), wantStatus: http.StatusOK},
		{name: "more slots than a generator proves", chainID: "eip155:1", generators: []string{honest}, slots: slices.Repeat([]string{fixture.SupplySlot}, maxStateSlots+1), wantStatus: http.StatusBadRequest},
		{name: "a slot that is not a storage slot", chainID: "eip155:1", generators: []string{honest}, slots: []string{"35"}, wantStatus: http.StatusBadRequest},
		{name: "a body over the cap", chainID: "eip155:1", generators: []string{honest}, padding: maxStateRequestBytes, wantStatus: http.StatusBadRequest},
		{name: "an answer over the cap, then a generator whose proof verifies", chainID: "eip155:1", generators: []string{oversized, honest}, wantStatus: http.StatusOK},
		{
			name:       "slots of an account not in the state",
			chainID:    "eip155:1",
			block:      at(absentHeight),
			generators: []string{absentGenerator},
			address:    absent.Account,
			slots:      []string{absent.Slot},
			wantStatus: http.StatusOK,
			want: &stateAnswer{
				Block: absentHeight, BlockHash: absentHash, Proof: absentProof, Balance: "0", Values: map[string]string{absent.Slot: "0"},
			},
		},
		// With no slots, only the balance check rejects a proof of nothing.
		{
			name:         "an account's balance alone, after a generator whose proof fails",
			chainID:      "eip155:1",
			block:        at(accountHeight),
			generators:   []string{lying, accountGenerator},
			address:      account.Account,
			slots:        []string{},
			wantStatus:   http.StatusOK,
			wantFailures: map[string]int64{lying: 1},
			want: &stateAnswer{
				Block: accountHeight, BlockHash: accountHash, Proof: accountProof,
				Balance: hexutil.MustDecodeBig("0x2ff7101b77259c1d7e1").String(), Values: map[string]string{},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			served := tokensChain
			served.Generators = nil
			for _, url := range tc.generators {
				served.Generators = append(served.Generators, chain.Generator{URL: url})
			}
			served.CAIP2 = tc.chainID
			cfg := *DefaultConfig
			cfg.Chains = []chain.Config{served}
			m := &server.ChainMetrics{Prefix: prefix, Tokens: true}
			enabled := []chain.Collections{tokens}
			if tc.disabled {
				enabled = nil
			}
			h := &Host{DefraNode: defraNode, config: &cfg, chains: enabled, metrics: server.NewHostMetrics(m)}
			address, requested := fixture.Contract, slots
			if tc.address != "" {
				address = tc.address
			}
			if tc.slots != nil {
				requested = tc.slots
			}
			body, err := json.Marshal(stateRequest{Chain: tc.chainID, Block: tc.block, Address: address, Slots: requested})
			require.NoError(t, err)
			body = append(bytes.Repeat([]byte(" "), tc.padding), body...)

			w := httptest.NewRecorder()
			h.StateHandler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, stateEndpoint, bytes.NewReader(body)))

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			require.Equal(t, tc.wantFailures, m.Counters().StateProofFailures)
			if tc.wantStatus != http.StatusOK {
				return
			}
			var answer stateAnswer
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &answer))
			want := wantSlots
			if tc.want != nil {
				want = *tc.want
			}
			require.Equal(t, want, answer)
		})
	}

	// At most maxConcurrentStateReads reads are served at once; one more is refused with 503
	// without asking a generator.
	t.Run("one read more than are served at once", func(t *testing.T) {
		release := make(chan struct{})
		var asked atomic.Int32
		blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			asked.Add(1)
			<-release
			_ = json.NewEncoder(w).Encode(map[string]any{"proof": hexutil.Bytes(proof)})
		}))
		t.Cleanup(blocking.Close)
		served := tokensChain
		served.Generators = []chain.Generator{{URL: blocking.URL}}
		cfg := *DefaultConfig
		cfg.Chains = []chain.Config{served}
		h := &Host{DefraNode: defraNode, config: &cfg, chains: []chain.Collections{tokens}, metrics: server.NewHostMetrics(&server.ChainMetrics{Prefix: prefix, Tokens: true})}
		handler := h.StateHandler()
		body, err := json.Marshal(stateRequest{Chain: served.CAIP2, Address: fixture.Contract, Slots: slots})
		require.NoError(t, err)
		read := func() int {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, stateEndpoint, bytes.NewReader(body)))
			return w.Code
		}

		codes := make(chan int, maxConcurrentStateReads)
		for range maxConcurrentStateReads {
			go func() { codes <- read() }()
		}
		require.Eventually(t, func() bool { return asked.Load() == maxConcurrentStateReads }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, http.StatusServiceUnavailable, read())
		require.Equal(t, int32(maxConcurrentStateReads), asked.Load())
		close(release)
		for range maxConcurrentStateReads {
			require.Equal(t, http.StatusOK, <-codes)
		}
	})
}
