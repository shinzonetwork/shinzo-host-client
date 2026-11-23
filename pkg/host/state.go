package host

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/shinzonetwork/shinzo-evidence-codec/evm"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

const (
	// stateEndpoint is the route that reads storage slots at a verified block, on the host's state
	// listener and on each generator.
	stateEndpoint = "/api/v1/state"

	// maxStateRequestBytes caps a state read's body. The largest valid read, maxStateSlots slots of
	// 64 hex digits, is 70,771 bytes as compact JSON.
	maxStateRequestBytes = 128 << 10

	// stateTimeout bounds one state read, which asks the chain's generators one after another.
	stateTimeout = 60 * time.Second

	// maxConcurrentStateReads is how many state reads are served at once. Each one asks a
	// generator to call its RPC endpoint, so more are refused rather than queued.
	maxConcurrentStateReads = 4

	// maxStateAnswerBytes caps a generator's state answer. A 1,000-slot answer is 7.04 MB on BSC and
	// 6.43 MB on Ethereum.
	maxStateAnswerBytes = 16 << 20
)

var (
	errStateChain           = errors.New("chain is not a tokens-mode eip155 chain this host serves")
	errStateAddress         = errors.New("address is not a 20-byte hex address")
	errStateSlots           = errors.New("slots must list storage slots")
	errStateNoVerifiedBlock = errors.New("no verified block at that height")
	errStateNoHeader        = errors.New("the verified block's header is not held")
	errStateRefused         = errors.New("the generator refused the state request")
	errStateTooLarge        = errors.New("the generator's state answer is too large")
)

// stateRequest asks for an account and any of its storage slots at a verified block of a
// tokens-mode chain. Without Block, the newest verified block is read.
type stateRequest struct {
	Chain   string   `json:"chain"`
	Block   *uint64  `json:"block"`
	Address string   `json:"address"`
	Slots   []string `json:"slots"`
}

// stateAnswer is a generator's proof, which verified against the block's state root, and the
// values it proves in decimal: the account's native balance and each requested slot, keyed as
// requested.
type stateAnswer struct {
	Block     uint64            `json:"block"`
	BlockHash string            `json:"blockHash"`
	Proof     hexutil.Bytes     `json:"proof"`
	Balance   string            `json:"balance"`
	Values    map[string]string `json:"values"`
}

// generatorStateRequest and generatorStateAnswer are a generator's state route's request and answer.
type generatorStateRequest struct {
	BlockHash string   `json:"blockHash"`
	Address   string   `json:"address"`
	Slots     []string `json:"slots"`
}

type generatorStateAnswer struct {
	Proof hexutil.Bytes `json:"proof"`
}

// StateHandler serves POST /api/v1/state for the host's enabled tokens-mode eip155 chains: it asks
// the chain's generators in config order for a proof of the slots at a verified block and answers
// with the first that verifies against that block's state root. Nothing is stored. A read arriving
// while maxConcurrentStateReads are being served is refused with 503, a body over
// maxStateRequestBytes is refused, and each read is given stateTimeout.
func (h *Host) StateHandler() http.Handler {
	client := schema.NewSchemaHTTPClient(h.config.Schema)
	serving := make(chan struct{}, maxConcurrentStateReads)
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+stateEndpoint, func(w http.ResponseWriter, r *http.Request) {
		select {
		case serving <- struct{}{}:
			defer func() { <-serving }()
		default:
			http.Error(w, "too many state reads at once, retry later", http.StatusServiceUnavailable)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxStateRequestBytes)
		ctx, cancel := context.WithTimeout(r.Context(), stateTimeout)
		defer cancel()
		h.serveState(w, r.WithContext(ctx), client)
	})
	return mux
}

func (h *Host) serveState(w http.ResponseWriter, r *http.Request, client *http.Client) {
	var req stateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "the request is not a state request", http.StatusBadRequest)
		return
	}
	c, address, slots, err := h.checkStateRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	collections := c.Collections()
	height, blockHash, err := h.verifiedBlock(r.Context(), collections.VerifiedBlock, req.Block)
	if err != nil {
		writeStateError(w, err)
		return
	}
	root, err := h.stateRoot(r.Context(), collections.Header, height, blockHash)
	if err != nil {
		writeStateError(w, err)
		return
	}

	m := h.chainMetrics(c.Prefix)
	for _, g := range c.Generators {
		proof, err := askGeneratorState(r.Context(), client, g.URL, generatorStateRequest{BlockHash: blockHash, Address: address.Hex(), Slots: req.Slots})
		if err != nil {
			logger.Sugar.Warnf("Generator %s did not answer a state read at block %d: %v (chain: %s)", g.URL, height, err, c.Prefix)
			continue
		}
		values, err := provenValues(root, address, req.Slots, slots, proof)
		var balance *big.Int
		if err == nil {
			balance, err = evm.VerifyAccountBalance(root, address, proof)
		}
		if err != nil {
			logger.Sugar.Warnf("Generator %s answered a state read at block %d with a proof that does not verify: %v (chain: %s)", g.URL, height, err, c.Prefix)
			if m != nil {
				m.RecordStateProofFailure(g.URL)
			}
			continue
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stateAnswer{Block: height, BlockHash: blockHash, Proof: proof, Balance: balance.String(), Values: values})
		return
	}
	http.Error(w, "no generator answered with a proof that verifies", http.StatusBadGateway)
}

// maxStateSlots is the most slots a generator's state endpoint proves in one request, the most
// storage keys geth's eth_getProof accepts. A larger read is refused here instead of by every generator.
const maxStateSlots = 1024

// checkStateRequest returns the chain, address and slots req names, or why it cannot be served.
func (h *Host) checkStateRequest(req stateRequest) (chain.Config, common.Address, []common.Hash, error) {
	c, ok := h.stateChain(req.Chain)
	if !ok {
		return chain.Config{}, common.Address{}, nil, fmt.Errorf("%w: %q", errStateChain, req.Chain)
	}
	if !common.IsHexAddress(req.Address) {
		return chain.Config{}, common.Address{}, nil, fmt.Errorf("%w: %q", errStateAddress, req.Address)
	}
	if len(req.Slots) > maxStateSlots {
		return chain.Config{}, common.Address{}, nil, fmt.Errorf("%w: up to %d, not %d", errStateSlots, maxStateSlots, len(req.Slots))
	}
	slots := make([]common.Hash, len(req.Slots))
	for i, s := range req.Slots {
		key, err := evm.ParseStorageKey(s)
		if err != nil {
			return chain.Config{}, common.Address{}, nil, fmt.Errorf("%w: %w", errStateSlots, err)
		}
		slots[i] = common.Hash(key)
	}
	return c, common.HexToAddress(req.Address), slots, nil
}

// stateChain returns the enabled tokens-mode chain whose CAIP-2 ID is caip2 and whose namespace is
// eip155, the only family whose state the codec proves.
func (h *Host) stateChain(caip2 string) (chain.Config, bool) {
	if !strings.HasPrefix(caip2, "eip155:") {
		return chain.Config{}, false
	}
	for _, c := range h.config.Chains {
		if c.Mode != chain.ModeTokens || c.CAIP2 != caip2 {
			continue
		}
		enabled := slices.ContainsFunc(h.chains, func(e chain.Collections) bool { return e.Prefix == c.Prefix })
		return c, enabled
	}
	return chain.Config{}, false
}

// verifiedBlock returns the height and hash of the VerifiedBlock at height, or of the newest one when
// height is nil. It returns errStateNoVerifiedBlock when there is none.
func (h *Host) verifiedBlock(ctx context.Context, vb chain.Collection, height *uint64) (uint64, string, error) {
	query := fmt.Sprintf(`%s(order: {%s: DESC}, limit: 1) { %s blockHash }`, vb.Name, vb.HeightField, vb.HeightField)
	if height != nil {
		query = fmt.Sprintf(`%s(filter: {%s: {_eq: %d}}) { %s blockHash }`, vb.Name, vb.HeightField, *height, vb.HeightField)
	}
	rows, err := defradb.QueryArray[map[string]any](ctx, h.DefraNode, query)
	if err != nil {
		return 0, "", err
	}
	if len(rows) == 0 {
		return 0, "", errStateNoVerifiedBlock
	}
	number, ok := rows[0][vb.HeightField].(float64)
	hash, hashOK := rows[0]["blockHash"].(string)
	if !ok || !hashOK || number < 0 {
		return 0, "", errStateNoVerifiedBlock
	}
	return uint64(number), hash, nil
}

// stateRoot returns the state root of the Header at height whose raw header hashes to blockHash.
// Other Header documents at the height are ignored.
func (h *Host) stateRoot(ctx context.Context, header chain.Collection, height uint64, blockHash string) (common.Hash, error) {
	rows, err := defradb.QueryArray[map[string]any](ctx, h.DefraNode,
		fmt.Sprintf(`%s(filter: {%s: {_eq: %d}}) { raw }`, header.Name, header.HeightField, height))
	if err != nil {
		return common.Hash{}, err
	}
	for _, row := range rows {
		s, _ := row["raw"].(string)
		raw, err := hex.DecodeString(s)
		if err != nil {
			continue
		}
		if decoded, err := evm.HeaderFromRaw(raw); err == nil && decoded.Hash.Hex() == blockHash {
			return decoded.StateRoot, nil
		}
	}
	return common.Hash{}, errStateNoHeader
}

// askGeneratorState asks the generator whose HTTP API is at base for a proof of req's slots. An
// answer over maxStateAnswerBytes returns errStateTooLarge.
func askGeneratorState(ctx context.Context, client *http.Client, base string, req generatorStateRequest) ([]byte, error) {
	endpoint, err := url.JoinPath(base, stateEndpoint)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s", errStateRefused, resp.Status)
	}
	answerBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxStateAnswerBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read the state answer: %w", err)
	}
	if len(answerBytes) > maxStateAnswerBytes {
		return nil, fmt.Errorf("%w: over %d bytes", errStateTooLarge, maxStateAnswerBytes)
	}
	var answer generatorStateAnswer
	if err := json.Unmarshal(answerBytes, &answer); err != nil {
		return nil, fmt.Errorf("read the state answer: %w", err)
	}
	return answer.Proof, nil
}

// provenValues checks proof against root for each of slots, which names lists as requested, and
// returns each slot's value in decimal by its requested name. A proof that the account is not in the
// state proves each of its slots zero.
func provenValues(root common.Hash, address common.Address, names []string, slots []common.Hash, proof []byte) (map[string]string, error) {
	values := make(map[string]string, len(slots))
	for i, slot := range slots {
		value, err := evm.VerifyStateCapture(root, address, slot, proof)
		if errors.Is(err, evm.ErrAccountNotInState) {
			value, err = new(big.Int), nil
		}
		if err != nil {
			return nil, fmt.Errorf("slot %s: %w", names[i], err)
		}
		values[names[i]] = value.String()
	}
	return values, nil
}

// writeStateError answers a state read that cannot be served from the host's own documents.
func writeStateError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errStateNoVerifiedBlock), errors.Is(err, errStateNoHeader):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		logger.Sugar.Warnf("State read failed: %v", err)
		http.Error(w, "cannot read the verified block", http.StatusInternalServerError)
	}
}
