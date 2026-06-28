package billing

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/shinzonetwork/shinzo-host-client/pkg/canonical"
)

// Request decodes the hex fields of e into the signed QueryRequest and the raw
// signature, so a verifier can recover the signer. It errors if any field is
// malformed (bad hex, wrong length).
func (e Extensions) Request() (QueryRequest, []byte, error) {
	queryHash, err := decodeBytes32(e.QueryHash)
	if err != nil {
		return QueryRequest{}, nil, fmt.Errorf("query_hash: %w", err)
	}
	nonce, err := NonceFromHex(e.Nonce)
	if err != nil {
		return QueryRequest{}, nil, fmt.Errorf("nonce: %w", err)
	}
	sig, err := hexutil.Decode(e.RequestSignature)
	if err != nil {
		return QueryRequest{}, nil, fmt.Errorf("request_signature: %w", err)
	}
	return QueryRequest{QueryHash: queryHash, Nonce: nonce, Timestamp: e.RequestTimestamp}, sig, nil
}

// VerifyRequest recomputes the query_hash from query and variables, confirms it
// matches the hash the client signed (so the query being served is the one that
// was authorized), recovers the payer from the request signature for chainID,
// and returns the payer address. ext is the signed envelope carried under the
// request's "extensions".
func VerifyRequest(chainID uint64, query string, variables json.RawMessage, ext Extensions) (common.Address, error) {
	req, sig, err := ext.Request()
	if err != nil {
		return common.Address{}, err
	}

	recomputed, _, err := canonical.QueryHash(query, variables)
	if err != nil {
		return common.Address{}, fmt.Errorf("recompute query hash: %w", err)
	}
	if req.QueryHash != recomputed {
		return common.Address{}, fmt.Errorf(
			"query_hash mismatch: signed %s, computed %s",
			hexutil.Encode(req.QueryHash[:]), hexutil.Encode(recomputed[:]),
		)
	}

	return RecoverQueryRequest(chainID, req, sig)
}

// decodeBytes32 decodes a 0x-prefixed hex value and requires exactly 32 bytes.
func decodeBytes32(s string) ([32]byte, error) {
	b, err := hexutil.Decode(s)
	if err != nil {
		return [32]byte{}, err
	}
	if len(b) != 32 {
		return [32]byte{}, fmt.Errorf("expected 32 bytes, got %d", len(b))
	}
	var out [32]byte
	copy(out[:], b)
	return out, nil
}
