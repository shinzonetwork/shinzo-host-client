package billing

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// QueryResponse is the EIP-712 message a host signs to attest it served a query.
// The accounting service recovers the host from the signature. RespondedAt is in
// unix seconds; ResponseCidsHash commits to the source documents the host read.
type QueryResponse struct {
	QueryHash        [32]byte
	Host             common.Address
	Pool             common.Address
	RowsQueried      uint64
	RespondedAt      uint64
	ResponseCidsHash [32]byte
}

// SignQueryResponse signs resp for chainID with priv and returns a 65-byte
// signature (r, s, v) with v as 27/28.
func SignQueryResponse(chainID uint64, priv *ecdsa.PrivateKey, resp QueryResponse) ([]byte, error) {
	digest, err := responseDigest(chainID, resp)
	if err != nil {
		return nil, err
	}
	sig, err := crypto.Sign(digest, priv)
	if err != nil {
		return nil, fmt.Errorf("sign query response: %w", err)
	}
	sig[64] += 27
	return sig, nil
}

// RecoverQueryResponse returns the host address that signed resp for chainID. It
// accepts a recovery id of 27/28 or 0/1.
func RecoverQueryResponse(chainID uint64, resp QueryResponse, sig []byte) (common.Address, error) {
	if len(sig) != 65 {
		return common.Address{}, fmt.Errorf("signature must be 65 bytes, got %d", len(sig))
	}
	digest, err := responseDigest(chainID, resp)
	if err != nil {
		return common.Address{}, err
	}
	normalized := make([]byte, 65)
	copy(normalized, sig)
	if normalized[64] >= 27 {
		normalized[64] -= 27
	}
	pub, err := crypto.SigToPub(digest, normalized)
	if err != nil {
		return common.Address{}, fmt.Errorf("recover query response signer: %w", err)
	}
	return crypto.PubkeyToAddress(*pub), nil
}

// ResponseCidsHash returns keccak256 over the response's source CIDs, sorted and
// joined by newline. CIDs carry no newline, so the encoding is unambiguous; the
// host and accounting service must use this same encoding. The empty set hashes
// to a fixed value, so a v1 record with no CIDs is still well-defined.
func ResponseCidsHash(cids []string) [32]byte {
	sorted := append([]string(nil), cids...)
	sort.Strings(sorted)
	var out [32]byte
	copy(out[:], crypto.Keccak256([]byte(strings.Join(sorted, "\n"))))
	return out
}

// responseDigest returns the EIP-712 signing digest for resp under chainID.
func responseDigest(chainID uint64, resp QueryResponse) ([]byte, error) {
	td := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
			},
			"QueryResponse": {
				{Name: "queryHash", Type: "bytes32"},
				{Name: "host", Type: "address"},
				{Name: "pool", Type: "address"},
				{Name: "rowsQueried", Type: "uint256"},
				{Name: "respondedAt", Type: "uint256"},
				{Name: "responseCidsHash", Type: "bytes32"},
			},
		},
		PrimaryType: "QueryResponse",
		Domain: apitypes.TypedDataDomain{
			Name:    domainName,
			Version: domainVersion,
			ChainId: (*math.HexOrDecimal256)(new(big.Int).SetUint64(chainID)),
		},
		Message: apitypes.TypedDataMessage{
			"queryHash":        hexutil.Encode(resp.QueryHash[:]),
			"host":             resp.Host.Hex(),
			"pool":             resp.Pool.Hex(),
			"rowsQueried":      (*math.HexOrDecimal256)(new(big.Int).SetUint64(resp.RowsQueried)),
			"respondedAt":      (*math.HexOrDecimal256)(new(big.Int).SetUint64(resp.RespondedAt)),
			"responseCidsHash": hexutil.Encode(resp.ResponseCidsHash[:]),
		},
	}
	digest, _, err := apitypes.TypedDataAndHash(td)
	if err != nil {
		return nil, fmt.Errorf("eip712 digest: %w", err)
	}
	return digest, nil
}
