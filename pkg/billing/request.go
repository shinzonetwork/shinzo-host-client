// Package billing signs and recovers the EIP-712 messages behind a billed
// query. A client signs a QueryRequest to prove it authorized (and pays for) a
// query; the host and accounting service recover the signer from the signature.
//
// The domain omits verifyingContract (there is no verifier contract). chainId
// binds a signature to one chain and must match across the signer and every
// verifier, so it is a parameter rather than a constant: the shinzohub EVM
// chain id is 91273003 local, 91273002 devnet. The browser client signs the
// same typed data with viem; the golden vector under testdata/golden gates that
// a viem signature recovers here.
package billing

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

const (
	domainName    = "ShinzoQueryBilling"
	domainVersion = "1"
)

// QueryRequest is the EIP-712 message a client signs to authorize one billed
// query. Timestamp is unix seconds; Nonce is 32 random bytes that make each
// request unique so a signature cannot be replayed.
type QueryRequest struct {
	QueryHash [32]byte
	Nonce     [32]byte
	Timestamp uint64
}

// NewNonce returns 32 cryptographically random bytes for QueryRequest.Nonce.
func NewNonce() ([32]byte, error) {
	var n [32]byte
	if _, err := rand.Read(n[:]); err != nil {
		return [32]byte{}, fmt.Errorf("read nonce: %w", err)
	}
	return n, nil
}

// NonceFromHex decodes a 0x-prefixed hex nonce and requires exactly 32 bytes, so
// a malformed wire value cannot be padded into a different request silently.
func NonceFromHex(s string) ([32]byte, error) {
	b, err := hexutil.Decode(s)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decode nonce: %w", err)
	}
	if len(b) != 32 {
		return [32]byte{}, fmt.Errorf("nonce must be 32 bytes, got %d", len(b))
	}
	var n [32]byte
	copy(n[:], b)
	return n, nil
}

// SignQueryRequest signs req for chainID with priv and returns a 65-byte
// signature (r, s, v) with v as 27/28, matching wallet and viem output.
func SignQueryRequest(chainID uint64, priv *ecdsa.PrivateKey, req QueryRequest) ([]byte, error) {
	digest, err := requestDigest(chainID, req)
	if err != nil {
		return nil, err
	}
	sig, err := crypto.Sign(digest, priv)
	if err != nil {
		return nil, fmt.Errorf("sign query request: %w", err)
	}
	sig[64] += 27
	return sig, nil
}

// RecoverQueryRequest returns the address that signed req for chainID. It
// accepts a recovery id of 27/28 or 0/1.
func RecoverQueryRequest(chainID uint64, req QueryRequest, sig []byte) (common.Address, error) {
	if len(sig) != 65 {
		return common.Address{}, fmt.Errorf("signature must be 65 bytes, got %d", len(sig))
	}
	digest, err := requestDigest(chainID, req)
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
		return common.Address{}, fmt.Errorf("recover query request signer: %w", err)
	}
	return crypto.PubkeyToAddress(*pub), nil
}

// requestDigest returns the EIP-712 signing digest for req under chainID.
func requestDigest(chainID uint64, req QueryRequest) ([]byte, error) {
	td := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
			},
			"QueryRequest": {
				{Name: "queryHash", Type: "bytes32"},
				{Name: "nonce", Type: "bytes32"},
				{Name: "timestamp", Type: "uint256"},
			},
		},
		PrimaryType: "QueryRequest",
		Domain: apitypes.TypedDataDomain{
			Name:    domainName,
			Version: domainVersion,
			ChainId: (*math.HexOrDecimal256)(new(big.Int).SetUint64(chainID)),
		},
		Message: apitypes.TypedDataMessage{
			"queryHash": hexutil.Encode(req.QueryHash[:]),
			"nonce":     hexutil.Encode(req.Nonce[:]),
			"timestamp": (*math.HexOrDecimal256)(new(big.Int).SetUint64(req.Timestamp)),
		},
	}
	digest, _, err := apitypes.TypedDataAndHash(td)
	if err != nil {
		return nil, fmt.Errorf("eip712 digest: %w", err)
	}
	return digest, nil
}
