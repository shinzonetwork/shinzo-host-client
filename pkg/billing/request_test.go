package billing

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

type eip712Golden struct {
	PrivateKey string `json:"privateKey"`
	Address    string `json:"address"`
	Domain     struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		ChainID uint64 `json:"chainId"`
	} `json:"domain"`
	Request struct {
		QueryHash string `json:"queryHash"`
		Nonce     string `json:"nonce"`
		Timestamp uint64 `json:"timestamp"`
	} `json:"request"`
	Signature string `json:"signature"`
}

func loadEIP712Golden(t *testing.T) (eip712Golden, QueryRequest, []byte) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden", "eip712.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g eip712Golden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	req := QueryRequest{
		QueryHash: hexTo32(t, g.Request.QueryHash),
		Nonce:     hexTo32(t, g.Request.Nonce),
		Timestamp: g.Request.Timestamp,
	}
	sig, err := hexutil.Decode(g.Signature)
	if err != nil {
		t.Fatalf("decode golden signature: %v", err)
	}
	return g, req, sig
}

func hexTo32(t *testing.T, s string) [32]byte {
	t.Helper()
	b, err := hexutil.Decode(s)
	if err != nil || len(b) != 32 {
		t.Fatalf("bad 32-byte hex %q: %v", s, err)
	}
	var out [32]byte
	copy(out[:], b)
	return out
}

// TestRecoverViemGolden is the cross-language gate: a signature produced by viem
// (testdata/golden/sign.mjs) must recover to the signer's address through the Go
// verifier, proving the browser client and the Go host agree on the typed data.
func TestRecoverViemGolden(t *testing.T) {
	g, req, sig := loadEIP712Golden(t)
	addr, err := RecoverQueryRequest(g.Domain.ChainID, req, sig)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !strings.EqualFold(addr.Hex(), g.Address) {
		t.Errorf("recovered %s, want %s", addr.Hex(), g.Address)
	}
}

// TestSignMatchesViem checks Go produces the identical signature bytes viem did
// for the same key and request: both use RFC 6979 deterministic ECDSA, so a
// divergence means the digests differ (a domain or encoding mismatch).
func TestSignMatchesViem(t *testing.T) {
	g, req, want := loadEIP712Golden(t)
	priv, err := crypto.HexToECDSA(strings.TrimPrefix(g.PrivateKey, "0x"))
	if err != nil {
		t.Fatalf("load key: %v", err)
	}
	got, err := SignQueryRequest(g.Domain.ChainID, priv, req)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Go signature differs from viem\n got:  %s\nwant: %s", hexutil.Encode(got), hexutil.Encode(want))
	}
}

func TestSignRecoverRoundTrip(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(priv.PublicKey)
	nonce, err := NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	req := QueryRequest{QueryHash: [32]byte{0x01}, Nonce: nonce, Timestamp: 1735689600}

	sig, err := SignQueryRequest(91273002, priv, req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RecoverQueryRequest(91273002, req, sig)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("round trip recovered %s, want %s", got, want)
	}
}

// TestRecoverBindsEveryField mutates one signed input at a time and checks the
// signer no longer recovers, proving each field (and the chain id) is part of
// the digest and cannot be swapped after signing.
func TestRecoverBindsEveryField(t *testing.T) {
	g, req, sig := loadEIP712Golden(t)
	signer := common.HexToAddress(g.Address)

	cases := []struct {
		name string
		mut  func(req *QueryRequest, chainID *uint64)
	}{
		{"queryHash", func(r *QueryRequest, _ *uint64) { r.QueryHash[0] ^= 0xff }},
		{"nonce", func(r *QueryRequest, _ *uint64) { r.Nonce[0] ^= 0xff }},
		{"timestamp", func(r *QueryRequest, _ *uint64) { r.Timestamp++ }},
		{"chainID", func(_ *QueryRequest, c *uint64) { *c++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := req
			chainID := g.Domain.ChainID
			tc.mut(&r, &chainID)
			got, err := RecoverQueryRequest(chainID, r, sig)
			if err != nil {
				t.Fatal(err)
			}
			if got == signer {
				t.Errorf("tampering %s still recovered the signer; field is not bound", tc.name)
			}
		})
	}
}

// TestRecoverAcceptsBothVConventions checks the recovery id normalization: a
// signature with v as 27/28 (wallet style) and the same signature with v as 0/1
// recover to the same address.
func TestRecoverAcceptsBothVConventions(t *testing.T) {
	g, req, sig := loadEIP712Golden(t)
	if sig[64] < 27 {
		t.Fatalf("golden signature has v=%d, expected 27/28", sig[64])
	}
	low := make([]byte, 65)
	copy(low, sig)
	low[64] -= 27

	high, err := RecoverQueryRequest(g.Domain.ChainID, req, sig)
	if err != nil {
		t.Fatal(err)
	}
	lowAddr, err := RecoverQueryRequest(g.Domain.ChainID, req, low)
	if err != nil {
		t.Fatal(err)
	}
	if high != lowAddr {
		t.Errorf("v=%d and v=%d recovered different addresses", sig[64], low[64])
	}
}

func TestRecoverRejectsBadLength(t *testing.T) {
	g, req, _ := loadEIP712Golden(t)
	for _, n := range []int{0, 64, 66} {
		if _, err := RecoverQueryRequest(g.Domain.ChainID, req, make([]byte, n)); err == nil {
			t.Errorf("signature length %d: expected an error", n)
		}
	}
}

func TestNonce(t *testing.T) {
	a, err := NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("two NewNonce calls returned the same value")
	}

	got, err := NonceFromHex(hexutil.Encode(a[:]))
	if err != nil {
		t.Fatal(err)
	}
	if got != a {
		t.Error("nonce hex round trip changed the value")
	}

	for _, bad := range []string{"0x11", "0x" + strings.Repeat("11", 33), "0xnothex", ""} {
		if _, err := NonceFromHex(bad); err == nil {
			t.Errorf("NonceFromHex(%q): expected an error", bad)
		}
	}
}
