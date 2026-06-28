package billing

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func testResponse() QueryResponse {
	return QueryResponse{
		QueryHash:        [32]byte{0x01},
		Host:             common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Pool:             common.HexToAddress("0x2222222222222222222222222222222222222222"),
		RowsQueried:      42,
		RespondedAt:      1735689600,
		ResponseCidsHash: ResponseCidsHash(nil),
	}
}

func TestSignRecoverResponseRoundTrip(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(priv.PublicKey)
	const chainID = 91273002

	sig, err := SignQueryResponse(chainID, priv, testResponse())
	if err != nil {
		t.Fatal(err)
	}
	got, err := RecoverQueryResponse(chainID, testResponse(), sig)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("recovered %s, want %s", got, want)
	}
}

// TestRecoverResponseBindsEveryField mutates one signed field at a time and
// checks the signer no longer recovers, proving each field (and the chain id) is
// part of the digest.
func TestRecoverResponseBindsEveryField(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.PubkeyToAddress(priv.PublicKey)
	const chainID = 91273002
	base := testResponse()

	sig, err := SignQueryResponse(chainID, priv, base)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		mut  func(r *QueryResponse, chainID *uint64)
	}{
		{"queryHash", func(r *QueryResponse, _ *uint64) { r.QueryHash[0] ^= 0xff }},
		{"host", func(r *QueryResponse, _ *uint64) { r.Host[0] ^= 0xff }},
		{"pool", func(r *QueryResponse, _ *uint64) { r.Pool[0] ^= 0xff }},
		{"rowsQueried", func(r *QueryResponse, _ *uint64) { r.RowsQueried++ }},
		{"respondedAt", func(r *QueryResponse, _ *uint64) { r.RespondedAt++ }},
		{"responseCidsHash", func(r *QueryResponse, _ *uint64) { r.ResponseCidsHash[0] ^= 0xff }},
		{"chainID", func(_ *QueryResponse, c *uint64) { *c++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			chainID := uint64(chainID)
			tc.mut(&r, &chainID)
			got, err := RecoverQueryResponse(chainID, r, sig)
			if err != nil {
				t.Fatal(err)
			}
			if got == signer {
				t.Errorf("tampering %s still recovered the signer; field is not bound", tc.name)
			}
		})
	}
}

func TestRecoverResponseRejectsBadLength(t *testing.T) {
	for _, n := range []int{0, 64, 66} {
		if _, err := RecoverQueryResponse(91273002, testResponse(), make([]byte, n)); err == nil {
			t.Errorf("signature length %d: expected an error", n)
		}
	}
}

func TestResponseCidsHash(t *testing.T) {
	empty := ResponseCidsHash(nil)
	if empty == ([32]byte{}) {
		t.Error("the empty set hashed to zero; expected a fixed non-zero value")
	}
	if ResponseCidsHash([]string{}) != empty {
		t.Error("nil and an empty slice must hash the same")
	}

	reordered := ResponseCidsHash([]string{"cidB", "cidA", "cidC"})
	sorted := ResponseCidsHash([]string{"cidA", "cidB", "cidC"})
	if reordered != sorted {
		t.Error("cid order must not change the hash")
	}
	if reordered == ResponseCidsHash([]string{"cidA", "cidB"}) {
		t.Error("a different cid set must hash differently")
	}
}
