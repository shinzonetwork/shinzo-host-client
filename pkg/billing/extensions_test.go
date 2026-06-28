package billing

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/shinzonetwork/shinzo-host-client/pkg/canonical"
)

// TestSignRequestRoundTrip checks the envelope is internally consistent: the
// signature recovers to the signer over exactly the query_hash, nonce, and
// timestamp the envelope reports, which is what the host re-derives.
func TestSignRequestRoundTrip(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.PubkeyToAddress(priv.PublicKey)
	const chainID = 91273002

	ext, err := SignRequest(chainID, priv, "query { Foo { id } }", json.RawMessage(`{"limit":10}`), 3, 1735689600)
	if err != nil {
		t.Fatal(err)
	}

	req := QueryRequest{
		QueryHash: hexTo32(t, ext.QueryHash),
		Nonce:     hexTo32(t, ext.Nonce),
		Timestamp: ext.RequestTimestamp,
	}
	sig, err := hexutil.Decode(ext.RequestSignature)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RecoverQueryRequest(chainID, req, sig)
	if err != nil {
		t.Fatal(err)
	}
	if got != signer {
		t.Errorf("signed extensions recovered %s, want %s", got, signer)
	}
}

// TestSignRequestQueryHashMatchesCanonical checks the envelope carries the same
// hash the host will recompute from the query and variables.
func TestSignRequestQueryHashMatchesCanonical(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	const query = "query { Foo { id } }"
	vars := json.RawMessage(`{"a":1,"b":2}`)

	ext, err := SignRequest(91273002, priv, query, vars, 1, 1735689600)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := canonical.QueryHash(query, vars)
	if err != nil {
		t.Fatal(err)
	}
	if ext.QueryHash != hexutil.Encode(want[:]) {
		t.Errorf("query_hash %s, want %s", ext.QueryHash, hexutil.Encode(want[:]))
	}
}

func TestSignRequestFieldsWellFormed(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	ext, err := SignRequest(91273002, priv, "q", nil, 5, 1735689600)
	if err != nil {
		t.Fatal(err)
	}

	if sig, err := hexutil.Decode(ext.RequestSignature); err != nil || len(sig) != 65 {
		t.Errorf("request_signature not 65-byte hex: err=%v len=%d", err, len(sig))
	}
	if nonce, err := hexutil.Decode(ext.Nonce); err != nil || len(nonce) != 32 {
		t.Errorf("nonce not 32-byte hex: err=%v len=%d", err, len(nonce))
	}
	if qh, err := hexutil.Decode(ext.QueryHash); err != nil || len(qh) != 32 {
		t.Errorf("query_hash not 32-byte hex: err=%v len=%d", err, len(qh))
	}
	if ext.RequestTimestamp != 1735689600 {
		t.Errorf("timestamp not carried: %d", ext.RequestTimestamp)
	}
	if ext.Fanout != 5 {
		t.Errorf("fanout not carried: %d", ext.Fanout)
	}
}

// TestSignRequestPropagatesVariablesError checks a variable the canonical layer
// rejects (>= 2^53) surfaces as an error rather than being signed.
func TestSignRequestPropagatesVariablesError(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SignRequest(91273002, priv, "q", json.RawMessage(`{"n":9007199254740992}`), 1, 1735689600); err == nil {
		t.Fatal("expected an error for an out-of-range variable, got nil")
	}
}

func TestSignRequestNonceUnique(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	a, err := SignRequest(91273002, priv, "q", nil, 1, 1735689600)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SignRequest(91273002, priv, "q", nil, 1, 1735689600)
	if err != nil {
		t.Fatal(err)
	}
	if a.Nonce == b.Nonce {
		t.Error("two signed requests reused the same nonce")
	}
	if a.RequestSignature == b.RequestSignature {
		t.Error("two signed requests produced the same signature despite a fresh nonce")
	}
}
