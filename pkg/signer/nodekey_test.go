package signer

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	defracrypto "github.com/sourcenetwork/defradb/crypto"

	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-querysig/billing"
)

// testNodeKeyHex is the public anvil/hardhat account 0, used only as a fixed test
// key.
const testNodeKeyHex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

func testDefraKey(t *testing.T) defracrypto.PrivateKey {
	t.Helper()
	raw, err := hex.DecodeString(testNodeKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := defracrypto.PrivateKeyFromBytes(defracrypto.KeyTypeSecp256k1, raw)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

func TestNodeKeyToECDSAMatchesGethKey(t *testing.T) {
	got, err := nodeKeyToECDSA(testDefraKey(t))
	if err != nil {
		t.Fatal(err)
	}
	want, err := ethcrypto.HexToECDSA(testNodeKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ethcrypto.FromECDSA(got), ethcrypto.FromECDSA(want)) {
		t.Error("converted key bytes differ from the geth key for the same input")
	}
}

func TestNodeKeyToECDSANilKey(t *testing.T) {
	if _, err := nodeKeyToECDSA(nil); err == nil {
		t.Fatal("expected an error for a nil key, got nil")
	}
}

// A non-secp256k1 identity must be rejected, not silently converted into a wrong key.
func TestNodeKeyToECDSARejectsNonSecp256k1(t *testing.T) {
	r1, err := defracrypto.GenerateKey(defracrypto.KeyTypeSecp256r1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeKeyToECDSA(r1); !errors.Is(err, ErrUnexpectedKeyType) {
		t.Fatalf("want ErrUnexpectedKeyType for a secp256r1 key, got %v", err)
	}
}

// TestNodeKeySignsRecoverableResponse proves the full bridge: the node identity's
// key signs a QueryResponse that recovers to the host address derived from the
// same key.
func TestNodeKeySignsRecoverableResponse(t *testing.T) {
	key, err := nodeKeyToECDSA(testDefraKey(t))
	if err != nil {
		t.Fatal(err)
	}
	hostAddr := ethcrypto.PubkeyToAddress(key.PublicKey)

	const chainID = 91273002
	resp := billing.QueryResponse{
		QueryHash:        [32]byte{0x01},
		Host:             hostAddr,
		Pool:             common.HexToAddress("0x2222222222222222222222222222222222222222"),
		RowsQueried:      10,
		RespondedAt:      1735689600,
		ResponseCidsHash: billing.ResponseCidsHash(nil),
	}
	sig, err := billing.SignQueryResponse(chainID, key, resp)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := billing.RecoverQueryResponse(chainID, resp, sig)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != hostAddr {
		t.Errorf("recovered %s, want host %s", recovered, hostAddr)
	}
}

// TestNodeECDSAKeyFromFileStore exercises the full load: NodeECDSAKey reads the
// node identity from a file-backed store and returns the matching ecdsa key.
func TestNodeECDSAKeyFromFileStore(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, keyFileName), []byte(testNodeKeyHex), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &defradb.Config{}
	cfg.DefraDB.Store.Path = dir

	got, err := NodeECDSAKey(nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := ethcrypto.HexToECDSA(testNodeKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ethcrypto.FromECDSA(got), ethcrypto.FromECDSA(want)) {
		t.Error("key loaded from the store differs from the expected key")
	}
}
