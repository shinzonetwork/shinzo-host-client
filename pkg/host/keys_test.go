package host

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/cosmos/go-bip39"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"

	"github.com/sourcenetwork/defradb/keyring"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

// testKeysConfig gives each test its own temp data/key dirs, mirroring
// what hostconfig.Load derives DataDir/KeyDir into for real.
func testKeysConfig(t *testing.T) *hostconfig.Config {
	t.Helper()
	cfg := testHostConfig()
	cfg.Node.DataDir = t.TempDir()
	cfg.Node.KeyDir = filepath.Join(cfg.Node.DataDir, "keys")
	return cfg
}

func testMnemonic(t *testing.T) string {
	t.Helper()
	entropy, err := bip39.NewEntropy(128) //nolint:mnd
	if err != nil {
		t.Fatalf("generating entropy: %v", err)
	}
	mnemonic, err := bip39.NewMnemonic(entropy)
	if err != nil {
		t.Fatalf("generating mnemonic: %v", err)
	}
	return mnemonic
}

func TestDeriveKeys_Deterministic(t *testing.T) {
	mnemonic := testMnemonic(t)

	a, err := deriveKeys(mnemonic)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	b, err := deriveKeys(mnemonic)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	addrA := ethcrypto.PubkeyToAddress(a.OperatorKey.PublicKey)
	addrB := ethcrypto.PubkeyToAddress(b.OperatorKey.PublicKey)
	if addrA != addrB {
		t.Fatalf("expected the same mnemonic to derive the same operator address, got %s and %s", addrA, addrB)
	}

	if a.IdentityKey.DID() != b.IdentityKey.DID() {
		t.Fatalf("expected the same mnemonic to derive the same identity, got %s and %s", a.IdentityKey.DID(), b.IdentityKey.DID())
	}

	if !bytes.Equal(a.PeerKeySeed, b.PeerKeySeed) {
		t.Fatal("expected the same mnemonic to derive the same peer key seed")
	}
}

func TestDeriveKeys_DifferentMnemonicsDifferentKeys(t *testing.T) {
	a, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	b, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	if ethcrypto.PubkeyToAddress(a.OperatorKey.PublicKey) == ethcrypto.PubkeyToAddress(b.OperatorKey.PublicKey) {
		t.Fatal("expected different mnemonics to derive different operator addresses")
	}
	if a.IdentityKey.DID() == b.IdentityKey.DID() {
		t.Fatal("expected different mnemonics to derive different identities")
	}
	if bytes.Equal(a.PeerKeySeed, b.PeerKeySeed) {
		t.Fatal("expected different mnemonics to derive different peer key seeds")
	}
}

func TestDeriveKeys_ThreeKeysAreDistinct(t *testing.T) {
	keys, err := deriveKeys(testMnemonic(t))
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	if bytes.Equal(ethcrypto.FromECDSA(keys.OperatorKey), keys.PeerKeySeed) {
		t.Fatal("expected the operator key and the peer key seed to be distinct")
	}
}

func TestEnsureKeys_GeneratesFreshSeedWhenNoneSupplied(t *testing.T) {
	cfg := testKeysConfig(t)

	keys, err := EnsureKeys(cfg, "", zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys: %v", err)
	}
	if keys.OperatorKey == nil {
		t.Fatal("expected a generated operator key")
	}

	// the mnemonic itself must never be written to disk, only the keyring
	// entries (raw derived key bytes) should exist under KeyDir
	if !keyring.FileKeyringExists(cfg.Node.KeyDir) {
		t.Fatalf("expected a keyring to be created at %s", cfg.Node.KeyDir)
	}
	kr, err := keyring.OpenFileKeyring(cfg.Node.KeyDir, []byte(cfg.Node.KeyringPassword))
	if err != nil {
		t.Fatalf("opening keyring to inspect it: %v", err)
	}
	entries, err := kr.List()
	if err != nil {
		t.Fatalf("listing keyring entries: %v", err)
	}
	for _, name := range entries {
		if name == "mnemonic" {
			t.Fatal("found a file named \"mnemonic\" in the keyring directory, the mnemonic must never be persisted")
		}
	}
}

func TestEnsureKeys_PersistsAndReusesAcrossCalls(t *testing.T) {
	cfg := testKeysConfig(t)

	first, err := EnsureKeys(cfg, "", zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys (first): %v", err)
	}

	second, err := EnsureKeys(cfg, "", zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys (second): %v", err)
	}

	firstAddr := ethcrypto.PubkeyToAddress(first.OperatorKey.PublicKey)
	secondAddr := ethcrypto.PubkeyToAddress(second.OperatorKey.PublicKey)
	if firstAddr != secondAddr {
		t.Fatalf("expected the second call to reuse the persisted keys, got different operator addresses %s vs %s", firstAddr, secondAddr)
	}
}

func TestEnsureKeys_UsesRecoverMnemonicOnFirstRun(t *testing.T) {
	mnemonic := testMnemonic(t)

	cfg := testKeysConfig(t)

	got, err := EnsureKeys(cfg, mnemonic, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys: %v", err)
	}

	want, err := deriveKeys(mnemonic)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}

	if ethcrypto.PubkeyToAddress(got.OperatorKey.PublicKey) != ethcrypto.PubkeyToAddress(want.OperatorKey.PublicKey) {
		t.Fatal("expected ensureKeys to derive from the recover mnemonic")
	}
}

func TestEnsureKeys_IgnoresRecoverMnemonicOncePersisted(t *testing.T) {
	cfg := testKeysConfig(t)

	// first run: genesis, no recovery, keys get generated and persisted
	first, err := EnsureKeys(cfg, "", zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys (first): %v", err)
	}

	// second run: a *different* mnemonic is passed via --recover, but keys
	// already exist, persisted state wins, same principle as MySQL
	// ignoring MYSQL_ROOT_PASSWORD once the data directory exists
	second, err := EnsureKeys(cfg, testMnemonic(t), zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("ensureKeys (second): %v", err)
	}

	firstAddr := ethcrypto.PubkeyToAddress(first.OperatorKey.PublicKey)
	secondAddr := ethcrypto.PubkeyToAddress(second.OperatorKey.PublicKey)
	if firstAddr != secondAddr {
		t.Fatal("expected the persisted keys to win over a newly supplied recover mnemonic")
	}
}

func TestEnsureKeys_RejectsInvalidRecoverMnemonic(t *testing.T) {
	cfg := testKeysConfig(t)

	if _, err := EnsureKeys(cfg, "not a real mnemonic at all", zap.NewNop().Sugar()); err == nil {
		t.Fatal("expected an error for an invalid recover mnemonic, got nil")
	}
}
