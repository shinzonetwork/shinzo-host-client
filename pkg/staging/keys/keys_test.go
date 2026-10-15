package keys

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/go-bip39"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	return zap.NewNop().Sugar()
}

func testMnemonic(t *testing.T) string {
	t.Helper()
	entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
	require.NoError(t, err)
	mnemonic, err := bip39.NewMnemonic(entropy)
	require.NoError(t, err)
	return mnemonic
}

// deriveNodeKeys is a test-only convenience combining the two steps
// EnsureKeys itself always does separately (derive, then convert).
func deriveNodeKeys(mnemonic string) (NodeKeys, error) {
	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, err
	}
	return raw.toNodeKeys()
}

func TestDeriveRawKeysIsDeterministic(t *testing.T) {
	mnemonic := testMnemonic(t)

	a, err := deriveRawKeys(mnemonic)
	require.NoError(t, err)
	b, err := deriveRawKeys(mnemonic)
	require.NoError(t, err)

	require.Equal(t, a, b)
}

func TestDeriveRawKeysDifferByMnemonic(t *testing.T) {
	a, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	b, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)

	require.NotEqual(t, a, b)
}

func TestDeriveRawKeysProducesThreeDistinctKeys(t *testing.T) {
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)

	require.NotEqual(t, raw.operator, raw.identity)
	require.NotEqual(t, raw.operator, raw.peerSeed)
	require.NotEqual(t, raw.identity, raw.peerSeed)
}

func TestNodeKeysOperatorAddressIsDeterministic(t *testing.T) {
	mnemonic := testMnemonic(t)

	a, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	b, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)

	require.Equal(t, a.OperatorAddress(), b.OperatorAddress())
}

func TestNodeKeysPeerIDIsDeterministic(t *testing.T) {
	mnemonic := testMnemonic(t)

	a, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	b, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)

	idA, err := a.PeerID()
	require.NoError(t, err)
	idB, err := b.PeerID()
	require.NoError(t, err)

	require.Equal(t, idA, idB)
}

func TestNodeKeysDID(t *testing.T) {
	nodeKeys, err := deriveNodeKeys(testMnemonic(t))
	require.NoError(t, err)

	require.NotEmpty(t, nodeKeys.DID())
}

func TestNodeKeysShinzoAddressHasShinzoPrefix(t *testing.T) {
	nodeKeys, err := deriveNodeKeys(testMnemonic(t))
	require.NoError(t, err)

	addr, err := nodeKeys.ShinzoAddress()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(addr, "shinzo1"))
}

func TestNodeKeysShinzoAddressMatchesOperatorAddressBytes(t *testing.T) {
	nodeKeys, err := deriveNodeKeys(testMnemonic(t))
	require.NoError(t, err)

	addr, err := nodeKeys.ShinzoAddress()
	require.NoError(t, err)

	hrp, decoded, err := bech32.DecodeAndConvert(addr)
	require.NoError(t, err)
	require.Equal(t, "shinzo", hrp)
	require.Equal(t, nodeKeys.OperatorAddress().Bytes(), decoded)
}

func TestStoreAndLoadRawKeysRoundTrip(t *testing.T) {
	keyDir := t.TempDir()
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)

	require.NoError(t, storeRawKeys(keyDir, raw))

	loaded, err := loadRawKeys(keyDir)
	require.NoError(t, err)
	require.Equal(t, raw, loaded)
}

func TestKeysExistIsFalseForAnEmptyDir(t *testing.T) {
	require.False(t, keysExist(t.TempDir()))
}

func TestKeysExistIsTrueOnceStored(t *testing.T) {
	keyDir := t.TempDir()
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	require.NoError(t, storeRawKeys(keyDir, raw))

	require.True(t, keysExist(keyDir))
}

// A partial write (a crash or a full disk mid-write) must look like
// keysExist is false, not true: loading an incomplete set would just
// fail, with no way to ever regenerate since keysExist would keep saying
// something's already there.
func TestKeysExistIsFalseForAPartialWrite(t *testing.T) {
	keyDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(keyDir, operatorKeyFile), []byte("deadbeef"), keyFileMode))

	require.False(t, keysExist(keyDir))
}

// The real-world consequence of the above: EnsureKeys must recover from
// a partial write on its own, by generating a fresh, complete set,
// rather than failing forever on every subsequent call.
func TestEnsureKeysRecoversFromAPartialWrite(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.MkdirAll(keyDir, dirMode))
	require.NoError(t, os.WriteFile(filepath.Join(keyDir, operatorKeyFile), []byte("deadbeef"), keyFileMode))

	_, err := EnsureKeys(keyDir, "", testLogger(t))
	require.NoError(t, err)
	require.True(t, keysExist(keyDir))
}

func TestEnsureKeysGeneratesFreshWhenNoneSupplied(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	nodeKeys, err := EnsureKeys(keyDir, "", testLogger(t))
	require.NoError(t, err)
	require.NotNil(t, nodeKeys.OperatorKey)
	require.True(t, keysExist(keyDir))
}

func TestEnsureKeysCreatesKeyDirIfMissing(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "does", "not", "exist", "yet")

	_, err := EnsureKeys(keyDir, "", testLogger(t))
	require.NoError(t, err)
	require.DirExists(t, keyDir)
}

func TestEnsureKeysPersistsAndReusesAcrossCalls(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	first, err := EnsureKeys(keyDir, "", testLogger(t))
	require.NoError(t, err)

	second, err := EnsureKeys(keyDir, "", testLogger(t))
	require.NoError(t, err)

	require.Equal(t, first.OperatorAddress(), second.OperatorAddress())
}

func TestEnsureKeysUsesRecoverMnemonicOnFirstRun(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	mnemonic := testMnemonic(t)

	recovered, err := EnsureKeys(keyDir, mnemonic, testLogger(t))
	require.NoError(t, err)

	want, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	require.Equal(t, want.OperatorAddress(), recovered.OperatorAddress())
}

// Once keys are on disk, EnsureKeys always loads them; a second --recover
// value is never looked at again, even if it's different from the one
// that created them.
func TestEnsureKeysIgnoresRecoverMnemonicOncePersisted(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	first, err := EnsureKeys(keyDir, testMnemonic(t), testLogger(t))
	require.NoError(t, err)

	second, err := EnsureKeys(keyDir, testMnemonic(t), testLogger(t))
	require.NoError(t, err)

	require.Equal(t, first.OperatorAddress(), second.OperatorAddress())
}

func TestEnsureKeysRejectsInvalidRecoverMnemonic(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, err := EnsureKeys(keyDir, "not a real mnemonic at all", testLogger(t))
	require.ErrorIs(t, err, ErrInvalidMnemonic)
	require.False(t, keysExist(keyDir))
}

func TestValidateMnemonicAcceptsAValidOne(t *testing.T) {
	require.NoError(t, ValidateMnemonic(testMnemonic(t)))
}

func TestValidateMnemonicRejectsGarbage(t *testing.T) {
	require.ErrorIs(t, ValidateMnemonic("not a real mnemonic at all"), ErrInvalidMnemonic)
}

func TestValidateMnemonicRejectsEmpty(t *testing.T) {
	require.ErrorIs(t, ValidateMnemonic(""), ErrInvalidMnemonic)
}
