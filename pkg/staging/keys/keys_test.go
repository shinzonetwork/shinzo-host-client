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

// storeRawKeys writes through a temp file, then renames it into place.
// Confirms no .tmp-* leftover survives a successful write.
func TestStoreRawKeysLeavesNoTempFileBehind(t *testing.T) {
	keyDir := t.TempDir()
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)

	require.NoError(t, storeRawKeys(keyDir, raw))

	entries, err := os.ReadDir(keyDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, nodeKeyFile, entries[0].Name())
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

func TestLoadRawKeysRejectsTrailingNewline(t *testing.T) {
	keyDir := t.TempDir()
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	require.NoError(t, storeRawKeys(keyDir, raw))

	path := filepath.Join(keyDir, nodeKeyFile)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), keyFileMode))

	_, err = loadRawKeys(keyDir)
	require.NoError(t, err, "a trailing newline should be trimmed, not fail decoding")
}

func TestLoadRawKeysRejectsWrongLineCount(t *testing.T) {
	keyDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(keyDir, nodeKeyFile), []byte("deadbeef\nabcd\n"), keyFileMode))

	_, err := loadRawKeys(keyDir)
	require.ErrorIs(t, err, ErrCorruptKeyFile)
}

func TestLoadRawKeysRejectsEmptyKeyData(t *testing.T) {
	keyDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(keyDir, nodeKeyFile), []byte("\n\n\n"), keyFileMode))

	_, err := loadRawKeys(keyDir)
	require.ErrorIs(t, err, ErrCorruptKeyFile)
}

// A corrupted but existing key file must never trigger regeneration:
// doing so would silently replace a real, possibly already-registered
// identity with a brand new one.
func TestEnsureKeysDoesNotOverwriteACorruptFile(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.MkdirAll(keyDir, dirMode))
	path := filepath.Join(keyDir, nodeKeyFile)
	require.NoError(t, os.WriteFile(path, []byte("deadbeef\nabcd\n"), keyFileMode))

	_, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.ErrorIs(t, err, ErrCorruptKeyFile)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "deadbeef\nabcd\n", string(data), "the existing file must be left exactly as it was")
}

func TestEnsureKeysGeneratesFreshWhenNoneSupplied(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	nodeKeys, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)
	require.NotNil(t, nodeKeys.OperatorKey)
	require.True(t, keysExist(keyDir))
}

func TestEnsureKeysReturnsTheGeneratedMnemonicOnlyWhenFresh(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, mnemonic, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)
	require.NoError(t, ValidateMnemonic(mnemonic), "the returned mnemonic should be a real one")

	_, mnemonic, err = EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)
	require.Empty(t, mnemonic, "loading existing keys should not return a mnemonic")
}

func TestEnsureKeysRefusesToGenerateWhenNotAllowed(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, _, err := EnsureKeys(keyDir, "", false, testLogger(t))
	require.ErrorIs(t, err, ErrNoKeys)
	require.False(t, keysExist(keyDir))
}

// allowGenerate only gates generating a fresh mnemonic. Recovering from
// a given one must still work even when generation itself is refused,
// since recovering isn't generating, the secret already exists.
func TestEnsureKeysAllowsRecoverEvenWhenGenerateIsRefused(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	mnemonic := testMnemonic(t)

	nodeKeys, returnedMnemonic, err := EnsureKeys(keyDir, mnemonic, false, testLogger(t))
	require.NoError(t, err)
	require.Empty(t, returnedMnemonic, "recovering shouldn't report a freshly generated mnemonic")

	want, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	require.Equal(t, want.OperatorAddress(), nodeKeys.OperatorAddress())
}

func TestEnsureKeysCreatesKeyDirIfMissing(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "does", "not", "exist", "yet")

	_, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)
	require.DirExists(t, keyDir)
}

func TestEnsureKeysPersistsAndReusesAcrossCalls(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	first, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)

	second, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)

	require.Equal(t, first.OperatorAddress(), second.OperatorAddress())
}

func TestEnsureKeysUsesRecoverMnemonicOnFirstRun(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	mnemonic := testMnemonic(t)

	recovered, _, err := EnsureKeys(keyDir, mnemonic, true, testLogger(t))
	require.NoError(t, err)

	want, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	require.Equal(t, want.OperatorAddress(), recovered.OperatorAddress())
}

// Once keys are on disk, a recover value that actually matches them is
// accepted, not just silently allowed through.
func TestEnsureKeysAllowsMatchingRecoverMnemonicOncePersisted(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	mnemonic := testMnemonic(t)

	first, _, err := EnsureKeys(keyDir, mnemonic, true, testLogger(t))
	require.NoError(t, err)

	second, _, err := EnsureKeys(keyDir, mnemonic, true, testLogger(t))
	require.NoError(t, err)

	require.Equal(t, first.OperatorAddress(), second.OperatorAddress())
}

// A recover value that doesn't match what's already stored must error,
// not silently load the existing keys as if the mnemonic was never
// given. Otherwise a recovery attempt against the wrong instance looks
// like it worked when it never actually checked anything.
func TestEnsureKeysErrorsOnMismatchedRecoverMnemonic(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, _, err := EnsureKeys(keyDir, testMnemonic(t), true, testLogger(t))
	require.NoError(t, err)

	_, _, err = EnsureKeys(keyDir, testMnemonic(t), true, testLogger(t))
	require.ErrorIs(t, err, ErrRecoverMismatch)
}

func TestEnsureKeysRejectsInvalidRecoverMnemonic(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, _, err := EnsureKeys(keyDir, "not a real mnemonic at all", true, testLogger(t))
	require.ErrorIs(t, err, ErrInvalidMnemonic)
	require.False(t, keysExist(keyDir))
}

// A malformed recover value must be reported the same way whether keys
// already exist or not. Without validating up front, this would instead
// surface as ErrRecoverMismatch once keys exist: deriveRawKeys doesn't
// check mnemonic validity, so a typo just derives the wrong seed and
// looks like a mismatch rather than a bad input.
func TestEnsureKeysRejectsInvalidRecoverMnemonicWhenKeysAlreadyExist(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, _, err := EnsureKeys(keyDir, "", true, testLogger(t))
	require.NoError(t, err)

	_, _, err = EnsureKeys(keyDir, "not a real mnemonic at all", true, testLogger(t))
	require.ErrorIs(t, err, ErrInvalidMnemonic)
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
