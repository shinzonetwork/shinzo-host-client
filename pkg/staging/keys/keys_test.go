package keys

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestDeriveRawKeysNormalizesWhitespaceAndCase(t *testing.T) {
	mnemonic := testMnemonic(t)
	words := strings.Fields(mnemonic)

	oneWordPerLine := strings.Join(words, "\n")
	mixedCaseAndSpacing := strings.ToUpper(strings.Join(words, "  "))

	canonical, err := deriveRawKeys(mnemonic)
	require.NoError(t, err)

	fromLines, err := deriveRawKeys(oneWordPerLine)
	require.NoError(t, err)
	require.Equal(t, canonical, fromLines)

	fromMixed, err := deriveRawKeys(mixedCaseAndSpacing)
	require.NoError(t, err)
	require.Equal(t, canonical, fromMixed)
}

func TestValidateMnemonicAcceptsOneWordPerLine(t *testing.T) {
	mnemonic := testMnemonic(t)
	oneWordPerLine := strings.Join(strings.Fields(mnemonic), "\n")

	require.NoError(t, ValidateMnemonic(oneWordPerLine))
}

func TestEnsureKeysRecoversTheSameAddressRegardlessOfMnemonicWhitespace(t *testing.T) {
	mnemonic := testMnemonic(t)
	oneWordPerLine := strings.Join(strings.Fields(mnemonic), "\n")

	canonical, _, err := EnsureKeys(filepath.Join(t.TempDir(), "keys"), mnemonic, true, testLogger(t))
	require.NoError(t, err)

	fromLines, _, err := EnsureKeys(filepath.Join(t.TempDir(), "keys"), oneWordPerLine, true, testLogger(t))
	require.NoError(t, err)

	require.Equal(t, canonical.OperatorAddress(), fromLines.OperatorAddress())
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

// storeRawKeys writes through a temp file, then links it into place.
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

// Two EnsureKeys calls racing to generate fresh keys for the same empty
// keyDir must not have one silently overwrite the other. The second
// storeRawKeys call must fail loudly instead, and the first call's
// file must survive untouched.
func TestStoreRawKeysFailsIfAlreadyStored(t *testing.T) {
	keyDir := t.TempDir()
	first, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	require.NoError(t, storeRawKeys(keyDir, first))

	second, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	require.ErrorIs(t, storeRawKeys(keyDir, second), ErrKeysAlreadyExist)

	loaded, err := loadRawKeys(keyDir)
	require.NoError(t, err)
	require.Equal(t, first, loaded, "the first call's keys must survive untouched")
}

// Simulates several EnsureKeys calls racing to generate fresh keys for
// the same empty keyDir at once. Exactly one must win, and the mnemonic
// it reports must match what actually ends up on disk, so an operator
// is never shown a seed phrase that doesn't match the stored key.
func TestEnsureKeysConcurrentGenerateNeverCorrupts(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	const n = 8
	mnemonics := make([]string, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, mnemonic, err := EnsureKeys(keyDir, "", true, testLogger(t))
			mnemonics[i] = mnemonic
			errs[i] = err
		}()
	}
	wg.Wait()

	var winner string
	successes := 0
	for i, err := range errs {
		if err == nil {
			successes++
			winner = mnemonics[i]
			continue
		}
		require.ErrorIs(t, err, ErrKeysAlreadyExist)
	}
	require.Equal(t, 1, successes, "exactly one concurrent generate should win")

	want, err := deriveNodeKeys(winner)
	require.NoError(t, err)
	wantAddr, err := want.ShinzoAddress()
	require.NoError(t, err)

	stored, _, err := EnsureKeys(keyDir, "", false, testLogger(t))
	require.NoError(t, err)
	storedAddr, err := stored.ShinzoAddress()
	require.NoError(t, err)

	require.Equal(t, wantAddr, storedAddr, "the reported winning mnemonic must match what's actually stored")
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

// A corrupted key file must never trigger regeneration over it.
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

// allowGenerate gates generation only. Recovery must still work.
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

// A recover value that doesn't match what's stored must error, not
// silently load the existing keys as if it were never given.
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

// A malformed recover value is ErrInvalidMnemonic, not ErrRecoverMismatch,
// even once keys already exist.
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
