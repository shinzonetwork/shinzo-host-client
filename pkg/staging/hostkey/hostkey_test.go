package hostkey

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/go-bip39"
	"github.com/stretchr/testify/require"
)

func testMnemonic(t *testing.T) string {
	t.Helper()
	entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
	require.NoError(t, err)
	mnemonic, err := bip39.NewMnemonic(entropy)
	require.NoError(t, err)
	return mnemonic
}

// deriveNodeKeys is a test-only convenience combining the two steps
// Ensure itself always does separately (derive, then convert).
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

// Two Ensure calls racing to generate fresh keys for the same empty
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

// Simulates several Ensure calls racing to generate fresh keys for the
// same empty keyDir at once. None of them may error: the one that
// loses the race to store its own generated keys must fall back to
// loading what the winner actually wrote, since neither was
// recovering a specific mnemonic, any freshly generated identity was
// as good as the loser's own would've been. Exactly one must report
// a mnemonic, and that mnemonic must match what actually ends up on
// disk, so an operator is never shown a seed phrase that doesn't
// match the stored key.
func TestEnsureConcurrentGenerateNeverCorrupts(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	const n = 8
	mnemonics := make([]string, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, mnemonic, err := Ensure(keyDir, "")
			mnemonics[i] = mnemonic
			errs[i] = err
		})
	}
	wg.Wait()

	var winner string
	generated := 0
	for i, err := range errs {
		require.NoError(t, err, "losing the race to store must fall back to loading, not error")
		if mnemonics[i] != "" {
			generated++
			winner = mnemonics[i]
		}
	}
	require.Equal(t, 1, generated, "exactly one concurrent call should freshly generate")

	want, err := deriveNodeKeys(winner)
	require.NoError(t, err)
	wantAddr, err := want.ShinzoAddress()
	require.NoError(t, err)

	stored, err := Load(keyDir)
	require.NoError(t, err)
	storedAddr, err := stored.ShinzoAddress()
	require.NoError(t, err)

	require.Equal(t, wantAddr, storedAddr, "the winning mnemonic must match what's actually stored")
}

// A lost race to generate keys falls back to loading. A lost race to
// recover a specific mnemonic must not: silently accepting whichever
// identity won would discard what the operator asked for.
func TestEnsureConcurrentRecoverStillFailsOnLostRace(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	const n = 4
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		mnemonic := testMnemonic(t)
		wg.Go(func() {
			_, _, errs[i] = Ensure(keyDir, mnemonic)
		})
	}
	wg.Wait()

	wins, losses := 0, 0
	for _, err := range errs {
		if err == nil {
			wins++
			continue
		}
		losses++
		require.ErrorIs(t, err, ErrKeysAlreadyExist)
	}
	require.Equal(t, 1, wins)
	require.Equal(t, n-1, losses)
}

func TestExistIsFalseForAnEmptyDir(t *testing.T) {
	require.False(t, Exist(t.TempDir()))
}

func TestExistIsTrueOnceStored(t *testing.T) {
	keyDir := t.TempDir()
	raw, err := deriveRawKeys(testMnemonic(t))
	require.NoError(t, err)
	require.NoError(t, storeRawKeys(keyDir, raw))

	require.True(t, Exist(keyDir))
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
func TestLoadDoesNotOverwriteACorruptFile(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.MkdirAll(keyDir, dirMode))
	path := filepath.Join(keyDir, nodeKeyFile)
	require.NoError(t, os.WriteFile(path, []byte("deadbeef\nabcd\n"), keyFileMode))

	_, err := Load(keyDir)
	require.ErrorIs(t, err, ErrCorruptKeyFile)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "deadbeef\nabcd\n", string(data), "the existing file must be left exactly as it was")
}

func TestEnsureGeneratesFreshWhenNoneSupplied(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	nodeKeys, mnemonic, err := Ensure(keyDir, "")
	require.NoError(t, err)
	require.NotNil(t, nodeKeys.OperatorKey)
	require.NoError(t, ValidateMnemonic(mnemonic), "the returned mnemonic should be a real one")
	require.True(t, Exist(keyDir))
}

func TestEnsureCreatesKeyDirIfMissing(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "does", "not", "exist", "yet")

	_, _, err := Ensure(keyDir, "")
	require.NoError(t, err)
	require.DirExists(t, keyDir)
}

// Keys generated by one Ensure call must load back identical in the next.
func TestEnsureGeneratedKeysLoadBackTheSame(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	created, _, err := Ensure(keyDir, "")
	require.NoError(t, err)

	loaded, err := Load(keyDir)
	require.NoError(t, err)

	require.Equal(t, created.OperatorAddress(), loaded.OperatorAddress())
}

// Ensure on an empty keyDir with a mnemonic given derives from exactly
// that mnemonic, not a freshly generated one.
func TestEnsureUsesRecoverMnemonicWhenGiven(t *testing.T) {
	mnemonic := testMnemonic(t)
	keyDir := filepath.Join(t.TempDir(), "keys")

	recovered, returnedMnemonic, err := Ensure(keyDir, mnemonic)
	require.NoError(t, err)
	require.Empty(t, returnedMnemonic, "recovering shouldn't report a freshly generated mnemonic")

	want, err := deriveNodeKeys(mnemonic)
	require.NoError(t, err)
	require.Equal(t, want.OperatorAddress(), recovered.OperatorAddress())
}

func TestEnsureRejectsInvalidRecoverMnemonic(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	_, _, err := Ensure(keyDir, "not a real mnemonic at all")
	require.ErrorIs(t, err, ErrInvalidMnemonic)
	require.False(t, Exist(keyDir))
}

// Recovering is only for a host that doesn't have keys yet. Once keys
// exist, Ensure must refuse rather than silently overwrite them.
func TestEnsureRefusesToRecoverWhenKeysAlreadyExist(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	original, _, err := Ensure(keyDir, "")
	require.NoError(t, err)

	_, _, err = Ensure(keyDir, testMnemonic(t))
	require.ErrorIs(t, err, ErrAlreadyHaveKeys)

	// the original keys must survive untouched
	stillThere, err := Load(keyDir)
	require.NoError(t, err)
	require.Equal(t, original.OperatorAddress(), stillThere.OperatorAddress())
}

// Once keys exist, Ensure with no mnemonic just loads them back,
// never regenerates.
func TestEnsureLoadsExistingKeysWithoutGenerating(t *testing.T) {
	keyDir := filepath.Join(t.TempDir(), "keys")

	first, _, err := Ensure(keyDir, "")
	require.NoError(t, err)

	second, mnemonic, err := Ensure(keyDir, "")
	require.NoError(t, err)
	require.Empty(t, mnemonic, "loading existing keys should not return a mnemonic")
	require.Equal(t, first.OperatorAddress(), second.OperatorAddress())
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
