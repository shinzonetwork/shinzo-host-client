package keys

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/go-bip39"
	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sourcenetwork/defradb/acp/identity"
	defracrypto "github.com/sourcenetwork/defradb/crypto"
	"go.uber.org/zap"
)

const (
	operatorKeyPath = "44'/60'/0'/0/0"
	identityKeyPath = "44'/60'/0'/1/0"
	peerKeyPath     = "44'/60'/0'/2/0"
)

const nodeKeyFile = "node.key"

const shinzoBech32Prefix = "shinzo"

const (
	keyFileMode = 0o600
	dirMode     = 0o700
)

const keyLineCount = 3

var (
	// ErrInvalidMnemonic is returned by ValidateMnemonic, and by
	// EnsureKeys for the same reason, when a recover value isn't a
	// valid BIP39 mnemonic. Deliberately generic, not tied to any CLI
	// flag name, since more than one caller can reach this. Callers
	// that want to name their own flag should wrap this error.
	ErrInvalidMnemonic = errors.New("invalid BIP39 mnemonic")

	// ErrNoKeys is returned by EnsureKeys when allowGenerate is false,
	// nothing exists yet at keyDir, and no recover value was given.
	ErrNoKeys = errors.New("no keys found")

	// ErrRecoverMismatch is returned by EnsureKeys when a recover value
	// is given but keys already exist, and the given mnemonic derives a
	// different operator key than the one already stored.
	ErrRecoverMismatch = errors.New("recover value does not match the stored keys")

	// ErrCorruptKeyFile is returned by loadRawKeys when the stored key
	// file doesn't have the expected shape, e.g. a crash or manual edit
	// left it with too few lines or empty key data.
	ErrCorruptKeyFile = errors.New("corrupt key file")
)

// ValidateMnemonic reports whether mnemonic is a valid BIP39 mnemonic.
// Meant to be called before any other work happens, e.g. before
// hostconfig.Setup, so a bad recover value is caught before anything is
// written to disk, rather than after.
func ValidateMnemonic(mnemonic string) error {
	if !bip39.IsMnemonicValid(mnemonic) {
		return ErrInvalidMnemonic
	}
	return nil
}

// NodeKeys holds the three keys derived from a node's seed: an operator
// key for on-chain registration, an identity key for DefraDB's ACP, and a
// peer key seed for its libp2p PeerID.
type NodeKeys struct {
	OperatorKey *ecdsa.PrivateKey
	IdentityKey identity.FullIdentity
	PeerKeySeed []byte
}

// OperatorAddress is the Ethereum address the operator key signs as.
func (k NodeKeys) OperatorAddress() common.Address {
	return ethcrypto.PubkeyToAddress(k.OperatorKey.PublicKey)
}

// ShinzoAddress is the operator address, bech32 encoded with the shinzo prefix.
func (k NodeKeys) ShinzoAddress() (string, error) {
	addr, err := bech32.ConvertAndEncode(shinzoBech32Prefix, k.OperatorAddress().Bytes())
	if err != nil {
		return "", fmt.Errorf("encoding shinzo address: %w", err)
	}
	return addr, nil
}

// DID is the node DefraDB identity, as a DID string.
func (k NodeKeys) DID() string {
	return k.IdentityKey.DID()
}

// PeerID regenerates the node libp2p peer ID from its key seed.
func (k NodeKeys) PeerID() (peer.ID, error) {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(bytes.NewReader(k.PeerKeySeed))
	if err != nil {
		return "", fmt.Errorf("regenerating p2p key: %w", err)
	}
	id, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		return "", fmt.Errorf("deriving peer id: %w", err)
	}
	return id, nil
}

type rawKeys struct {
	operator []byte
	identity []byte
	peerSeed []byte
}

func deriveRawKeys(mnemonic string) (rawKeys, error) {
	seed := bip39.NewSeed(mnemonic, "")
	masterKey, chainCode := hd.ComputeMastersFromSeed(seed)

	operator, err := hd.DerivePrivateKeyForPath(masterKey, chainCode, operatorKeyPath)
	if err != nil {
		return rawKeys{}, fmt.Errorf("deriving operator key: %w", err)
	}

	identityBytes, err := hd.DerivePrivateKeyForPath(masterKey, chainCode, identityKeyPath)
	if err != nil {
		return rawKeys{}, fmt.Errorf("deriving identity key: %w", err)
	}

	peerSeed, err := hd.DerivePrivateKeyForPath(masterKey, chainCode, peerKeyPath)
	if err != nil {
		return rawKeys{}, fmt.Errorf("deriving peer key seed: %w", err)
	}

	return rawKeys{operator: operator, identity: identityBytes, peerSeed: peerSeed}, nil
}

func (r rawKeys) toNodeKeys() (NodeKeys, error) {
	operatorKey, err := ethcrypto.ToECDSA(r.operator)
	if err != nil {
		return NodeKeys{}, fmt.Errorf("operator key: %w", err)
	}

	identityPrivKey, err := defracrypto.PrivateKeyFromBytes(defracrypto.KeyTypeSecp256k1, r.identity)
	if err != nil {
		return NodeKeys{}, fmt.Errorf("identity key: %w", err)
	}
	fullIdentity, err := identity.FromPrivateKey(identityPrivKey)
	if err != nil {
		return NodeKeys{}, fmt.Errorf("building identity: %w", err)
	}

	return NodeKeys{
		OperatorKey: operatorKey,
		IdentityKey: fullIdentity,
		PeerKeySeed: r.peerSeed,
	}, nil
}

// TODO: this file is plain, not encrypted at rest. A real keyring needs
// a password, and there's no good source for one yet that isn't either
// hardcoded (same password on every default-configured node) or
// required on every start (which defeats persisting the keys at all).
// Revisit once a real per-instance secret exists.
//
// All three keys go in one file, one line each, written atomically
// (temp file, fsync, rename). That makes a partial write impossible: a
// crash or a full disk mid-write either leaves the old file untouched or
// produces nothing, never a half-written file in its place.
func storeRawKeys(keyDir string, raw rawKeys) error {
	content := hex.EncodeToString(raw.operator) + "\n" +
		hex.EncodeToString(raw.identity) + "\n" +
		hex.EncodeToString(raw.peerSeed) + "\n"

	return writeFileAtomic(filepath.Join(keyDir, nodeKeyFile), []byte(content))
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op once the rename below succeeds

	if err := tmp.Chmod(keyFileMode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("setting temp file permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("syncing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming into place: %w", err)
	}
	return nil
}

func loadRawKeys(keyDir string) (rawKeys, error) {
	data, err := os.ReadFile(filepath.Join(keyDir, nodeKeyFile)) //nolint:gosec // keyDir is operator-resolved, not untrusted input
	if err != nil {
		return rawKeys{}, fmt.Errorf("loading keys: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != keyLineCount {
		return rawKeys{}, fmt.Errorf("%w: expected %d lines, got %d", ErrCorruptKeyFile, keyLineCount, len(lines))
	}

	operator, err := hex.DecodeString(strings.TrimSpace(lines[0]))
	if err != nil {
		return rawKeys{}, fmt.Errorf("decoding operator key: %w", err)
	}
	identityBytes, err := hex.DecodeString(strings.TrimSpace(lines[1]))
	if err != nil {
		return rawKeys{}, fmt.Errorf("decoding identity key: %w", err)
	}
	peerSeed, err := hex.DecodeString(strings.TrimSpace(lines[2]))
	if err != nil {
		return rawKeys{}, fmt.Errorf("decoding peer seed: %w", err)
	}

	if len(operator) == 0 || len(identityBytes) == 0 || len(peerSeed) == 0 {
		return rawKeys{}, fmt.Errorf("%w: empty key data", ErrCorruptKeyFile)
	}

	return rawKeys{operator: operator, identity: identityBytes, peerSeed: peerSeed}, nil
}

func keysExist(keyDir string) bool {
	_, err := os.Stat(filepath.Join(keyDir, nodeKeyFile))
	return err == nil
}

// EnsureKeys loads the node's keys from keyDir if they already exist, or
// generates (or recovers from recoverMnemonic, if given) and stores a
// new set otherwise. Safe to call on every start: it's a no-op past the
// first run.
//
// If keys already exist and recoverMnemonic is also given, it must
// match what's stored (ErrRecoverMismatch otherwise), rather than being
// silently ignored: a recovery that looks like it worked but was never
// actually checked is worse than one that errors.
//
// If keys don't exist yet, mnemonic is empty, and allowGenerate is
// false, EnsureKeys returns ErrNoKeys instead of generating one. init
// passes true; start passes false, since start can run unattended and
// a freshly generated, unrecoverable seed needs someone to actually see
// it.
//
// The second return value is the mnemonic, but only when one was just
// generated. The caller is expected to show it to the operator directly
// (e.g. on cmd.OutOrStdout()), not through a logger: a logger's level
// filtering, or a non-terminal destination turning it into a structured
// log field, both risk the one unrecoverable copy never actually
// reaching anyone.
func EnsureKeys(keyDir, recoverMnemonic string, allowGenerate bool, log *zap.SugaredLogger) (NodeKeys, string, error) {
	// Validated once, up front, so a malformed recover value is always
	// reported as ErrInvalidMnemonic. Without this, a typo caught here
	// when keys don't exist yet would instead surface as the more
	// confusing ErrRecoverMismatch once keys do exist: deriveRawKeys
	// doesn't check mnemonic validity, it just derives whatever seed a
	// bad string happens to produce, which then simply fails to match.
	if recoverMnemonic != "" {
		if err := ValidateMnemonic(recoverMnemonic); err != nil {
			return NodeKeys{}, "", err
		}
	}

	if err := os.MkdirAll(keyDir, dirMode); err != nil {
		return NodeKeys{}, "", fmt.Errorf("creating key dir: %w", err)
	}

	if keysExist(keyDir) {
		log.Infow("loading existing keys", "dir", keyDir)
		raw, err := loadRawKeys(keyDir)
		if err != nil {
			return NodeKeys{}, "", err
		}

		if recoverMnemonic != "" {
			if err := checkRecoverMatches(recoverMnemonic, raw); err != nil {
				return NodeKeys{}, "", err
			}
		}

		nodeKeys, err := raw.toNodeKeys()
		return nodeKeys, "", err
	}

	mnemonic := recoverMnemonic
	fresh := mnemonic == ""

	if fresh && !allowGenerate {
		return NodeKeys{}, "", ErrNoKeys
	}

	log.Infow("creating new keys", "dir", keyDir)

	if fresh {
		entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
		if err != nil {
			return NodeKeys{}, "", fmt.Errorf("generating entropy: %w", err)
		}
		mnemonic, err = bip39.NewMnemonic(entropy)
		if err != nil {
			return NodeKeys{}, "", fmt.Errorf("generating mnemonic: %w", err)
		}
	}

	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, "", err
	}

	if err := storeRawKeys(keyDir, raw); err != nil {
		return NodeKeys{}, "", err
	}

	nodeKeys, err := raw.toNodeKeys()
	if err != nil {
		return NodeKeys{}, "", err
	}

	if fresh {
		log.Warnw("generated a new node seed, it will be shown once and is not saved anywhere, write it down now")
		return nodeKeys, mnemonic, nil
	}

	return nodeKeys, "", nil
}

func checkRecoverMatches(recoverMnemonic string, stored rawKeys) error {
	candidate, err := deriveRawKeys(recoverMnemonic)
	if err != nil {
		return err
	}
	if !bytes.Equal(candidate.operator, stored.operator) {
		return ErrRecoverMismatch
	}
	return nil
}
