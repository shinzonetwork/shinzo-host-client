package hostkey

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
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
	// ErrInvalidMnemonic means mnemonic isn't a valid BIP39 phrase.
	ErrInvalidMnemonic = errors.New("invalid BIP39 mnemonic")

	// ErrNoKeys means no keys exist yet at the keyDir Load was given.
	ErrNoKeys = errors.New("no keys found")

	// ErrCorruptKeyFile means the stored key file isn't the expected shape.
	ErrCorruptKeyFile = errors.New("corrupt key file")

	// ErrKeysAlreadyExist means a key file appeared at keyDir between
	// the exist check and the write, almost always two Ensure calls
	// racing to generate fresh keys for the same empty keyDir at once.
	ErrKeysAlreadyExist = errors.New("keys already exist")

	// ErrAlreadyHaveKeys means Ensure was given a mnemonic to recover
	// but keyDir already has keys. Recovering is only for a host that
	// doesn't have any yet.
	ErrAlreadyHaveKeys = errors.New("keys already exist, nothing to recover")
)

// ValidateMnemonic reports whether mnemonic is a valid BIP39 mnemonic.
func ValidateMnemonic(mnemonic string) error {
	if !bip39.IsMnemonicValid(normalizeMnemonic(mnemonic)) {
		return ErrInvalidMnemonic
	}
	return nil
}

// normalizeMnemonic runs before both validating and deriving. go-bip39
// validates with loose whitespace rules but hashes the raw string.
// Without this, a mnemonic saved one word per line would validate fine
// but derive the wrong keys.
func normalizeMnemonic(mnemonic string) string {
	return strings.Join(strings.Fields(strings.ToLower(mnemonic)), " ")
}

// NodeKeys holds the three keys derived from a node's seed. An
// operator key for on-chain registration, an identity key for
// DefraDB's ACP, and a peer key seed for its libp2p PeerID.
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
	seed := bip39.NewSeed(normalizeMnemonic(mnemonic), "")
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

// TODO this file is plain, not encrypted at rest. Revisit once a real
// per-instance secret exists to encrypt it with.
func storeRawKeys(keyDir string, raw rawKeys) error {
	content := hex.EncodeToString(raw.operator) + "\n" +
		hex.EncodeToString(raw.identity) + "\n" +
		hex.EncodeToString(raw.peerSeed) + "\n"

	return writeFileAtomic(filepath.Join(keyDir, nodeKeyFile), []byte(content))
}

// writeFileAtomic writes data to path, failing instead of overwriting
// if something is already there. A plain rename would silently replace
// an existing file, which matters here, two processes racing to
// generate fresh keys for the same empty keyDir must not have one
// silently clobber the other's. os.Link gives the same atomicity as a
// rename (it either lands fully or not at all), but fails with EEXIST
// if path already exists, instead of overwriting it.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

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

	if err := os.Link(tmpPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: another process may have just created it", ErrKeysAlreadyExist)
		}
		return fmt.Errorf("linking into place: %w", err)
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

// Exist reports whether a node.key file is already stored at keyDir.
func Exist(keyDir string) bool {
	_, err := os.Stat(filepath.Join(keyDir, nodeKeyFile))
	return err == nil
}

// Load reads the keys already stored at keyDir. It never generates or
// stores anything, that's why start uses it.
func Load(keyDir string) (NodeKeys, error) {
	if !Exist(keyDir) {
		return NodeKeys{}, ErrNoKeys
	}

	raw, err := loadRawKeys(keyDir)
	if err != nil {
		return NodeKeys{}, err
	}

	return raw.toNodeKeys()
}

// Ensure loads the keys already at keyDir, or generates (or recovers,
// if mnemonic is given) and stores a new set. Refuses to recover over
// keys that already exist, recovering is only for a host that doesn't
// have any yet. The returned mnemonic is empty unless one was freshly
// generated. Print it directly, immediately, before anything else
// that could still fail.
func Ensure(keyDir, mnemonic string) (NodeKeys, string, error) {
	if Exist(keyDir) {
		if mnemonic != "" {
			return NodeKeys{}, "", fmt.Errorf("%w: %s", ErrAlreadyHaveKeys, keyDir)
		}
		nodeKeys, err := Load(keyDir)
		return nodeKeys, "", err
	}

	fresh := mnemonic == ""
	if fresh {
		entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
		if err != nil {
			return NodeKeys{}, "", fmt.Errorf("generating entropy: %w", err)
		}
		mnemonic, err = bip39.NewMnemonic(entropy)
		if err != nil {
			return NodeKeys{}, "", fmt.Errorf("generating mnemonic: %w", err)
		}
	} else if err := ValidateMnemonic(mnemonic); err != nil {
		return NodeKeys{}, "", err
	}

	if err := os.MkdirAll(keyDir, dirMode); err != nil {
		return NodeKeys{}, "", fmt.Errorf("creating key dir: %w", err)
	}

	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, "", err
	}
	nodeKeys, err := raw.toNodeKeys()
	if err != nil {
		return NodeKeys{}, "", err
	}
	if err := storeRawKeys(keyDir, raw); err != nil {
		// Another process can win the race to create keys here first,
		// between our Exist check above and this store. Safe to fall
		// back to loading what it wrote only when we weren't
		// recovering a specific mnemonic ourselves, any freshly
		// generated identity was just as good as ours would've been.
		// A lost recovery must still fail loudly, silently accepting
		// whichever identity won would discard what the operator
		// asked for.
		if fresh && errors.Is(err, ErrKeysAlreadyExist) {
			nodeKeys, loadErr := Load(keyDir)
			return nodeKeys, "", loadErr
		}
		return NodeKeys{}, "", err
	}

	if fresh {
		return nodeKeys, mnemonic, nil
	}
	return nodeKeys, "", nil
}
