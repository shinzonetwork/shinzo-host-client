package keys

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

const (
	operatorKeyFile = "operator.key"
	identityKeyFile = "identity.key"
	peerSeedFile    = "peerseed.key"
)

const shinzoBech32Prefix = "shinzo"

const (
	keyFileMode = 0o600
	dirMode     = 0o700
)

// ErrInvalidMnemonic is returned by ValidateMnemonic, and by EnsureKeys
// for the same reason, when a recover value isn't a valid BIP39 mnemonic.
var ErrInvalidMnemonic = errors.New("--recover value is not a valid BIP39 mnemonic")

// ValidateMnemonic reports whether mnemonic is a valid BIP39 mnemonic.
// Meant to be called before any other work happens, e.g. before
// hostconfig.Setup, so a bad --recover value is caught before anything
// is written to disk, rather than after.
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

// TODO: these are plain files, not encrypted at rest. A real keyring
// needs a password, and there's no good source for one yet that isn't
// either hardcoded (same password on every default-configured node) or
// required on every start (which defeats persisting the keys at all).
// Revisit once a real per-instance secret exists.
func storeRawKeys(keyDir string, raw rawKeys) error {
	files := map[string][]byte{
		operatorKeyFile: raw.operator,
		identityKeyFile: raw.identity,
		peerSeedFile:    raw.peerSeed,
	}

	for name, key := range files {
		path := filepath.Join(keyDir, name)
		if err := os.WriteFile(path, []byte(hex.EncodeToString(key)), keyFileMode); err != nil {
			return fmt.Errorf("storing %s: %w", name, err)
		}
	}

	return nil
}

func loadRawKeys(keyDir string) (rawKeys, error) {
	read := func(name string) ([]byte, error) {
		data, err := os.ReadFile(filepath.Join(keyDir, name)) //nolint:gosec // keyDir is operator-resolved, not untrusted input
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", name, err)
		}
		return hex.DecodeString(string(data))
	}

	operator, err := read(operatorKeyFile)
	if err != nil {
		return rawKeys{}, err
	}
	identityBytes, err := read(identityKeyFile)
	if err != nil {
		return rawKeys{}, err
	}
	peerSeed, err := read(peerSeedFile)
	if err != nil {
		return rawKeys{}, err
	}

	return rawKeys{operator: operator, identity: identityBytes, peerSeed: peerSeed}, nil
}

// keysExist reports whether a complete set of keys is already stored at
// keyDir. All three files are checked, not just one: a partial write
// (a crash or a full disk mid-write) must be treated as not existing, so
// the next call regenerates a complete, consistent set instead of
// permanently failing to load an incomplete one.
func keysExist(keyDir string) bool {
	for _, name := range []string{operatorKeyFile, identityKeyFile, peerSeedFile} {
		if _, err := os.Stat(filepath.Join(keyDir, name)); err != nil {
			return false
		}
	}
	return true
}

// EnsureKeys loads the node's keys from keyDir if they already exist, or
// generates (or recovers from recoverMnemonic, if given) and stores a new
// set otherwise. Safe to call on every start: it's a no-op past the first
// run.
func EnsureKeys(keyDir, recoverMnemonic string, log *zap.SugaredLogger) (NodeKeys, error) {
	if err := os.MkdirAll(keyDir, dirMode); err != nil {
		return NodeKeys{}, fmt.Errorf("creating key dir: %w", err)
	}

	if keysExist(keyDir) {
		log.Infow("loading existing keys", "dir", keyDir)
		raw, err := loadRawKeys(keyDir)
		if err != nil {
			return NodeKeys{}, err
		}
		return raw.toNodeKeys()
	}
	log.Infow("creating new keys", "dir", keyDir)

	mnemonic := recoverMnemonic
	fresh := mnemonic == ""

	if fresh {
		entropy, err := bip39.NewEntropy(128) //nolint:mnd // 128 bits -> 12 words
		if err != nil {
			return NodeKeys{}, fmt.Errorf("generating entropy: %w", err)
		}
		mnemonic, err = bip39.NewMnemonic(entropy)
		if err != nil {
			return NodeKeys{}, fmt.Errorf("generating mnemonic: %w", err)
		}
	} else if err := ValidateMnemonic(mnemonic); err != nil {
		return NodeKeys{}, err
	}

	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, err
	}

	if err := storeRawKeys(keyDir, raw); err != nil {
		return NodeKeys{}, err
	}

	if fresh {
		log.Warnw("generated a new node seed, write this phrase down now, "+
			"it is the only way to recover this node if this machine is lost, "+
			"it is not saved anywhere",
			"mnemonic", mnemonic)
	}

	return raw.toNodeKeys()
}
