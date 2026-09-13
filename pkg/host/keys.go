package host

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/go-bip39"
	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sourcenetwork/defradb/acp/identity"
	defracrypto "github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/keyring"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

const (
	operatorKeyPath = "44'/60'/0'/0/0"
	identityKeyPath = "44'/60'/0'/1/0"
	peerKeyPath     = "44'/60'/0'/2/0"
)

const (
	keyNameOperator = "operator"
	keyNameIdentity = "identity"
	keyNamePeerSeed = "peerseed"
)

const shinzoBech32Prefix = "shinzo"

type NodeKeys struct {
	OperatorKey *ecdsa.PrivateKey
	IdentityKey identity.FullIdentity
	PeerKeySeed []byte
}

func (k NodeKeys) OperatorAddress() common.Address {
	return ethcrypto.PubkeyToAddress(k.OperatorKey.PublicKey)
}

func (k NodeKeys) ShinzoAddress() (string, error) {
	addr, err := bech32.ConvertAndEncode(shinzoBech32Prefix, k.OperatorAddress().Bytes())
	if err != nil {
		return "", fmt.Errorf("encoding shinzo address: %w", err)
	}
	return addr, nil
}

func (k NodeKeys) DID() string {
	return k.IdentityKey.DID()
}

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

func deriveKeys(mnemonic string) (NodeKeys, error) {
	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, err
	}
	return raw.toNodeKeys()
}

func storeRawKeys(kr keyring.Keyring, raw rawKeys) error {
	if err := kr.Set(keyNameOperator, raw.operator); err != nil {
		return fmt.Errorf("storing operator key: %w", err)
	}
	if err := kr.Set(keyNameIdentity, raw.identity); err != nil {
		return fmt.Errorf("storing identity key: %w", err)
	}
	if err := kr.Set(keyNamePeerSeed, raw.peerSeed); err != nil {
		return fmt.Errorf("storing peer key seed: %w", err)
	}
	return nil
}

func loadRawKeys(kr keyring.Keyring) (rawKeys, error) {
	operator, err := kr.Get(keyNameOperator)
	if err != nil {
		return rawKeys{}, fmt.Errorf("loading operator key: %w", err)
	}
	identityBytes, err := kr.Get(keyNameIdentity)
	if err != nil {
		return rawKeys{}, fmt.Errorf("loading identity key: %w", err)
	}
	peerSeed, err := kr.Get(keyNamePeerSeed)
	if err != nil {
		return rawKeys{}, fmt.Errorf("loading peer key seed: %w", err)
	}
	return rawKeys{operator: operator, identity: identityBytes, peerSeed: peerSeed}, nil
}

func EnsureKeys(cfg *hostconfig.Config, recoverMnemonic string, log *zap.SugaredLogger) (NodeKeys, error) {
	alreadyExists := keyring.FileKeyringExists(cfg.Node.KeyDir)

	kr, err := keyring.OpenFileKeyring(cfg.Node.KeyDir, []byte(cfg.Node.KeyringPassword))
	if err != nil {
		return NodeKeys{}, fmt.Errorf("opening keyring: %w", err)
	}

	if alreadyExists {
		raw, err := loadRawKeys(kr)
		if err != nil {
			return NodeKeys{}, err
		}
		return raw.toNodeKeys()
	}

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
	} else if !bip39.IsMnemonicValid(mnemonic) {
		return NodeKeys{}, errors.New("--recover value is not a valid BIP39 mnemonic")
	}

	raw, err := deriveRawKeys(mnemonic)
	if err != nil {
		return NodeKeys{}, err
	}

	if err := storeRawKeys(kr, raw); err != nil {
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
