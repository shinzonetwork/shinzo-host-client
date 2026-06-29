package signer

import (
	"crypto/ecdsa"
	"fmt"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	defracrypto "github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
)

// NodeECDSAKey loads the host's defradb node identity and returns its secp256k1
// key as a geth *ecdsa.PrivateKey, for recoverable (EIP-712) signing of query
// responses. The host address the signatures recover to is
// ethcrypto.PubkeyToAddress of the returned key.
func NodeECDSAKey(defraNode *node.Node, cfg *defradb.Config) (*ecdsa.PrivateKey, error) {
	storePath, err := getStorePath(defraNode, cfg)
	if err != nil {
		return nil, fmt.Errorf("get store path: %w", err)
	}
	fullIdentity, err := loadIdentityFromStoreFn(cfg, storePath)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}
	return nodeKeyToECDSA(fullIdentity.PrivateKey())
}

// nodeKeyToECDSA converts a defradb secp256k1 private key to a geth
// *ecdsa.PrivateKey. The raw 32-byte scalar is identical in both
// representations.
func nodeKeyToECDSA(priv defracrypto.PrivateKey) (*ecdsa.PrivateKey, error) {
	if priv == nil {
		return nil, ErrNoPrivateKey
	}
	key, err := ethcrypto.ToECDSA(priv.Raw())
	if err != nil {
		return nil, fmt.Errorf("convert node key to ecdsa: %w", err)
	}
	return key, nil
}
