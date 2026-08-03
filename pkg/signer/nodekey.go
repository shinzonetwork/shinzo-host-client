package signer

import (
	"fmt"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	defracrypto "github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
)

// NodeECDSAKey loads the host's defradb node identity and returns its secp256k1
// key as a geth *ecdsa.PrivateKey, for recoverable (EIP-712) signing of query
// responses. The host address the signatures recover to is
// ethcrypto.PubkeyToAddress of the returned key.
func NodeECDSAKey(defraNode *node.Node, cfg *defradb.Config) (*secp256k1.PrivateKey, error) {
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
func nodeKeyToECDSA(priv defracrypto.PrivateKey) (*secp256k1.PrivateKey, error) {
	if priv == nil {
		return nil, ErrNoPrivateKey
	}
	// ToECDSA accepts any 32-byte scalar, so a secp256r1 identity would silently
	// convert to a wrong key and host address. Reject non-secp256k1 up front.
	if priv.Type() != defracrypto.KeyTypeSecp256k1 {
		return nil, fmt.Errorf("%w, got %s", ErrUnexpectedKeyType, priv.Type())
	}
	return secp256k1.PrivKeyFromBytes(priv.Raw()), nil
}
