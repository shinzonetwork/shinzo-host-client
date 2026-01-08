package attestation

import (
	"context"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
)

type SignatureVerifier interface {
	Verify(ctx context.Context, cid string, signature Signature) error
}

type DefraSignatureVerifier struct { // Implements SignatureVerifier interface
	defraNode *node.Node
}

func NewDefraSignatureVerifier(defraNode *node.Node) *DefraSignatureVerifier {
	return &DefraSignatureVerifier{
		defraNode: defraNode,
	}
}

// Verify verifies that the signature is valid for the given CID using DefraDB directly (no HTTP)
func (v *DefraSignatureVerifier) Verify(ctx context.Context, cid string, signature Signature) error {
	// Validate required fields
	if signature.Identity == "" {
		return fmt.Errorf("empty identity in signature for CID %s", cid)
	}
	if cid == "" {
		return fmt.Errorf("empty CID provided for signature verification")
	}
	if v.defraNode == nil || v.defraNode.DB == nil {
		return fmt.Errorf("defradb node or DB is not available for signature verification")
	}

	// Validate signature type - only ES256K is expected from DefraDB
	if strings.ToUpper(signature.Type) != "ES256K" {
		return fmt.Errorf("invalid signature type %s, expected ES256K", signature.Type)
	}

	// Parse the public key from the hex identity string
	pubKey, err := crypto.PublicKeyFromString(crypto.KeyTypeSecp256k1, signature.Identity)
	if err != nil {
		return fmt.Errorf("failed to parse public key from identity %s: %w", signature.Identity, err)
	}

	// Verify signature directly using the DB - no HTTP calls needed
	err = v.defraNode.DB.VerifySignature(ctx, cid, pubKey)
	if err != nil {
		return fmt.Errorf("signature verification failed for CID %s: %w", cid, err)
	}

	return nil
}

type MockSignatureVerifier struct { // Implements SignatureVerifier interface
	verifyFunc func(ctx context.Context, cid string, signature Signature) error
}

func (m *MockSignatureVerifier) Verify(ctx context.Context, cid string, signature Signature) error {
	if m.verifyFunc != nil {
		return m.verifyFunc(ctx, cid, signature)
	}
	return nil
}
