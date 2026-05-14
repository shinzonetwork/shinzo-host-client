package attestation

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
)

// SignatureVerifier defines the interface for verifying signatures. It abstracts the verification logic, allowing for different implementations (e.g., using DefraDB directly or a mock verifier for testing). The Verify method checks if the provided signature is valid for the given CID.
type SignatureVerifier interface {
	Verify(ctx context.Context, cid string, signature constants.Signature) error
}

// SignatureMetrics defines the metrics interface for signature verification.
type SignatureMetrics interface {
	IncrementSignatureVerifications()
	IncrementSignatureFailures()
}

// DefraSignatureVerifier is an implementation of the SignatureVerifier interface that uses a DefraDB node to verify signatures directly against the database. It also reports metrics for successful verifications and failures.
type DefraSignatureVerifier struct { // Implements SignatureVerifier interface
	defraNode *node.Node
	metrics   SignatureMetrics
}

// NewDefraSignatureVerifier creates a new instance of DefraSignatureVerifier with the provided DefraDB node and metrics. This verifier will use the DefraDB node to verify signatures directly against the database, and will report metrics for successful verifications and failures.
func NewDefraSignatureVerifier(defraNode *node.Node, metrics SignatureMetrics) *DefraSignatureVerifier {
	return &DefraSignatureVerifier{
		defraNode: defraNode,
		metrics:   metrics,
	}
}

// Verify verifies that the signature is valid for the given CID using DefraDB directly (no HTTP).
func (v *DefraSignatureVerifier) Verify(ctx context.Context, cid string, signature constants.Signature) error {
	if signature.Identity == "" || cid == "" {
		return ErrEmptyIdentityOrCID
	}
	if !strings.EqualFold(signature.Type, "ES256K") {
		return fmt.Errorf("signature type %s: %w", signature.Type, ErrInvalidSignatureType)
	}
	if v.defraNode == nil || v.defraNode.DB == nil {
		return ErrDefraDBUnavailable
	}

	// Parse the public key from the hex identity string
	pubKey, err := crypto.PublicKeyFromString(crypto.KeyTypeSecp256k1, signature.Identity)
	if err != nil {
		return fmt.Errorf("failed to parse public key from identity %s: %w", signature.Identity, err)
	}

	// Verify signature directly using the DB - no HTTP calls needed
	err = v.defraNode.DB.VerifySignature(ctx, cid, pubKey)
	if err != nil {
		if v.metrics != nil {
			v.metrics.IncrementSignatureFailures()
		}
		return fmt.Errorf("signature verification failed for CID %s: %w", cid, err)
	}

	if v.metrics != nil {
		v.metrics.IncrementSignatureVerifications()
	}
	return nil
}

// MockSignatureVerifier is a mock implementation of the SignatureVerifier interface for testing purposes. It allows tests to define custom verification behavior by providing a function that will be called when Verify is invoked.
type MockSignatureVerifier struct {
	verifyFunc func(ctx context.Context, cid string, signature constants.Signature) error
}

// Verify calls the custom verify function if set, otherwise it returns nil (indicating success). This allows tests to define specific verification behavior by providing a custom function.
func (m *MockSignatureVerifier) Verify(ctx context.Context, cid string, signature constants.Signature) error {
	if m.verifyFunc != nil {
		return m.verifyFunc(ctx, cid, signature)
	}
	return nil
}
