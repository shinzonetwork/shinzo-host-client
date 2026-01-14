package attestation

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignatureVerifier_Validation(t *testing.T) {
	tests := []struct {
		name      string
		signature constants.Signature
		cid       string
		wantErr   bool
		errMsg    string
	}{
		{
			name: "empty identity",
			signature: constants.Signature{
				Identity: "",
				Value:    "signature123",
				Type:     "ES256K",
			},
			cid:     "test-cid",
			wantErr: true,
			errMsg:  "empty identity",
		},
		{
			name: "empty CID",
			signature: constants.Signature{
				Identity: "0x1234567890abcdef",
				Value:    "signature123",
				Type:     "ES256K",
			},
			cid:     "",
			wantErr: true,
			errMsg:  "empty identity or CID",
		},
		{
			name: "invalid signature type",
			signature: constants.Signature{
				Identity: "0x1234567890abcdef",
				Value:    "signature123",
				Type:     "RSA256",
			},
			cid:     "test-cid",
			wantErr: true,
			errMsg:  "invalid signature type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use nil node - validation should fail before reaching DB check
			verifier := NewDefraSignatureVerifier(nil)

			ctx := context.Background()
			err := verifier.Verify(ctx, tt.cid, tt.signature)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCachedSignatureVerifier_MockVerifier tests the MockSignatureVerifier behavior
func TestCachedSignatureVerifier_MockVerifier(t *testing.T) {
	// Test MockSignatureVerifier
	mockVerifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature constants.Signature) error {
			if cid == "error-cid" {
				return assert.AnError
			}
			return nil
		},
	}
	ctx := context.Background()
	signature := constants.Signature{Identity: "0x123", Value: "sig", Type: "ES256K"}

	// Test success case
	err := mockVerifier.Verify(ctx, "success-cid", signature)
	assert.NoError(t, err)

	// Test error case
	err = mockVerifier.Verify(ctx, "error-cid", signature)
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestDefraSignatureVerifier_Verify_InvalidIdentityFormat(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil)

	err := verifier.Verify(ctx, "test-cid", constants.Signature{
		Type:     "es256k",
		Identity: "", // Empty identity
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

// TestDefraSignatureVerifier_NilDB tests that verification fails gracefully when DB is nil
func TestDefraSignatureVerifier_NilDB(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil)

	// Valid inputs but nil DB
	err := verifier.Verify(ctx, "test-cid", constants.Signature{
		Type:     "ES256K",
		Identity: "0x1234567890abcdef",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}

// TestMockSignatureVerifier_ConcurrentAccess tests thread safety of MockSignatureVerifier
func TestMockSignatureVerifier_ConcurrentAccess(t *testing.T) {
	callCount := 0
	mockVerifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature constants.Signature) error {
			callCount++
			return nil
		},
	}

	ctx := context.Background()
	signature := constants.Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}

	// Run concurrent tests
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for range numGoroutines {
		go func() {
			mockVerifier.Verify(ctx, "concurrent-test", signature)
			done <- true
		}()
	}

	for range numGoroutines {
		<-done
	}

	assert.Equal(t, numGoroutines, callCount)
}

// TestMockSignatureVerifier_DifferentCIDs tests that MockSignatureVerifier handles different CIDs
func TestMockSignatureVerifier_DifferentCIDs(t *testing.T) {
	callCount := 0
	mockVerifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature constants.Signature) error {
			callCount++
			return nil
		},
	}

	ctx := context.Background()
	signature := constants.Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}
	// Test different CIDs
	cids := []string{"cid1", "cid2", "cid3", "cid1", "cid2"}
	for _, cid := range cids {
		err := mockVerifier.Verify(ctx, cid, signature)
		assert.NoError(t, err)
	}

	assert.Equal(t, 5, callCount)
}
