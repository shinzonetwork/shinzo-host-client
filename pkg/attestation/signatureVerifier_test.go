package attestation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMetrics implements SignatureMetrics for testing
type testMetrics struct {
	verifications atomic.Int64
	failures      atomic.Int64
}

func (m *testMetrics) IncrementSignatureVerifications() {
	m.verifications.Add(1)
}

func (m *testMetrics) IncrementSignatureFailures() {
	m.failures.Add(1)
}

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
			verifier := NewDefraSignatureVerifier(nil, nil)

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
	verifier := NewDefraSignatureVerifier(nil, nil)

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
	verifier := NewDefraSignatureVerifier(nil, nil)

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
	var callCount atomic.Int64
	mockVerifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature constants.Signature) error {
			callCount.Add(1)
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

	assert.Equal(t, int64(numGoroutines), callCount.Load())
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

// ========================================
// DEFRA SIGNATURE VERIFIER - PUBKEY PARSE ERROR
// ========================================

// setupDefraForVerifier creates a defra client with the attestation schema and returns the node.
func setupDefraForVerifier(t *testing.T) *defra.Client {
	t.Helper()
	testSchema := `
		type TestDoc {
			name: String
		}
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { client.Stop(t.Context()) })

	err = client.ApplySchema(t.Context(), testSchema)
	require.NoError(t, err)
	return client
}

func TestDefraSignatureVerifier_Verify_InvalidPublicKeyParse(t *testing.T) {
	// This test covers the PublicKeyFromString error path (line 54-57).
	// We need a real defra node so we pass the nil node check,
	// but provide an identity that is valid hex but invalid secp256k1 key.
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	verifier := NewDefraSignatureVerifier(defraNode, nil)

	err := verifier.Verify(ctx, "test-cid", constants.Signature{
		Type:     "ES256K",
		Identity: "aabb", // Valid hex but only 2 bytes, not a valid secp256k1 key
		Value:    "sig-value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse public key from identity")
}

func TestDefraSignatureVerifier_Verify_InvalidPublicKeyParse_NonHex(t *testing.T) {
	// Identity that is not valid hex at all
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	verifier := NewDefraSignatureVerifier(defraNode, nil)

	err := verifier.Verify(ctx, "test-cid", constants.Signature{
		Type:     "ES256K",
		Identity: "zzzz-not-hex", // Not valid hex
		Value:    "sig-value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse public key from identity")
}

func TestDefraSignatureVerifier_Verify_VerifySignatureFailure_WithMetrics(t *testing.T) {
	// This test covers: valid public key parsed, but VerifySignature on DB fails
	// because the CID does not exist in the DB. Also tests metrics.IncrementSignatureFailures().
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	metrics := &testMetrics{}

	// Generate a real secp256k1 key pair
	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	pubKeyHex := privKey.GetPublic().String()

	verifier := NewDefraSignatureVerifier(defraNode, metrics)

	// Use a CID that does not exist in the DB
	err = verifier.Verify(ctx, "nonexistent-cid", constants.Signature{
		Type:     "ES256K",
		Identity: pubKeyHex,
		Value:    "dummy-sig",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature verification failed for CID")
	require.Equal(t, int64(1), metrics.failures.Load())
	require.Equal(t, int64(0), metrics.verifications.Load())
}

func TestDefraSignatureVerifier_Verify_VerifySignatureFailure_NilMetrics(t *testing.T) {
	// Same as above but with nil metrics to cover the nil metrics branch
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	pubKeyHex := privKey.GetPublic().String()

	verifier := NewDefraSignatureVerifier(defraNode, nil)

	err = verifier.Verify(ctx, "nonexistent-cid", constants.Signature{
		Type:     "ES256K",
		Identity: pubKeyHex,
		Value:    "dummy-sig",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature verification failed for CID")
}

func TestDefraSignatureVerifier_Verify_Success_WithMetrics(t *testing.T) {
	// This test covers the full success path including metrics.IncrementSignatureVerifications().
	// We create a document in DefraDB, get its CID and signature, then verify.
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	metrics := &testMetrics{}

	type TestDoc struct {
		Name    string    `json:"name"`
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	createDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-for-verify"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
					}
				}
			}
		}
	`

	docResult, err := defra.PostMutation[TestDoc](ctx, defraNode, createDocMutation)
	require.NoError(t, err)
	require.NotNil(t, docResult)
	require.Len(t, docResult.Version, 1)

	version := docResult.Version[0]

	verifier := NewDefraSignatureVerifier(defraNode, metrics)

	err = verifier.Verify(ctx, version.CID, version.Signature)
	require.NoError(t, err)
	require.Equal(t, int64(1), metrics.verifications.Load())
	require.Equal(t, int64(0), metrics.failures.Load())
}

func TestDefraSignatureVerifier_Verify_Success_NilMetrics(t *testing.T) {
	// Same as above but with nil metrics to cover the nil metrics branch on success path
	client := setupDefraForVerifier(t)
	defraNode := client.GetNode()
	ctx := context.Background()

	type TestDoc struct {
		Name    string    `json:"name"`
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	createDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-for-verify-nil-metrics"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
					}
				}
			}
		}
	`

	docResult, err := defra.PostMutation[TestDoc](ctx, defraNode, createDocMutation)
	require.NoError(t, err)
	require.NotNil(t, docResult)
	require.Len(t, docResult.Version, 1)

	version := docResult.Version[0]

	verifier := NewDefraSignatureVerifier(defraNode, nil)

	err = verifier.Verify(ctx, version.CID, version.Signature)
	require.NoError(t, err)
}

// ========================================
// DEFRA SIGNATURE VERIFIER - NODE WITH NIL DB
// ========================================

func TestDefraSignatureVerifier_Verify_NodeWithNilDB(t *testing.T) {
	// Test the path where defraNode is non-nil but DB is nil.
	// node.Node has a public DB field (an interface), so a zero-value Node has DB == nil.
	ctx := context.Background()

	emptyNode := &node.Node{}
	verifier := NewDefraSignatureVerifier(emptyNode, nil)

	err := verifier.Verify(ctx, "test-cid", constants.Signature{
		Type:     "ES256K",
		Identity: "aabbccdd",
		Value:    "sig-value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}
