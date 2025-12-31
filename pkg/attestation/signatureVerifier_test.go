package attestation

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

func TestDefraSignatureVerifier_Verify_EmptyIdentity(t *testing.T) {
	ctx := context.Background()
	
	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`
	
	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers
	
	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())
	
	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

func TestDefraSignatureVerifier_Verify_EmptyCID(t *testing.T) {
	ctx := context.Background()
	
	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`
	
	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers
	
	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())
	
	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "", Signature{
		Type:     "es256k",
		Identity: "identity",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty CID")
}

func TestDefraSignatureVerifier_Verify_UnsupportedSignatureType(t *testing.T) {
	ctx := context.Background()
	
	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`
	
	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers
	
	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())
	
	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", Signature{
		Type:     "unsupported-type",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid signature type")
}

func TestDefraSignatureVerifier_Verify_InvalidIdentityFormat(t *testing.T) {
	ctx := context.Background()
	
	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`
	
	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers
	
	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())
	
	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	// Test with invalid base64
	err = verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "not-valid-base64!!!",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "identity must be valid hex")
}

func TestDefraSignatureVerifier_Verify_ValidSignature(t *testing.T) {
	ctx := context.Background()
	
	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`
	
	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers
	
	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())
	
	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a test document to get a real CID and signature
	createTestDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-document"}) {
				_docID
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

	type TestDoc struct {
		DocId   string                `json:"_docID"`
		Version []Version `json:"_version"`
	}

	testDocResult, err := defra.PostMutation[TestDoc](ctx, defraNode, createTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, testDocResult)
	require.Len(t, testDocResult.Version, 1)

	version := testDocResult.Version[0]
	require.NotEmpty(t, version.CID)
	require.NotEmpty(t, version.Signature.Identity)

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, version.CID, version.Signature)
	require.NoError(t, err)
}

func TestDefraSignatureVerifier_Verify_WithNilNode(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil)

	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or API URL is not available")
}
