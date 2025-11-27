package attestation

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

func TestDefraSignatureVerifier_Verify_EmptyIdentity(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", attestation.Signature{
		Type:     "es256k",
		Identity: "",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

func TestDefraSignatureVerifier_Verify_EmptyCID(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "", attestation.Signature{
		Type:     "es256k",
		Identity: "identity",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty CID")
}

func TestDefraSignatureVerifier_Verify_UnsupportedSignatureType(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", attestation.Signature{
		Type:     "unsupported-type",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected signature type")
}

func TestDefraSignatureVerifier_Verify_InvalidIdentityFormat(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	verifier := NewDefraSignatureVerifier(defraNode)

	// Test with invalid base64
	err = verifier.Verify(ctx, "test-cid", attestation.Signature{
		Type:     "es256k",
		Identity: "not-valid-base64!!!",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected format")
}

func TestDefraSignatureVerifier_Verify_ValidSignature(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

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
		Version []attestation.Version `json:"_version"`
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

	err := verifier.Verify(ctx, "test-cid", attestation.Signature{
		Type:     "es256k",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or API URL is not available")
}

