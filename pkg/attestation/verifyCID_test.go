package attestation

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

func TestDefraSignatureVerifier_VerifyCID(t *testing.T) {
	ctx := context.Background()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	//create new testdoc and return cid
	createTestDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-document"}) {
				_docID
				_version {
					cid
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

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.VerifyCID(ctx, testDocResult.Version[0].CID, "TestDoc")
	require.NoError(t, err)
}

func TestDefraSignatureVerifier_VerifyCID_InvalidCID(t *testing.T) {
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

	err = verifier.VerifyCID(ctx, "invalid-cid", "TestDoc")
	require.Error(t, err)
}
