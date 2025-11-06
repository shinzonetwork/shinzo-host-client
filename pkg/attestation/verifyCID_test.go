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

	err = verifier.VerifyCID(ctx, testDocResult.Version[0].CID, testDocResult.DocId)
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

	err = verifier.VerifyCID(ctx, "invalid-cid", "doesnt matter")
	require.Error(t, err)
}

func TestDefraSignatureVerifier_VerifyCID_InvalidDocID(t *testing.T) {
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

	err = verifier.VerifyCID(ctx, testDocResult.Version[0].CID, "invalid-doc-id")
	require.Error(t, err)
}

func TestDefraSignatureVerifier_VerifyCID_HeightGreaterThanOne(t *testing.T) {
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

	//create new version of testdoc
	updateTestDocMutation := `
		mutation {
			update_TestDoc(docID: "` + testDocResult.DocId + `", input: {name: "test2"}) {
				_docID
				_version {
					cid
				}
			}
		}
	`

	secondTestDocResult, err := defra.PostMutation[TestDoc](ctx, defraNode, updateTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, secondTestDocResult)
	require.Len(t, secondTestDocResult.Version, 1)

	if testDocResult.Version[0].CID == secondTestDocResult.Version[0].CID {
		t.Errorf("expected different CIDs, got %s", testDocResult.Version[0].CID)
	}

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.VerifyCID(ctx, secondTestDocResult.Version[0].CID, testDocResult.DocId)
	require.Error(t, err)
}
