package attestation

import (
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/view"
	"github.com/stretchr/testify/require"
)

func TestPostAttestationRecord(t *testing.T) {
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type TestDoc {
			name: String
		}
	`)

	type TestDoc struct {
		Name    string    `json:"name"`
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "TestDoc")
	require.NoError(t, err)
	defer defraNode.Close(t.Context())

	createTestDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-document"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
						__typename
					}
				}
			}
		}
	`

	testDocResult, err := defra.PostMutation[TestDoc](t.Context(), defraNode, createTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, testDocResult)
	require.Greater(t, len(testDocResult.DocId), 0)
	require.Len(t, testDocResult.Version, 1)

	testVersions := testDocResult.Version

	testViewName := "TestView"
	err = AddAttestationRecordCollection(t.Context(), defraNode, &view.View{Name: testViewName})
	require.NoError(t, err)

	attestationRecord := &AttestationRecord{}
	attestedDocId := "attested-doc-123" // This would be the View doc created after processing the view
	sourceDocId := testDocResult.DocId

	err = attestationRecord.PostAttestationRecord(t.Context(), defraNode, testViewName, attestedDocId, sourceDocId, testVersions)
	require.NoError(t, err)

	expectedAttestationCollectionName := fmt.Sprintf("AttestationRecord_%s", testViewName)
	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
			source_doc
			signatures {
				identity
				value
				type
			}
		}
	`, expectedAttestationCollectionName)

	results, err := defra.QueryArray[AttestationRecord](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	record := results[0]
	require.Equal(t, attestedDocId, record.AttestedDocId)
	require.Equal(t, testDocResult.DocId, record.SourceDocId)
	require.NotNil(t, record.Signatures)
	require.Len(t, record.Signatures, 1)
}
