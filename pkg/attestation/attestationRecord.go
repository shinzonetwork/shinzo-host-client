package attestation

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord struct {
	DocId         string      `json:"_docID"`
	AttestedDocId string      `json:"attested_doc"`
	SourceDocIds  []string    `json:"source_docs"`
	Signatures    []Signature `json:"signatures"`
}

func CreateAttestationRecord(docId string, sourceDocIds []string, versions []Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocIds:  sourceDocIds,
		Signatures:    []Signature{},
	}
	for _, version := range versions {
		attestationRecord.Signatures = append(attestationRecord.Signatures, version.Signature)
	}

	return attestationRecord, nil
}

func getAttestationRecordSDL(viewName string) string {
	// Omitting the IndexerSignature schema (even if included in schema/schema.graphql) causes an error because those objects are included in the AttestationRecord
	// If either AttestationRecord or IndexerSignature do not have unique names, we will get an error when trying to apply them as a schema (collection already exists error)
	// We want a separate collection of AttestationRecords for each View so that app clients don't receive all AttestationRecords, only those that are relevant to the collections/Views they care about - we can just append the View names as those must also be unique
	return fmt.Sprintf(`type AttestationRecord_%s {
	attested_doc: String
    source_docs: [String]
	signatures: [IndexerSignature_%s] @relation(name:"indexer_signatures")
}

type IndexerSignature_%s {
	identity: String @index
	value: String @index (unique: true)
	type: String 
	attestation: AttestationRecord_%s @relation(name:"indexer_signatures")
}`, viewName, viewName, viewName, viewName)
}

func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedView *view.View) error {
	collectionSDL := getAttestationRecordSDL(associatedView.Name)
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(collectionSDL)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	attestationRecords := fmt.Sprintf("AttestationRecord_%s", associatedView.Name)
	err = defraNode.DB.AddP2PCollections(ctx, attestationRecords)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", attestationRecords, err)
	}

	indexerSignatures := fmt.Sprintf("IndexerSignature_%s", associatedView.Name)
	err = defraNode.DB.AddP2PCollections(ctx, indexerSignatures)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", indexerSignatures, err)
	}
	return nil
}
