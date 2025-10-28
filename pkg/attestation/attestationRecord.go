package attestation

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord struct {
	DocId         string      `json:"_docID"`
	AttestedDocId string      `json:"attested_doc"`
	SourceDocId   string      `json:"source_doc"`
	Signatures    []Signature `json:"signatures"`
}

func CreateAttestationRecord(docId string, sourceDocId string, versions []Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
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
    source_doc: String
	signatures: [IndexerSignature_%s] @relation(name:"indexer_signatures")
}

type IndexerSignature_%s {
	identity: String @index
	value: String @index (unique: true)
	type: String 
	attestation: AttestationRecord_%s @relation(name:"indexer_signatures")
}`, viewName, viewName, viewName, viewName)
}

func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	collectionSDL := getAttestationRecordSDL(associatedViewName)
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(collectionSDL)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	attestationRecords := fmt.Sprintf("AttestationRecord_%s", associatedViewName)
	err = defraNode.DB.AddP2PCollections(ctx, attestationRecords)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", attestationRecords, err)
	}

	indexerSignatures := fmt.Sprintf("IndexerSignature_%s", associatedViewName)
	err = defraNode.DB.AddP2PCollections(ctx, indexerSignatures)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", indexerSignatures, err)
	}
	return nil
}

func (record *AttestationRecord) PostAttestationRecord(ctx context.Context, defraNode *node.Node, viewName string, attestedDocId string, sourceDocId string, sourceDocVersions []Version) error {
	attestationRecord, err := CreateAttestationRecord(attestedDocId, sourceDocId, sourceDocVersions)
	if err != nil {
		return fmt.Errorf("error creating attestation record: %v", err)
	}

	// Get the attestation record collection
	attestationCollectionName := fmt.Sprintf("AttestationRecord_%s", viewName)
	attestationCollection, err := defraNode.DB.GetCollectionByName(ctx, attestationCollectionName)
	if err != nil {
		return fmt.Errorf("error getting attestation collection %s: %v", attestationCollectionName, err)
	}

	// Create attestation record document
	attestationDoc, err := client.NewDocFromMap(map[string]any{
		"attested_doc": attestedDocId,
		"source_doc":   attestationRecord.SourceDocId,
	}, attestationCollection.Version())
	if err != nil {
		return fmt.Errorf("error creating attestation document: %v", err)
	}

	// Save attestation record
	err = attestationCollection.Save(ctx, attestationDoc)
	if err != nil {
		return fmt.Errorf("error saving attestation record: %v", err)
	}

	// Get the indexer signature collection
	signatureCollectionName := fmt.Sprintf("IndexerSignature_%s", viewName)
	signatureCollection, err := defraNode.DB.GetCollectionByName(ctx, signatureCollectionName)
	if err != nil {
		return fmt.Errorf("error getting signature collection %s: %v", signatureCollectionName, err)
	}

	// Create and save indexer signature documents
	for _, signature := range attestationRecord.Signatures {
		signatureDoc, err := client.NewDocFromMap(map[string]any{
			"identity":    signature.Identity,
			"value":       signature.Value,
			"type":        signature.Type,
			"attestation": attestationDoc.ID().String(),
		}, signatureCollection.Version())
		if err != nil {
			return fmt.Errorf("error creating signature document: %v", err)
		}

		err = signatureCollection.Save(ctx, signatureDoc)
		if err != nil {
			return fmt.Errorf("error saving signature document: %v", err)
		}
	}

	return nil
}
