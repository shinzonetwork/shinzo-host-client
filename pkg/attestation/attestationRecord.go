package attestation

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord attestation.AttestationRecord

func CreateAttestationRecord(docId string, sourceDocId string, versions []attestation.Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		Signatures:    []attestation.Signature{},
	}
	for _, version := range versions {
		attestationRecord.Signatures = append(attestationRecord.Signatures, version.Signature)
	}

	return attestationRecord, nil
}

func (record *AttestationRecord) PostAttestationRecord(ctx context.Context, defraNode *node.Node, viewName string) error {
	// Get the attestation record collection
	attestationCollectionName := fmt.Sprintf("AttestationRecord_%s", viewName)
	attestationCollection, err := defraNode.DB.GetCollectionByName(ctx, attestationCollectionName)
	if err != nil {
		return fmt.Errorf("error getting attestation collection %s: %v", attestationCollectionName, err)
	}

	// Create attestation record document using the receiver's fields
	attestationDoc, err := client.NewDocFromMap(map[string]any{
		"attested_doc": record.AttestedDocId,
		"source_doc":   record.SourceDocId,
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

	// Create and save indexer signature documents using the receiver's signatures
	for _, signature := range record.Signatures {
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
