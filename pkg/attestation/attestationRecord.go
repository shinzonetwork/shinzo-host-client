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
		CIDs:          []string{},
	}
	for _, version := range versions {
		// TODO here we need to validate the signatures first
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
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
		"CIDs":         record.CIDs,
	}, attestationCollection.Version())
	if err != nil {
		return fmt.Errorf("error creating attestation document: %v", err)
	}

	// Save attestation record
	err = attestationCollection.Save(ctx, attestationDoc)
	if err != nil {
		return fmt.Errorf("error saving attestation record: %v", err)
	}

	return nil
}
