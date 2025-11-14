package attestation

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord attestation.AttestationRecord

func CreateAttestationRecord(ctx context.Context, verifier SignatureVerifier, docId string, sourceDocId string, versions []attestation.Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		CIDs:          []string{},
	}
	for _, version := range versions {
		// Validate the signature against the CID
		if err := verifier.Verify(ctx, version.CID, version.Signature); err != nil {
			// Todo here we might want to send a message to ShinzoHub (or similar) indicating that we received an invalid signature
			if logger.Sugar != nil {
				logger.Sugar.Errorf("Invalid signature for CID %s from identity %s: %w", version.CID, version.Signature.Identity, err)
			}
			continue
		}
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
	}

	return attestationRecord, nil
}

func (record *AttestationRecord) PostAttestationRecord(ctx context.Context, defraNode *node.Node, viewName string) error {
	cidsArray := make([]string, len(record.CIDs))
	for i, cid := range record.CIDs {
		cidsArray[i] = fmt.Sprintf(`"%s"`, cid)
	}
	cidsString := fmt.Sprintf("[%s]", strings.Join(cidsArray, ", "))

	attestationCollectionName := fmt.Sprintf("AttestationRecord_%s", viewName)
	mutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {
				attested_doc: "%s",
				source_doc: "%s",
				CIDs: %s
			}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, attestationCollectionName, record.AttestedDocId, record.SourceDocId, cidsString)

	_, err := defra.PostMutation[AttestationRecord](ctx, defraNode, mutation)
	if err != nil {
		return fmt.Errorf("error posting attestation record mutation: %v", err)
	}

	return nil
}
