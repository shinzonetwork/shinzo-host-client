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

// Wrapper functions for app-sdk attestation functionality

// AddAttestationRecordCollection wraps the app-sdk function
func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	return attestation.AddAttestationRecordCollection(ctx, defraNode, associatedViewName)
}

// GetAttestationRecords wraps the app-sdk function and converts types
func GetAttestationRecords(ctx context.Context, defraNode *node.Node, associatedViewName string, viewDocIds []string) ([]AttestationRecord, error) {
	// Use app-sdk function
	sdkRecords, err := attestation.GetAttestationRecords(ctx, defraNode, associatedViewName, viewDocIds)
	if err != nil {
		return nil, err
	}

	// Convert from app-sdk type to host type
	hostRecords := make([]AttestationRecord, len(sdkRecords))
	for i, record := range sdkRecords {
		hostRecords[i] = AttestationRecord(record)
	}

	return hostRecords, nil
}

// Document Handler Functions - The main attestation logic

// HandleDocumentAttestation is the main handler for processing document attestations
func HandleDocumentAttestation(ctx context.Context, defraNode *node.Node, docID string, docType string, versions []attestation.Version) error {
	logger.Sugar.Infof("🔐 Processing attestation for document %s (type: %s)", docID, docType)

	if len(versions) == 0 {
		logger.Sugar.Infof("⚠️  No signatures found for document %s, skipping attestation", docID)
		return nil
	}

	logger.Sugar.Infof("📝 Found %d signatures for document %s:", len(versions), docID)
	for i, version := range versions {
		logger.Sugar.Infof("  Signature %d: CID=%s, Identity=%s, Type=%s",
			i+1, version.CID, version.Signature.Identity, version.Signature.Type)
	}

	// Create attestation record with signature verification
	verifier := NewDefraSignatureVerifier(defraNode)
	attestationRecord, err := CreateAttestationRecord(ctx, verifier, docID, docID, versions)
	if err != nil {
		return fmt.Errorf("failed to create attestation record for document %s: %w", docID, err)
	}

	// For document attestations, we use a generic view name since this is document-level attestation
	attestationViewName := fmt.Sprintf("Document_%s", docType)

	// Ensure the attestation collection exists for this document type (using app-sdk wrapper)
	err = AddAttestationRecordCollection(ctx, defraNode, attestationViewName)
	if err != nil && !strings.Contains(err.Error(), "collection already exists") {
		return fmt.Errorf("failed to add attestation collection for %s: %w", attestationViewName, err)
	}

	// Post the attestation record to DefraDB
	logger.Sugar.Infof("💾 Posting attestation record to collection: AttestationRecord_%s", attestationViewName)
	err = attestationRecord.PostAttestationRecord(ctx, defraNode, attestationViewName)
	if err != nil {
		return fmt.Errorf("failed to post attestation record for document %s: %w", docID, err)
	}

	logger.Sugar.Infof("✅ Successfully created attestation record:")
	logger.Sugar.Infof("   📄 Document ID: %s", docID)
	logger.Sugar.Infof("   📂 Document Type: %s", docType)
	logger.Sugar.Infof("   🗂️  Collection: AttestationRecord_%s", attestationViewName)
	logger.Sugar.Infof("   🔗 Attested Doc: %s", attestationRecord.AttestedDocId)
	logger.Sugar.Infof("   📋 Source Doc: %s", attestationRecord.SourceDocId)
	logger.Sugar.Infof("   ✅ Verified CIDs: %v", attestationRecord.CIDs)
	logger.Sugar.Infof("   🔢 Total Signatures: %d", len(attestationRecord.CIDs))
	logger.Sugar.Infof("   🔍 Query with: { AttestationRecord_%s(filter: {attested_doc: {_eq: \"%s\"}}) { attested_doc source_doc CIDs _docID } }", attestationViewName, docID)

	return nil
}

// CheckExistingAttestation checks if an attestation already exists for a document
func CheckExistingAttestation(ctx context.Context, defraNode *node.Node, docID string, docType string) ([]AttestationRecord, error) {
	attestationViewName := fmt.Sprintf("Document_%s", docType)

	// Use app-sdk wrapper function
	existing, err := GetAttestationRecords(ctx, defraNode, attestationViewName, []string{docID})
	if err != nil {
		if strings.Contains(err.Error(), "No attestation records found") {
			return nil, nil // No existing attestation, not an error
		}
		return nil, fmt.Errorf("failed to check existing attestation for document %s: %w", docID, err)
	}

	return existing, nil
}

// ExtractVersionsFromDocument extracts version/signature information from a document based on its type
func ExtractVersionsFromDocument(docData map[string]interface{}) ([]attestation.Version, error) {
	// Try to extract version information from the document data
	// The document data should contain the version field with signatures

	if versionData, exists := docData["_version"]; exists {
		// Convert the version data to the expected format
		if versionArray, ok := versionData.([]interface{}); ok {
			versions := make([]attestation.Version, 0, len(versionArray))

			for _, v := range versionArray {
				if versionMap, ok := v.(map[string]interface{}); ok {
					version := attestation.Version{}

					// Extract CID
					if cid, ok := versionMap["cid"].(string); ok {
						version.CID = cid
					}

					// Extract Height
					if height, ok := versionMap["height"].(float64); ok {
						version.Height = uint(height)
					}

					// Extract Signature
					if sigData, ok := versionMap["signature"].(map[string]interface{}); ok {
						signature := attestation.Signature{}

						if sigType, ok := sigData["type"].(string); ok {
							signature.Type = sigType
						}
						if identity, ok := sigData["identity"].(string); ok {
							signature.Identity = identity
						}
						if value, ok := sigData["value"].(string); ok {
							signature.Value = value
						}

						version.Signature = signature
					}

					versions = append(versions, version)
				}
			}

			return versions, nil
		}
	}

	// If no version data found, return empty slice (not an error)
	return []attestation.Version{}, nil
}
