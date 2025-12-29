package attestation

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/node"
)

type AttestationRecord struct {
	AttestedDocId string   `json:"attested_doc"`
	SourceDocId   string   `json:"source_doc"`
	CIDs          []string `json:"CIDs"`
	DocType       string   `json:"doc_type"`
	VoteCount     int      `json:"vote_count"`
}

func CreateAttestationRecord(ctx context.Context, verifier SignatureVerifier, docId string, sourceDocId string, docType string, versions []attestation.Version) (*AttestationRecord, error) {
	attestationRecord := &AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		CIDs:          []string{},
		DocType:       docType,
		VoteCount:     1,
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

func (record *AttestationRecord) PostAttestationRecord(ctx context.Context, defraNode *node.Node) error {
	// First, check if an attestation record already exists for this attested_doc
	existing, err := CheckExistingAttestation(ctx, defraNode, record.AttestedDocId, record.DocType)
	if err != nil {
		return fmt.Errorf("failed to check existing attestation: %w", err)
	}

	var mergedCIDs []string
	if len(existing) > 0 {
		// Merge existing CIDs with new CIDs
		existingRecord := existing[0]
		mergedCIDs = append(existingRecord.CIDs, record.CIDs...)
	} else {
		// No existing record, use new CIDs
		mergedCIDs = record.CIDs
	}

	// Format CIDs for GraphQL
	cidsArray := make([]string, len(mergedCIDs))
	for i, cid := range mergedCIDs {
		cidsArray[i] = fmt.Sprintf(`"%s"`, cid)
	}
	cidsString := fmt.Sprintf("[%s]", strings.Join(cidsArray, ", "))

	// Use upsert to merge P-counter vote_count and CIDs for same attested_doc
	mutation := fmt.Sprintf(`
		mutation {
			upsert_%v(
				create: {
					attested_doc: "%s",
					source_doc: "%s",
					CIDs: %s,
					doc_type: "%s",
					vote_count: %d
				},
				update: {
					CIDs: %s,
					vote_count: %d
				},
				filter: {attested_doc: {_eq: "%s"}}
			) {
				_docID
				attested_doc
				source_doc
				CIDs
				doc_type
				vote_count
			}
		}
	`, constants.CollectionAttestationRecord, record.AttestedDocId, record.SourceDocId, cidsString, record.DocType, record.VoteCount, cidsString, record.VoteCount, record.AttestedDocId)

	_, err = defra.PostMutation[AttestationRecord](ctx, defraNode, mutation)
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

// GetAttestationRecords queries attestation records by document type
func GetAttestationRecords(ctx context.Context, defraNode *node.Node, docType string, viewDocIds []string) ([]AttestationRecord, error) {
	query := fmt.Sprintf(`
		query {
			%s(filter: {doc_type: {_eq: "%s"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
				doc_type
				vote_count
			}
		}
	`, constants.CollectionAttestationRecord, docType)

	return defra.QueryArray[AttestationRecord](ctx, defraNode, query)
}

// Document Handler Functions - The main attestation logic

// HandleDocumentAttestation is the main handler for processing document attestations
func HandleDocumentAttestation(ctx context.Context, defraNode *node.Node, docID string, docType string, versions []attestation.Version) error {
	// logger.Sugar.Debugf("🔐 Processing attestation for document %s (type: %s)", docID, docType)

	if len(versions) == 0 {
		// logger.Sugar.Debugf("📊 No signatures found for document %s, skipping attestation", docID)
		return nil
	}

	// logger.Sugar.Debugf("📝 Found %d signatures for document %s:", len(versions), docID)
	// for i, version := range versions {
	// 	logger.Sugar.Debugf("  Signature %d: CID=%s, Identity=%s, Type=%s", i+1, version.CID, version.Signature.Identity, version.Signature.Type)
	// }

	// Create attestation record with signature verification
	verifier := NewDefraSignatureVerifier(defraNode)
	attestationRecord, err := CreateAttestationRecord(ctx, verifier, docID, docID, docType, versions)
	if err != nil {
		return fmt.Errorf("failed to create attestation record for document %s: %w", docID, err)
	}

	// AttestationRecord schema is now defined in schema.graphql and loaded at startup

	// Post the attestation record to DefraDB
	// logger.Sugar.Debugf("💾 Posting attestation record to collection: AttestationRecord")
	err = attestationRecord.PostAttestationRecord(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("failed to post attestation record for document %s: %w", docID, err)
	}

	// logger.Sugar.Debugf("✅ Successfully created attestation record:")
	// logger.Sugar.Debugf("   📄 Document ID: %s", docID)
	// logger.Sugar.Debugf("   📂 Document Type: %s", docType)
	// logger.Sugar.Debugf("   🗂️  Collection: AttestationRecord")
	// logger.Sugar.Debugf("   🔗 Attested Doc: %s", attestationRecord.AttestedDocId)
	// logger.Sugar.Debugf("   📋 Source Doc: %s", attestationRecord.SourceDocId)
	// logger.Sugar.Debugf("   ✅ Verified CIDs: %v", attestationRecord.CIDs)
	// logger.Sugar.Debugf("   🔢 Total Signatures: %d", len(attestationRecord.CIDs))
	// logger.Sugar.Debugf("   �️  Vote Count: %d", attestationRecord.VoteCount)

	return nil
}

// CheckExistingAttestation checks if an attestation already exists for a document
func CheckExistingAttestation(ctx context.Context, defraNode *node.Node, docID string, docType string) ([]AttestationRecord, error) {
	// Query the general attestation collection for this specific document
	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "%s"}, doc_type: {_eq: "%s"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
				doc_type
				vote_count
			}
		}
	`, constants.CollectionAttestationRecord, docID, docType)

	existing, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
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

// ========================================
// ATTESTATION RECORD MERGING
// ========================================

// MergeAttestationRecords merges two attestation records with the same attested document
// This is useful when multiple sources attest to the same document and you want to combine their CIDs
func MergeAttestationRecords(record1, record2 *AttestationRecord) (*AttestationRecord, error) {
	if record1.AttestedDocId != record2.AttestedDocId {
		return nil, fmt.Errorf("cannot merge records with different attested document IDs: %s vs %s", record1.AttestedDocId, record2.AttestedDocId)
	}

	// Create merged record
	merged := &AttestationRecord{
		AttestedDocId: record1.AttestedDocId,
		SourceDocId:   record1.SourceDocId, // Use first record's source doc ID
		CIDs:          make([]string, 0),
	}

	// Merge CIDs, avoiding duplicates
	cidSet := make(map[string]bool)

	// Add CIDs from first record
	for _, cid := range record1.CIDs {
		if !cidSet[cid] {
			merged.CIDs = append(merged.CIDs, cid)
			cidSet[cid] = true
		}
	}

	// Add CIDs from second record
	for _, cid := range record2.CIDs {
		if !cidSet[cid] {
			merged.CIDs = append(merged.CIDs, cid)
			cidSet[cid] = true
		}
	}

	return merged, nil
}
