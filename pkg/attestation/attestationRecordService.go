package attestation

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/node"
)

// AttestationRecord represents an attestation record with verified signatures
type AttestationRecord = constants.AttestationRecord

// Version represents a version with signature information
type Version = constants.Version

// Signature represents a cryptographic signature
type Signature = constants.Signature

// CreateAttestationRecord creates an attestation record after verifying signatures
func CreateAttestationRecord(ctx context.Context, verifier SignatureVerifier, docId string, sourceDocId string, docType string, versions []Version) (*AttestationRecord, error) {
	attestationRecord := &constants.AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		CIDs:          []string{},
		DocType:       docType,
		VoteCount:     1,
	}
	for _, version := range versions {
		// Validate the signature against the CID
		if err := verifier.Verify(ctx, version.CID, version.Signature); err != nil {
			// Signature verification failed - this is expected during P2P sync when
			// blocks may not be fully synced yet. Silently skip failed verifications.
			continue
		}
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
	}

	return attestationRecord, nil
}

// PostAttestationRecord posts the attestation record to DefraDB
func PostAttestationRecord(ctx context.Context, defraNode *node.Node, record *AttestationRecord) error {
	// Format CIDs for GraphQL
	cidsArray := make([]string, len(record.CIDs))
	for i, cid := range record.CIDs {
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
			}
		}
	`, constants.CollectionAttestationRecord, record.AttestedDocId, record.SourceDocId, cidsString, record.DocType, record.VoteCount, cidsString, record.VoteCount, record.AttestedDocId)

	_, err := defra.PostMutation[constants.AttestationRecord](ctx, defraNode, mutation)
	if err != nil {
		return fmt.Errorf("error posting attestation record mutation: %v", err)
	}

	return nil
}

// AddAttestationRecordCollection creates and subscribes to attestation record collection
func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	collectionSDL := getAttestationRecordSDL(associatedViewName)
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(collectionSDL)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	attestationRecords := "AttestationRecord"
	err = defraNode.DB.AddP2PCollections(ctx, attestationRecords)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", attestationRecords, err)
	}
	return nil
}

// GetAttestationRecords queries attestation records by document type
func GetAttestationRecords(ctx context.Context, defraNode *node.Node, docType string, viewDocIds []string) ([]AttestationRecord, error) {
	if len(viewDocIds) > 0 {
		// Build a comma-separated list of quoted doc IDs for GraphQL _in filter
		quoted := make([]string, 0, len(viewDocIds))
		for _, id := range viewDocIds {
			quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
		}
		inList := strings.Join(quoted, ", ")

		query := fmt.Sprintf(`
			query {
				%s(filter: {doc_type: {_eq: "%s"}, attested_doc: {_in: [%s]}}) {
					_docID
					attested_doc
					source_doc
					CIDs
					doc_type
					vote_count
				}
			}
		`, constants.CollectionAttestationRecord, docType, inList)

		return defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	} else {
		// Query all records for this doc type
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
}

// HandleDocumentAttestation is the main handler for processing document attestations
func HandleDocumentAttestation(ctx context.Context, verifier SignatureVerifier, defraNode *node.Node, docID string, docType string, versions []Version) error {

	if len(versions) == 0 {
		return nil
	}

	// Create attestation record with signature verification
	attestationRecord, err := CreateAttestationRecord(ctx, verifier, docID, docID, docType, versions)
	if err != nil {
		return fmt.Errorf("failed to create attestation record for document %s: %w", docID, err)
	}

	if len(attestationRecord.CIDs) == 0 {
		return nil
	}

	// Post the attestation record to DefraDB
	err = PostAttestationRecord(ctx, defraNode, attestationRecord)
	if err != nil {
		return fmt.Errorf("failed to post attestation record for document %s: %w", docID, err)
	}

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
func ExtractVersionsFromDocument(docData map[string]interface{}) ([]Version, error) {
	// Try to extract version information from the document data
	// The document data should contain the version field with signatures

	if versionData, exists := docData["_version"]; exists {
		// Convert the version data to the expected format
		if versionArray, ok := versionData.([]interface{}); ok {
			versions := make([]Version, 0, len(versionArray))

			for _, v := range versionArray {
				if versionMap, ok := v.(map[string]interface{}); ok {
					version := Version{}

					// Extract CID
					if cid, ok := versionMap["cid"].(string); ok {
						version.CID = cid
					}

					// Extract Signature
					if sigData, ok := versionMap["signature"].(map[string]interface{}); ok {
						signature := Signature{}

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

					// Extract CollectionVersionId
					if collectionVersionId, ok := versionMap["collectionVersionId"].(string); ok {
						version.CollectionVersionId = collectionVersionId
					}

					versions = append(versions, version)
				}
			}

			return versions, nil
		}
	}

	// If no version data found, return empty slice (not an error)
	return []Version{}, nil
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

func getAttestationRecordSDL(viewName string) string {
	// Check if this is a primitive type (Block, Transaction, Log, AccessListEntry)
	primitiveTypes := []string{"Block", "Transaction", "Log", "AccessListEntry"}
	if slices.Contains(primitiveTypes, viewName) { // For our primitive attestation records, we use a condensed schema
		return `type AttestationRecord { 
				attested_doc: String
				CIDs: [String]
			}`
	}

	// If either AttestationRecord does not have unique name, we will get an error when trying to the schema (collection already exists error)
	// We want a separate collection of AttestationRecords for each View so that app clients don't receive all AttestationRecords, only those that are relevant to the collections/Views they care about - we can just append the View names as those must also be unique
	return `type AttestationRecord {
		attested_doc: String
		source_doc: String
		CIDs: [String]
	}`
}

// ========================================
// UTILITY FUNCTIONS
// ========================================

// GetAttestationRecordsByViewName queries attestation records for a specific view
func GetAttestationRecordsByViewName(ctx context.Context, defraNode *node.Node, viewName string, viewDocIds []string) ([]AttestationRecord, error) {
	collectionName := fmt.Sprintf("Ethereum__Mainnet__AttestationRecord_%s", viewName)

	if len(viewDocIds) > 0 {
		// Build a comma-separated list of quoted doc IDs for GraphQL _in filter
		quoted := make([]string, 0, len(viewDocIds))
		for _, id := range viewDocIds {
			quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
		}
		inList := strings.Join(quoted, ", ")

		query := fmt.Sprintf(`
			query {
				%s(filter: {attested_doc: {_in: [%s]}}) {
					_docID
					attested_doc
					source_doc
					CIDs
				}
			}
		`, collectionName, inList)

		return defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	} else {
		// Query all records for this view
		query := fmt.Sprintf(`
			query {
				%s {
					_docID
					attested_doc
					source_doc
					CIDs
				}
			}
		`, collectionName)

		return defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	}
}
