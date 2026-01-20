package attestation

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
)

// AttestationRecord represents an attestation record with verified signatures
type AttestationRecord = constants.AttestationRecord

// Version represents a version with signature information
type Version = constants.Version

// Signature represents a cryptographic signature
type Signature = constants.Signature

// CreateAttestationRecord creates an attestation record after verifying signatures
func CreateAttestationRecord(ctx context.Context, verifier SignatureVerifier, docId string, sourceDocId string, docType string, versions []Version, maxConcurrentVerifications int) (*AttestationRecord, error) {
	if len(versions) == 0 {
		return &constants.AttestationRecord{
			AttestedDocId: docId,
			SourceDocId:   sourceDocId,
			CIDs:          []string{},
			DocType:       docType,
			VoteCount:     1,
		}, nil
	}

	if maxConcurrentVerifications <= 0 {
		maxConcurrentVerifications = 50
	}
	sem := make(chan struct{}, maxConcurrentVerifications)
	verifiedCIDs := make(chan string, len(versions))
	var wg sync.WaitGroup

	for _, version := range versions {
		wg.Add(1)
		go func(v Version) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := verifier.Verify(ctx, v.CID, v.Signature); err != nil {
				return
			}
			verifiedCIDs <- v.CID
		}(version)
	}

	go func() {
		wg.Wait()
		close(verifiedCIDs)
	}()

	cids := make([]string, 0, len(versions))
	for cid := range verifiedCIDs {
		cids = append(cids, cid)
	}

	return &constants.AttestationRecord{
		AttestedDocId: docId,
		SourceDocId:   sourceDocId,
		CIDs:          cids,
		DocType:       docType,
		VoteCount:     1,
	}, nil
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

// PostAttestationRecordsBatch posts multiple attestation records.
func PostAttestationRecordsBatch(ctx context.Context, defraNode *node.Node, records []*AttestationRecord) error {
	if len(records) == 0 {
		return nil
	}

	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
	if err != nil {
		return fmt.Errorf("failed to get attestation collection: %w", err)
	}

	attestedDocIDs := make([]string, 0, len(records))
	for _, record := range records {
		if record == nil || len(record.CIDs) == 0 {
			continue
		}
		attestedDocIDs = append(attestedDocIDs, record.AttestedDocId)
	}

	if len(attestedDocIDs) == 0 {
		return nil
	}

	existingByAttestedDoc := make(map[string]*client.Document)
	for _, attestedDocID := range attestedDocIDs {
		query := fmt.Sprintf(`query {
			%s(filter: {attested_doc: {_eq: "%s"}}) {
				_docID
			}
		}`, constants.CollectionAttestationRecord, attestedDocID)

		result := defraNode.DB.ExecRequest(ctx, query)
		if len(result.GQL.Errors) > 0 {
			continue
		}

		dataMap, ok := result.GQL.Data.(map[string]any)
		if !ok {
			continue
		}

		var docIDStr string
		switch collectionData := dataMap[constants.CollectionAttestationRecord].(type) {
		case []any:
			if len(collectionData) == 0 {
				continue
			}
			firstDoc, ok := collectionData[0].(map[string]any)
			if !ok {
				continue
			}
			docIDStr, ok = firstDoc["_docID"].(string)
			if !ok {
				continue
			}
		case []map[string]any:
			if len(collectionData) == 0 {
				continue
			}
			docIDStr, ok = collectionData[0]["_docID"].(string)
			if !ok {
				continue
			}
		default:
			continue
		}

		if docIDStr == "" {
			continue
		}

		docID, err := client.NewDocIDFromString(docIDStr)
		if err != nil {
			continue
		}

		existingDoc, err := col.Get(ctx, docID, false)
		if err != nil {
			continue
		}
		existingByAttestedDoc[attestedDocID] = existingDoc
	}

	docs := make([]*client.Document, 0, len(records))

	for _, record := range records {
		if record == nil || len(record.CIDs) == 0 {
			continue
		}

		existingDoc, exists := existingByAttestedDoc[record.AttestedDocId]
		if exists {
			cidSet := make(map[string]struct{})
			existingCIDsVal, err := existingDoc.Get("CIDs")
			if err == nil && existingCIDsVal != nil {
				switch existingCIDs := existingCIDsVal.(type) {
				case []immutable.Option[string]:
					for _, opt := range existingCIDs {
						if opt.HasValue() {
							cidSet[opt.Value()] = struct{}{}
						}
					}
				case []any:
					for _, c := range existingCIDs {
						if cidStr, ok := c.(string); ok {
							cidSet[cidStr] = struct{}{}
						}
					}
				case []string:
					for _, cidStr := range existingCIDs {
						cidSet[cidStr] = struct{}{}
					}
				}
			}
			for _, newCID := range record.CIDs {
				cidSet[newCID] = struct{}{}
			}
			mergedCIDs := make([]any, 0, len(cidSet))
			for cid := range cidSet {
				mergedCIDs = append(mergedCIDs, cid)
			}

			if err := existingDoc.Set(ctx, "CIDs", mergedCIDs); err != nil {
				logger.Sugar.Warnf("Failed to set CIDs for attestation %s: %v", record.AttestedDocId, err)
				continue
			}
			if err := existingDoc.Set(ctx, "vote_count", record.VoteCount); err != nil {
				logger.Sugar.Warnf("Failed to set vote_count for attestation %s: %v", record.AttestedDocId, err)
			}
			docs = append(docs, existingDoc)
		} else {
			cidsAny := make([]any, len(record.CIDs))
			for i, cid := range record.CIDs {
				cidsAny[i] = cid
			}

			data := map[string]any{
				"attested_doc": record.AttestedDocId,
				"source_doc":   record.SourceDocId,
				"CIDs":         cidsAny,
				"doc_type":     record.DocType,
				"vote_count":   record.VoteCount,
			}

			doc, err := client.NewDocFromMap(ctx, data, col.Version())
			if err != nil {
				logger.Sugar.Warnf("Failed to create document for attestation %s: %v", record.AttestedDocId, err)
				continue
			}
			docs = append(docs, doc)
		}
	}

	if len(docs) == 0 {
		return nil
	}

	err = col.SaveMany(ctx, docs)
	if err != nil {
		return fmt.Errorf("failed to save attestation records: %w", err)
	}

	return nil
}

// HandleDocumentAttestation is the main handler for processing document attestations
func HandleDocumentAttestation(ctx context.Context, verifier SignatureVerifier, defraNode *node.Node, docID string, docType string, versions []Version, maxConcurrentVerifications int) error {

	if len(versions) == 0 {
		return nil
	}

	// Create attestation record with signature verification
	attestationRecord, err := CreateAttestationRecord(ctx, verifier, docID, docID, docType, versions, maxConcurrentVerifications)
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

// DocumentAttestationInput represents input for batch attestation processing
type DocumentAttestationInput struct {
	DocID    string
	DocType  string
	Versions []Version
}

// HandleDocumentAttestationBatch processes multiple document attestations in a single batch.
func HandleDocumentAttestationBatch(ctx context.Context, verifier SignatureVerifier, defraNode *node.Node, inputs []DocumentAttestationInput, maxConcurrentVerifications int) error {
	if len(inputs) == 0 {
		return nil
	}

	records := make([]*AttestationRecord, 0, len(inputs))
	for _, input := range inputs {
		if len(input.Versions) == 0 {
			continue
		}

		record, err := CreateAttestationRecord(ctx, verifier, input.DocID, input.DocID, input.DocType, input.Versions, maxConcurrentVerifications)
		if err != nil {
			continue
		}

		if len(record.CIDs) > 0 {
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		return nil
	}

	return PostAttestationRecordsBatch(ctx, defraNode, records)
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
func ExtractVersionsFromDocument(docData map[string]any) ([]Version, error) {
	// Try to extract version information from the document data
	// The document data should contain the version field with signatures

	if versionData, exists := docData["_version"]; exists {
		// Convert the version data to the expected format
		if versionArray, ok := versionData.([]any); ok {
			versions := make([]Version, 0, len(versionArray))

			for _, v := range versionArray {
				if versionMap, ok := v.(map[string]any); ok {
					version := Version{}

					// Extract CID
					if cid, ok := versionMap["cid"].(string); ok {
						version.CID = cid
					}

					// Extract Signature
					if sigData, ok := versionMap["signature"].(map[string]any); ok {
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
