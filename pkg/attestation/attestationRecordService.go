package attestation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
)

// Record represents an attestation record with verified signatures.
type Record = constants.AttestationRecord

// PostAttestationRecord posts the attestation record to DefraDB using read-modify-write
// to correctly merge source_doc lists when multiple indexers attest to the same block.
// It returns an error wrapping ErrDocumentNotFound when the existing record is deleted between
// the read and the write, which the pruner does to records at or below its cutoff.
func PostAttestationRecord(ctx context.Context, defraNode *node.Node, record *Record) error {
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
	if err != nil {
		return fmt.Errorf("failed to get attestation collection: %w", err)
	}

	existingDoc, err := lookupExistingAttestation(ctx, defraNode, col, record.AttestedDocID)
	if errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		return fmt.Errorf("attestation %s: %w", record.AttestedDocID, ErrDocumentNotFound)
	}
	if err != nil {
		return fmt.Errorf("failed to lookup existing attestation: %w", err)
	}

	if existingDoc != nil {
		return updateAttestationRecord(ctx, col, existingDoc, record)
	}

	// Create new document
	sourceDocsAny := make([]any, len(record.SourceDocIDs))
	for i, s := range record.SourceDocIDs {
		sourceDocsAny[i] = s
	}
	cidsAny := make([]any, len(record.CIDs))
	for i, c := range record.CIDs {
		cidsAny[i] = c
	}

	data := map[string]any{
		"attested_doc": record.AttestedDocID,
		"source_doc":   sourceDocsAny,
		"CIDs":         cidsAny,
		"doc_type":     record.DocType,
		"vote_count":   record.VoteCount,
	}
	if record.BlockNumber != nil {
		data["blockNumber"] = *record.BlockNumber
	}

	doc, err := client.NewDocFromMap(ctx, data, col.Version())
	if err != nil {
		return fmt.Errorf("failed to create attestation document: %w", err)
	}

	return col.SaveDocument(ctx, doc)
}

// mergeStringListField reads an existing string list field from a document and merges
// it with newValues, returning a deduplicated list.
func mergeStringListField(doc *client.Document, fieldName string, newValues []string) []string {
	set := make(map[string]struct{})
	var result []string

	if val, err := doc.Get(fieldName); err == nil && val != nil {
		switch v := val.(type) {
		case []immutable.Option[string]:
			for _, opt := range v {
				if opt.HasValue() {
					s := opt.Value()
					if _, exists := set[s]; !exists {
						set[s] = struct{}{}
						result = append(result, s)
					}
				}
			}
		case []string:
			for _, s := range v {
				if _, exists := set[s]; !exists {
					set[s] = struct{}{}
					result = append(result, s)
				}
			}
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					if _, exists := set[s]; !exists {
						set[s] = struct{}{}
						result = append(result, s)
					}
				}
			}
		}
	}

	for _, s := range newValues {
		if _, exists := set[s]; !exists {
			set[s] = struct{}{}
			result = append(result, s)
		}
	}

	return result
}

// lookupExistingAttestation queries DefraDB for an existing attestation record
// matching the given attestedDocID and returns the Document if found.
// Returns (nil, nil) when no matching document exists.
func lookupExistingAttestation(ctx context.Context, defraNode *node.Node, col client.Collection, attestedDocID string) (*client.Document, error) {
	query := fmt.Sprintf(`query {
		%s(filter: {attested_doc: {_eq: "%s"}}) {
			_docID
		}
	}`, constants.CollectionAttestationRecord, attestedDocID)

	result := defraNode.DB.ExecRequest(ctx, query)

	docIDStr := extractDocIDFromResult(result.GQL.Data, constants.CollectionAttestationRecord)
	if docIDStr == "" {
		return nil, nil // nolint:nilnil
	}

	docID, err := client.NewDocIDFromString(docIDStr)
	if err != nil {
		return nil, err
	}

	return col.GetDocument(ctx, docID)
}

// extractDocIDFromResult extracts the first _docID string from a GQL query result.
// Returns "" if no document is found or the result structure is unexpected.
func extractDocIDFromResult(data any, collectionName string) string {
	dataMap, ok := data.(map[string]any)
	if !ok {
		return ""
	}

	raw := dataMap[collectionName]
	switch list := raw.(type) {
	case []any:
		if len(list) == 0 {
			return ""
		}
		firstDoc, ok := list[0].(map[string]any)
		if !ok {
			return ""
		}
		docIDStr, _ := firstDoc["_docID"].(string)
		return docIDStr
	case []map[string]any:
		if len(list) == 0 {
			return ""
		}
		docIDStr, _ := list[0]["_docID"].(string)
		return docIDStr
	default:
		return ""
	}
}

// updateAttestationRecord merges record into the existing document and writes it back, returning
// ErrDocumentNotFound if the document no longer exists as the update starts. SaveDocument would
// create it again holding only the fields set here, a record neither the lookup nor the pruner
// could find.
func updateAttestationRecord(ctx context.Context, col client.Collection, existingDoc *client.Document, record *Record) error {
	// Merge source_doc identities
	mergedSources := mergeStringListField(existingDoc, "source_doc", record.SourceDocIDs)
	sourcesAny := make([]any, len(mergedSources))
	for i, s := range mergedSources {
		sourcesAny[i] = s
	}
	if err := existingDoc.Set(ctx, "source_doc", sourcesAny); err != nil {
		return fmt.Errorf("failed to set source_doc: %w", err)
	}

	// Merge CIDs
	mergedCIDs := mergeStringListField(existingDoc, "CIDs", record.CIDs)
	cidsAny := make([]any, len(mergedCIDs))
	for i, c := range mergedCIDs {
		cidsAny[i] = c
	}
	if err := existingDoc.Set(ctx, "CIDs", cidsAny); err != nil {
		return fmt.Errorf("failed to set CIDs: %w", err)
	}

	if err := existingDoc.Set(ctx, "vote_count", record.VoteCount); err != nil {
		return fmt.Errorf("failed to set vote_count: %w", err)
	}

	err := col.UpdateDocument(ctx, existingDoc)
	if errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		return fmt.Errorf("attestation %s: %w", record.AttestedDocID, ErrDocumentNotFound)
	}
	return err
}

// CheckExistingAttestation checks if an attestation already exists for a document.
func CheckExistingAttestation(ctx context.Context, defraNode *node.Node, docID string, docType string) ([]Record, error) {
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

	existing, err := defradb.QueryArray[Record](ctx, defraNode, query)
	if err != nil {
		if strings.Contains(err.Error(), "No attestation records found") {
			return nil, nil // No existing attestation, not an error
		}
		return nil, fmt.Errorf("failed to check existing attestation for document %s: %w", docID, err)
	}

	return existing, nil
}

// ========================================
// ATTESTATION RECORD MERGING
// ========================================

// MergeAttestationRecords merges two attestation records with the same attested document
// This is useful when multiple sources attest to the same document and you want to combine their CIDs.
func MergeAttestationRecords(record1, record2 *Record) (*Record, error) {
	if record1.AttestedDocID != record2.AttestedDocID {
		return nil, fmt.Errorf("%s vs %s: %w", record1.AttestedDocID, record2.AttestedDocID, ErrDifferentAttestedDocIDs)
	}

	// Merge source doc identities, avoiding duplicates
	sourceSet := make(map[string]bool)
	var mergedSources []string
	for _, s := range record1.SourceDocIDs {
		if !sourceSet[s] {
			mergedSources = append(mergedSources, s)
			sourceSet[s] = true
		}
	}
	for _, s := range record2.SourceDocIDs {
		if !sourceSet[s] {
			mergedSources = append(mergedSources, s)
			sourceSet[s] = true
		}
	}

	// Create merged record
	merged := &Record{
		AttestedDocID: record1.AttestedDocID,
		SourceDocIDs:  mergedSources,
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
// BLOCK ATTESTATION VERIFICATION
// ========================================

// IsDocumentAttestedViaBlock checks if a document's CID is attested via a block-level attestation.
// This is used when block signatures are enabled - individual documents inherit attestation
// from the block they belong to. Returns true if the CID is found in any block attestation.
func IsDocumentAttestedViaBlock(ctx context.Context, defraNode *node.Node, blockNumber int64, documentCID string) (bool, error) {
	blockAttestedID := fmt.Sprintf("block:%d", blockNumber)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "%s"}, doc_type: {_eq: "Block"}}) {
				_docID
				attested_doc
				CIDs
			}
		}
	`, constants.CollectionAttestationRecord, blockAttestedID)

	records, err := defradb.QueryArray[Record](ctx, defraNode, query)
	if err != nil {
		if strings.Contains(err.Error(), "No attestation records found") {
			return false, nil
		}
		return false, fmt.Errorf("failed to query block attestation for block %d: %w", blockNumber, err)
	}

	if len(records) == 0 {
		return false, nil
	}

	for _, record := range records {
		if slices.Contains(record.CIDs, documentCID) {
			return true, nil
		}
	}

	return false, nil
}

// GetBlockAttestations retrieves all attestation records for a specific block height.
// With multiple indexers, different merkle roots produce separate attestation records.
func GetBlockAttestations(ctx context.Context, defraNode *node.Node, blockNumber int64) ([]Record, error) {
	blockPrefix := fmt.Sprintf("block:%d:", blockNumber)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_like: "%s%%"}, doc_type: {_eq: "Block"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
				doc_type
				vote_count
			}
		}
	`, constants.CollectionAttestationRecord, blockPrefix)

	records, err := defradb.QueryArray[Record](ctx, defraNode, query)
	if err != nil {
		if strings.Contains(err.Error(), "No attestation records found") {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query block attestations for block %d: %w", blockNumber, err)
	}

	return records, nil
}
