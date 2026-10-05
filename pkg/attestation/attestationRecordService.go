package attestation

import (
	"context"
	"errors"
	"fmt"
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
