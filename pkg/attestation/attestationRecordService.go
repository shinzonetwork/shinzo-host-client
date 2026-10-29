package attestation

import (
	"context"
	"errors"
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
)

// Record represents an attestation record with verified signatures.
type Record = constants.AttestationRecord

// PostAttestationRecord writes record to the attestation collection named collection, merging it
// into the record the collection already holds for the same attested document. A merge adds to
// vote_count only the sources the record did not hold. It reports whether it created the record;
// false means it merged record into one the collection already held.
// The read and the write run in one transaction, so if another writer changes or deletes the
// record in between, the commit fails with a transaction conflict for the caller to retry.
func PostAttestationRecord(ctx context.Context, defraNode *node.Node, collection string, record *Record) (bool, error) {
	txn, err := defraNode.DB.NewTxn(false)
	if err != nil {
		return false, fmt.Errorf("failed to start attestation transaction: %w", err)
	}
	defer txn.Discard()

	created, err := postAttestationRecordIn(ctx, txn, collection, record)
	if err != nil {
		return false, err
	}
	if err := txn.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit attestation: %w", err)
	}
	return created, nil
}

// postAttestationRecordIn is PostAttestationRecord's read and write, made in txn and left for the
// caller to commit.
func postAttestationRecordIn(ctx context.Context, txn client.Txn, collection string, record *Record) (bool, error) {
	// A collection from txn.GetCollectionByName writes outside txn; one from GetCollections writes
	// inside it.
	cols, err := txn.GetCollections(ctx, options.GetCollections().SetCollectionName(collection))
	if err != nil {
		return false, fmt.Errorf("failed to get attestation collection: %w", err)
	}
	if len(cols) == 0 {
		return false, fmt.Errorf("failed to get attestation collection %s: %w", collection, client.ErrCollectionNotFound)
	}
	col := cols[0]

	existingDoc, err := lookupExistingAttestation(ctx, txn, col, record.AttestedDocID)
	if errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		return false, fmt.Errorf("attestation %s: %w", record.AttestedDocID, ErrDocumentNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("failed to lookup existing attestation: %w", err)
	}

	if existingDoc != nil {
		return false, updateAttestationRecord(ctx, col, existingDoc, record)
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
		return false, fmt.Errorf("failed to create attestation document: %w", err)
	}

	if err := col.SaveDocument(ctx, doc); err != nil {
		return false, err
	}
	return true, nil
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

// lookupExistingAttestation queries store for an existing attestation record
// matching the given attestedDocID and returns the Document if found.
// Returns (nil, nil) when no matching document exists.
func lookupExistingAttestation(ctx context.Context, store client.Store, col client.Collection, attestedDocID string) (*client.Document, error) {
	query := fmt.Sprintf(`query {
		%s(filter: {attested_doc: {_eq: "%s"}}) {
			_docID
		}
	}`, col.Name(), attestedDocID)

	result := store.ExecRequest(ctx, query)

	docIDStr := extractDocIDFromResult(result.GQL.Data, col.Name())
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
	existingSources := mergeStringListField(existingDoc, "source_doc", nil)
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

	// vote_count is a counter: a write adds its value to the stored count.
	if added := len(mergedSources) - len(existingSources); added > 0 {
		if err := existingDoc.Set(ctx, "vote_count", added); err != nil {
			return fmt.Errorf("failed to set vote_count: %w", err)
		}
	}

	err := col.UpdateDocument(ctx, existingDoc)
	if errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		return fmt.Errorf("attestation %s: %w", record.AttestedDocID, ErrDocumentNotFound)
	}
	return err
}
