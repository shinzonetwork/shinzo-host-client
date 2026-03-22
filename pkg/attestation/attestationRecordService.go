package attestation

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
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
func CreateAttestationRecord(ctx context.Context, verifier SignatureVerifier, docId string, sourceDocIds []string, docType string, versions []Version, maxConcurrentVerifications int) (*AttestationRecord, error) {
	if len(versions) == 0 {
		return &constants.AttestationRecord{
			AttestedDocId: docId,
			SourceDocIds:  sourceDocIds,
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
		SourceDocIds:  sourceDocIds,
		CIDs:          cids,
		DocType:       docType,
		VoteCount:     1,
	}, nil
}

// PostAttestationRecord posts the attestation record to DefraDB using read-modify-write
// to correctly merge source_doc lists when multiple indexers attest to the same block.
func PostAttestationRecord(ctx context.Context, defraNode *node.Node, record *AttestationRecord) error {
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
	if err != nil {
		return fmt.Errorf("failed to get attestation collection: %w", err)
	}

	existingDoc, err := lookupExistingAttestation(ctx, defraNode, col, record.AttestedDocId)
	if err != nil {
		return fmt.Errorf("failed to lookup existing attestation: %w", err)
	}

	if existingDoc != nil {
		// Merge source_doc identities
		mergedSources := mergeStringListField(existingDoc, "source_doc", record.SourceDocIds)
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

		return col.Save(ctx, existingDoc)
	}

	// Create new document
	sourceDocsAny := make([]any, len(record.SourceDocIds))
	for i, s := range record.SourceDocIds {
		sourceDocsAny[i] = s
	}
	cidsAny := make([]any, len(record.CIDs))
	for i, c := range record.CIDs {
		cidsAny[i] = c
	}

	data := map[string]any{
		"attested_doc": record.AttestedDocId,
		"source_doc":   sourceDocsAny,
		"CIDs":         cidsAny,
		"doc_type":     record.DocType,
		"vote_count":   record.VoteCount,
	}

	doc, err := client.NewDocFromMap(ctx, data, col.Version())
	if err != nil {
		return fmt.Errorf("failed to create attestation document: %w", err)
	}

	return col.Save(ctx, doc)
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
		return nil, nil
	}

	docID, err := client.NewDocIDFromString(docIDStr)
	if err != nil {
		return nil, err
	}

	return col.Get(ctx, docID)
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
		existingDoc, err := lookupExistingAttestation(ctx, defraNode, col, attestedDocID)
		if err != nil || existingDoc == nil {
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
			// Merge source_doc identities
			mergedSources := mergeStringListField(existingDoc, "source_doc", record.SourceDocIds)
			sourcesAny := make([]any, len(mergedSources))
			for i, s := range mergedSources {
				sourcesAny[i] = s
			}
			if err := existingDoc.Set(ctx, "source_doc", sourcesAny); err != nil {
				return fmt.Errorf("failed to set source_doc on existing doc: %w", err)
			}

			// Merge CIDs
			mergedCIDs := mergeStringListField(existingDoc, "CIDs", record.CIDs)
			cidsAny := make([]any, len(mergedCIDs))
			for i, c := range mergedCIDs {
				cidsAny[i] = c
			}
			if err := existingDoc.Set(ctx, "CIDs", cidsAny); err != nil {
				return fmt.Errorf("failed to set CIDs on existing doc: %w", err)
			}
			if err := existingDoc.Set(ctx, "vote_count", record.VoteCount); err != nil {
				return fmt.Errorf("failed to set vote_count on existing doc: %w", err)
			}
			docs = append(docs, existingDoc)
		} else {
			cidsAny := make([]any, len(record.CIDs))
			for i, cid := range record.CIDs {
				cidsAny[i] = cid
			}
			sourceDocsAny := make([]any, len(record.SourceDocIds))
			for i, s := range record.SourceDocIds {
				sourceDocsAny[i] = s
			}

			data := map[string]any{
				"attested_doc": record.AttestedDocId,
				"source_doc":   sourceDocsAny,
				"CIDs":         cidsAny,
				"doc_type":     record.DocType,
				"vote_count":   record.VoteCount,
			}

			doc, err := client.NewDocFromMap(ctx, data, col.Version())
			if err != nil {
				return fmt.Errorf("failed to create document for attestation %s: %w", record.AttestedDocId, err)
			}
			docs = append(docs, doc)
		}
	}

	return col.SaveMany(ctx, docs)
}

// HandleDocumentAttestation is the main handler for processing document attestations
func HandleDocumentAttestation(ctx context.Context, verifier SignatureVerifier, defraNode *node.Node, docID string, docType string, versions []Version, maxConcurrentVerifications int) error {

	if len(versions) == 0 {
		return nil
	}

	// Create attestation record with signature verification
	attestationRecord, _ := CreateAttestationRecord(ctx, verifier, docID, []string{docID}, docType, versions, maxConcurrentVerifications)

	if len(attestationRecord.CIDs) == 0 {
		return nil
	}

	// Post the attestation record to DefraDB
	if err := PostAttestationRecord(ctx, defraNode, attestationRecord); err != nil {
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

		record, _ := CreateAttestationRecord(ctx, verifier, input.DocID, []string{input.DocID}, input.DocType, input.Versions, maxConcurrentVerifications)

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

	// Merge source doc identities, avoiding duplicates
	sourceSet := make(map[string]bool)
	var mergedSources []string
	for _, s := range record1.SourceDocIds {
		if !sourceSet[s] {
			mergedSources = append(mergedSources, s)
			sourceSet[s] = true
		}
	}
	for _, s := range record2.SourceDocIds {
		if !sourceSet[s] {
			mergedSources = append(mergedSources, s)
			sourceSet[s] = true
		}
	}

	// Create merged record
	merged := &AttestationRecord{
		AttestedDocId: record1.AttestedDocId,
		SourceDocIds:  mergedSources,
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

	records, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
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
		for _, cid := range record.CIDs {
			if cid == documentCID {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetBlockAttestations retrieves all attestation records for a specific block height.
// With multiple indexers, different merkle roots produce separate attestation records.
func GetBlockAttestations(ctx context.Context, defraNode *node.Node, blockNumber int64) ([]AttestationRecord, error) {
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

	records, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	if err != nil {
		if strings.Contains(err.Error(), "No attestation records found") {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query block attestations for block %d: %w", blockNumber, err)
	}

	return records, nil
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
