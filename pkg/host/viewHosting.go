package host

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/graphql"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/client"
)

// extractVersionFromDocument extracts the _version field from a document map and converts it to []attestation.Version
func extractVersionFromDocument(doc map[string]any) ([]attestation.Version, error) {
	versionData, exists := doc["_version"]
	if !exists {
		return nil, fmt.Errorf("_version field not found in document")
	}

	versionArray, ok := versionData.([]any)
	if !ok {
		return nil, fmt.Errorf("_version field is not an array, got type: %T", versionData)
	}

	versions := make([]attestation.Version, 0, len(versionArray))
	for i, v := range versionArray {
		versionMap, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("_version[%d] is not a map, got type: %T", i, v)
		}

		version := attestation.Version{}

		// Extract CID
		if cid, ok := versionMap["cid"].(string); ok {
			version.CID = cid
		}

		// Extract signature
		sigMap, ok := versionMap["signature"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("_version[%d].signature is not a map, got type: %T", i, versionMap["signature"])
		}

		signature := attestation.Signature{}
		if sigType, ok := sigMap["type"].(string); ok {
			signature.Type = sigType
		}
		if identity, ok := sigMap["identity"].(string); ok {
			signature.Identity = identity
		}
		if value, ok := sigMap["value"].(string); ok {
			signature.Value = value
		}

		version.Signature = signature
		versions = append(versions, version)
	}

	return versions, nil
}

func (h *Host) PrepareView(ctx context.Context, v view.View) error {
	err := v.SubscribeTo(ctx, h.DefraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Error subscribing to view %+v: %w", v, err)
		} else {
			return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
		}
	}

	err = attestation.AddAttestationRecordCollection(ctx, h.DefraNode, v.Name)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Error subscribing to view %+v: %w", v, err)
		} else {
			return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
		}
	}

	if v.HasLenses() {
		err = v.PostWasmToFile(ctx, h.LensRegistryPath)
		if err != nil {
			return fmt.Errorf("Error downloading lenses to local env: %w", err)
		}

		err = v.ConfigureLens(ctx, h.DefraNode)
		if err != nil {
			return fmt.Errorf("Error configuring lenses: %w", err)
		}
	}

	// Store the view to defra so it can be recovered on restart
	err = h.storeView(ctx, v)
	if err != nil {
		return fmt.Errorf("Error storing view to defra: %w", err)
	}

	return nil
}

// storeView saves a view to the defra View collection so it can be recovered on restart
func (h *Host) storeView(ctx context.Context, v view.View) error {
	return h.storeViewWithBlock(ctx, v, 0)
}

// storeViewWithBlock saves a view to the defra View collection with a specific lastProcessedBlock
func (h *Host) storeViewWithBlock(ctx context.Context, v view.View, lastProcessedBlock uint64) error {
	collection, err := h.DefraNode.DB.GetCollectionByName(ctx, "View")
	if err != nil {
		return fmt.Errorf("error getting View collection: %w", err)
	}

	// Prepare the view document with the fields from the schema
	viewDoc := make(map[string]any)
	viewDoc["name"] = v.Name
	if v.Query != nil {
		viewDoc["query"] = *v.Query
	} else {
		viewDoc["query"] = ""
	}
	if v.Sdl != nil {
		viewDoc["sdl"] = *v.Sdl
	} else {
		viewDoc["sdl"] = ""
	}
	viewDoc["lastProcessedBlock"] = int64(lastProcessedBlock)

	// Create a document from the map
	document, err := client.NewDocFromMap(viewDoc, collection.Version())
	if err != nil {
		return fmt.Errorf("failed to create document from map: %w", err)
	}

	// Save will update if document exists (by unique name), create if it doesn't
	err = collection.Save(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to save view document: %w", err)
	}

	return nil
}

// updateViewLastProcessedBlock updates the lastProcessedBlock field for a view in the View collection
func (h *Host) updateViewLastProcessedBlock(ctx context.Context, viewName string, blockNumber uint64) error {
	collection, err := h.DefraNode.DB.GetCollectionByName(ctx, "View")
	if err != nil {
		return fmt.Errorf("error getting View collection: %w", err)
	}

	// Query for the existing view document by name
	query := fmt.Sprintf(`query {
		View(filter: {name: {_eq: "%s"}}) {
			_docID
			name
			query
			sdl
			lastProcessedBlock
		}
	}`, viewName)

	type ViewDoc struct {
		DocID              string `json:"_docID"`
		Name               string `json:"name"`
		Query              string `json:"query"`
		Sdl                string `json:"sdl"`
		LastProcessedBlock int64  `json:"lastProcessedBlock"`
	}

	views, err := defra.QueryArray[ViewDoc](ctx, h.DefraNode, query)
	if err != nil {
		return fmt.Errorf("error querying view: %w", err)
	}

	if len(views) == 0 {
		return fmt.Errorf("view %s not found in View collection", viewName)
	}

	viewDoc := views[0]

	// Update the document with the new lastProcessedBlock
	updateDoc := map[string]any{
		"_docID":             viewDoc.DocID,
		"name":               viewDoc.Name,
		"query":              viewDoc.Query,
		"sdl":                viewDoc.Sdl,
		"lastProcessedBlock": int64(blockNumber),
	}

	document, err := client.NewDocFromMap(updateDoc, collection.Version())
	if err != nil {
		return fmt.Errorf("failed to create document from map: %w", err)
	}

	err = collection.Save(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to update view document: %w", err)
	}

	return nil
}

// sortDescendingBlockNumber sorts documents by blockNumber in descending order
// This is a workaround for DefraDB panicking when using order parameters in queries
// The function handles different field locations based on the schema:
//   - blockNumber: Int field (Log, Transaction) - see schema.graphql
//   - number: Int field (Block) - see schema.graphql
//   - transaction.blockNumber: nested Int field (AccessListEntry via Transaction relation) - see schema.graphql
func sortDescendingBlockNumber(documents []map[string]any) ([]map[string]any, error) {
	if len(documents) == 0 {
		return documents, nil
	}

	// Create a copy to avoid mutating the original slice
	sorted := make([]map[string]any, len(documents))
	copy(sorted, documents)

	// Extract block number from a document, handling different field locations
	// Schema defines these as Int, but JSON unmarshaling may produce various numeric types
	extractBlockNumber := func(doc map[string]any) (uint64, error) {
		// Helper to convert various numeric types to uint64
		convertToUint64 := func(v any) (uint64, bool) {
			switch val := v.(type) {
			case uint64:
				return val, true
			case uint32:
				return uint64(val), true
			case int64:
				if val < 0 {
					return 0, false
				}
				return uint64(val), true
			case int32:
				if val < 0 {
					return 0, false
				}
				return uint64(val), true
			case int:
				if val < 0 {
					return 0, false
				}
				return uint64(val), true
			case float64:
				// JSON numbers are typically unmarshaled as float64
				if val < 0 {
					return 0, false
				}
				return uint64(val), true
			case float32:
				if val < 0 {
					return 0, false
				}
				return uint64(val), true
			default:
				return 0, false
			}
		}

		// Try blockNumber first (Log, Transaction - schema: blockNumber: Int)
		if blockNum, ok := doc["blockNumber"]; ok && blockNum != nil {
			if result, ok := convertToUint64(blockNum); ok {
				return result, nil
			}
		}

		// Try number field (Block - schema: number: Int)
		if num, ok := doc["number"]; ok && num != nil {
			if result, ok := convertToUint64(num); ok {
				return result, nil
			}
		}

		// Try nested transaction.blockNumber (AccessListEntry -> Transaction.blockNumber: Int)
		if transaction, ok := doc["transaction"].(map[string]any); ok && transaction != nil {
			if blockNum, ok := transaction["blockNumber"]; ok && blockNum != nil {
				if result, ok := convertToUint64(blockNum); ok {
					return result, nil
				}
			}
		}

		return 0, fmt.Errorf("unable to extract blockNumber from document (tried blockNumber, number, and transaction.blockNumber): %+v", doc)
	}

	// Sort in descending order
	sort.Slice(sorted, func(i, j int) bool {
		blockNumI, errI := extractBlockNumber(sorted[i])
		blockNumJ, errJ := extractBlockNumber(sorted[j])

		// If either extraction fails, maintain original order
		if errI != nil || errJ != nil {
			return false
		}

		return blockNumI > blockNumJ
	})

	return sorted, nil
}

func (h *Host) ApplyView(ctx context.Context, v view.View, startingBlockNumber uint64, endingBlockNumber uint64) error {
	query, err := graphql.WithBlockNumberFilter(*v.Query, startingBlockNumber, endingBlockNumber)
	if err != nil {
		return fmt.Errorf("Error assembling query: %w", err)
	}

	query, err = graphql.WithReturnDocIdAndVersion(query)
	if err != nil {
		return fmt.Errorf("Error assembling query: %w", err)
	}

	sourceDocuments, err := defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
	if err != nil {
		return fmt.Errorf("Error fetching source data with query %s: %w", query, err)
	}
	if len(sourceDocuments) == 0 {
		return fmt.Errorf("No source data found using query %s", query)
	}

	sourceDocuments, err = sortDescendingBlockNumber(sourceDocuments)
	if err != nil {
		return fmt.Errorf("Error sorting documents by blockNumber: %w", err)
	}

	type attestationInfo struct {
		SourceDocumentId string
		Version          []attestation.Version
	}

	transformedDocuments := map[*attestationInfo][]map[string]any{} // mapping source doc attestation info to transformed docs ([]map[string]any)
	for _, sourceDocument := range sourceDocuments {
		sourceDocumentId, ok := sourceDocument["_docID"].(string)
		if !ok {
			return fmt.Errorf("Error retrieving _docID from source document: %+v", sourceDocument)
		}
		sourceVersion, err := extractVersionFromDocument(sourceDocument)
		if err != nil {
			return fmt.Errorf("Error retrieving _version from source document: %w", err)
		}
		sourceAttestationInfo := &attestationInfo{
			SourceDocumentId: sourceDocumentId,
			Version:          sourceVersion,
		}

		if v.HasLenses() {
			transformed, err := v.ApplyLensTransform(ctx, h.DefraNode, query)
			if err != nil {
				return fmt.Errorf("Error applying lens transforms from view %s: %w", v.Name, err)
			}
			if len(transformed) == 0 {
				continue
			}
			transformedDocuments[sourceAttestationInfo] = transformed
		} else {
			// For views without lenses, use the source collection directly
			transformedDocuments[sourceAttestationInfo] = sourceDocuments
		}
	}

	for sourceDocumentAttestationInfo, transformedDocs := range transformedDocuments {
		transformedDocIds, err := v.WriteTransformedToCollection(ctx, h.DefraNode, transformedDocs)
		if err != nil {
			return fmt.Errorf("Error writing transformed data to collection %s: %w", v.Name, err)
		}

		for _, transformedDocId := range transformedDocIds {
			verifier := hostAttestation.NewDefraSignatureVerifier(h.DefraNode)
			attestationRecord, err := hostAttestation.CreateAttestationRecord(ctx, verifier, transformedDocId, sourceDocumentAttestationInfo.SourceDocumentId, sourceDocumentAttestationInfo.Version)
			if err != nil {
				return fmt.Errorf("Error creating attestation record: %w", err)
			}

			err = attestationRecord.PostAttestationRecord(ctx, h.DefraNode, v.Name)
			if err != nil {
				return fmt.Errorf("Error posting attestation record %+v: %w", attestationRecord, err)
			}
		}
	}

	return nil
}
