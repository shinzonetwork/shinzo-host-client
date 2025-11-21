package host

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	indexerschema "github.com/shinzonetwork/indexer/pkg/schema"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/graphql"
)

// extractSchemaTypes extracts all type names from a GraphQL SDL schema
func extractSchemaTypes(schema string) ([]string, error) {
	// Find all type definitions: type TypeName { ... }
	re := regexp.MustCompile(`type\s+(\w+)\s*@?[^{]*\{`)
	matches := re.FindAllStringSubmatch(schema, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no type definitions found in schema")
	}

	types := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			typeName := strings.TrimSpace(match[1])
			types = append(types, typeName)
		}
	}

	return types, nil
}

// getPrimitiveAttestationRecordSDL generates the SDL for an attestation record collection for a primitive
// Note: This does NOT include source_doc, unlike view attestation records
func getPrimitiveAttestationRecordSDL(primitiveName string) string {
	return fmt.Sprintf(`type AttestationRecord_%s {
		attested_doc: String
		CIDs: [String]
	}`, primitiveName)
}

// appendAttestationRecordSchemas appends attestation record schemas for all primitives to the base schema
func appendAttestationRecordSchemas(baseSchema string) (string, error) {
	primitives, err := extractSchemaTypes(baseSchema)
	if err != nil {
		return "", fmt.Errorf("failed to extract primitive types: %w", err)
	}

	var attestationSchemas strings.Builder
	for _, primitive := range primitives {
		attestationSchemas.WriteString("\n")
		attestationSchemas.WriteString(getPrimitiveAttestationRecordSDL(primitive))
		attestationSchemas.WriteString("\n")
	}

	return baseSchema + attestationSchemas.String(), nil
}

// generateQueryForPrimitive generates a GraphQL query that returns _docID and _version for a primitive type
func generateQueryForPrimitive(primitiveName string, startingBlockNumber uint64, endingBlockNumber uint64) (string, error) {
	// Build a simple query with just _docID and _version
	baseQuery := fmt.Sprintf(`%s { _docID _version { cid signature { type identity value } } }`, primitiveName)

	// Apply block number filter - this will add the filter and preserve our selection set
	query, err := graphql.WithBlockNumberFilter(baseQuery, startingBlockNumber, endingBlockNumber)
	if err != nil {
		return "", fmt.Errorf("failed to apply block number filter: %w", err)
	}

	// No need to call WithReturnDocIdAndVersion since we already have _docID and _version in the query
	return query, nil
}

// PostPrimitiveAttestationRecords processes attestation records for all primitive types
func (h *Host) PostPrimitiveAttestationRecords(ctx context.Context, startingBlockNumber uint64, endingBlockNumber uint64) error {
	schema := indexerschema.GetSchema()
	primitives, err := extractSchemaTypes(schema)
	if err != nil {
		return fmt.Errorf("failed to extract primitive types: %w", err)
	}

	// Skip AccessListEntry due to DefraDB planner issues
	primitivesToProcess := make([]string, 0)
	for _, primitive := range primitives {
		if primitive != "AccessListEntry" {
			primitivesToProcess = append(primitivesToProcess, primitive)
		}
	}

	for _, primitive := range primitivesToProcess {
		err := h.postAttestationRecordsForPrimitive(ctx, primitive, startingBlockNumber, endingBlockNumber)
		if err != nil {
			logger.Sugar.Errorf("Error posting attestation records for primitive %s: %w", primitive, err)
			// Continue with other primitives even if one fails
			continue
		}
	}

	return nil
}

// postAttestationRecordsForPrimitive handles attestation records for a single primitive type
func (h *Host) postAttestationRecordsForPrimitive(ctx context.Context, primitiveName string, startingBlockNumber uint64, endingBlockNumber uint64) error {
	// Ensure startingBlockNumber is at least 1 to avoid query issues
	if startingBlockNumber == 0 {
		startingBlockNumber = 1
	}

	// For AccessListEntry, query directly without nested blockNumber filters to avoid DefraDB planner panic
	// We'll query all AccessListEntry and filter by transaction blockNumber in a separate query
	var documents []map[string]any
	var err error
	if primitiveName == "AccessListEntry" {
		// Query all AccessListEntry documents with transaction _docID (but not blockNumber to avoid panic)
		accessListQuery := `AccessListEntry { _docID _version { cid signature { type identity value } } transaction { _docID } }`

		// Query documents - use a recover to catch any panics from DefraDB
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Sugar.Warnf("Panic while querying AccessListEntry: %v. Skipping AccessListEntry.", r)
					err = fmt.Errorf("panic while querying: %v", r)
				}
			}()
			documents, err = defra.QueryArray[map[string]any](ctx, h.DefraNode, accessListQuery)
		}()

		if err != nil {
			logger.Sugar.Warnf("Skipping AccessListEntry attestation records due to query issue: %v", err)
			return nil
		}

		// Get unique transaction IDs from AccessListEntry documents
		transactionIDs := make(map[string]bool)
		for _, doc := range documents {
			if transaction, ok := doc["transaction"].(map[string]any); ok {
				if txID, ok := transaction["_docID"].(string); ok && txID != "" {
					transactionIDs[txID] = true
				}
			}
		}

		// Query transactions to get block numbers for the transaction IDs
		if len(transactionIDs) > 0 {
			// Build filter for transaction IDs
			txIDList := make([]string, 0, len(transactionIDs))
			for txID := range transactionIDs {
				txIDList = append(txIDList, fmt.Sprintf(`"%s"`, txID))
			}
			txIDFilter := strings.Join(txIDList, ", ")

			// Also filter by block number to reduce the set
			transactionQuery := fmt.Sprintf(`Transaction(filter: { _and: [ { _docID: { _in: [%s] } }, { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ] }) { _docID blockNumber }`, txIDFilter, startingBlockNumber, endingBlockNumber)

			var transactions []map[string]any
			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Sugar.Warnf("Panic while querying Transaction for AccessListEntry filtering: %v. Skipping AccessListEntry.", r)
						err = fmt.Errorf("panic while querying transactions: %v", r)
					}
				}()
				transactions, err = defra.QueryArray[map[string]any](ctx, h.DefraNode, transactionQuery)
			}()

			if err != nil {
				logger.Sugar.Warnf("Skipping AccessListEntry attestation records due to transaction query issue: %v", err)
				return nil
			}

			// Create a set of valid transaction IDs within the block range
			validTxIDs := make(map[string]bool)
			for _, tx := range transactions {
				if txID, ok := tx["_docID"].(string); ok {
					validTxIDs[txID] = true
				}
			}

			// Filter AccessListEntry documents to only those with valid transaction IDs
			filteredDocuments := make([]map[string]any, 0)
			for _, doc := range documents {
				if transaction, ok := doc["transaction"].(map[string]any); ok {
					if txID, ok := transaction["_docID"].(string); ok && validTxIDs[txID] {
						filteredDocuments = append(filteredDocuments, doc)
					}
				}
			}
			documents = filteredDocuments
		} else {
			documents = []map[string]any{} // No valid transaction IDs found
		}
	} else {
		// Generate query for this primitive with block number filter
		query, err := generateQueryForPrimitive(primitiveName, startingBlockNumber, endingBlockNumber)
		if err != nil {
			return fmt.Errorf("failed to generate query for primitive %s: %w", primitiveName, err)
		}

		// Query documents - use a recover to catch any panics from DefraDB
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Sugar.Warnf("Panic while querying primitive %s with query %s: %v. Skipping this primitive.", primitiveName, query, r)
					err = fmt.Errorf("panic while querying: %v", r)
				}
			}()
			documents, err = defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
		}()

		if err != nil {
			return fmt.Errorf("error fetching documents for primitive %s with query %s: %w", primitiveName, query, err)
		}
	}

	if len(documents) == 0 {
		// No documents found, this is not an error - just return
		logger.Sugar.Debugf("No documents found for primitive %s in block range %d-%d", primitiveName, startingBlockNumber, endingBlockNumber)
		return nil
	}

	logger.Sugar.Debugf("Found %d documents for primitive %s in block range %d-%d", len(documents), primitiveName, startingBlockNumber, endingBlockNumber)

	// Sort documents by block number
	documents, err = sortDescendingBlockNumber(documents)
	if err != nil {
		return fmt.Errorf("error sorting documents by blockNumber for primitive %s: %w", primitiveName, err)
	}

	// Process each document and create attestation records
	postedCount := 0
	for _, document := range documents {
		docId, ok := document["_docID"].(string)
		if !ok {
			logger.Sugar.Warnf("Error retrieving _docID from document for primitive %s: %+v", primitiveName, document)
			continue
		}

		versions, err := extractVersionFromDocument(document)
		if err != nil {
			logger.Sugar.Warnf("Error retrieving _version from document for primitive %s: %w", primitiveName, err)
			continue
		}

		// Create attestation record (without source_doc for primitives)
		verifier := hostAttestation.NewDefraSignatureVerifier(h.DefraNode)
		attestationRecord, err := hostAttestation.CreateAttestationRecordForPrimitive(ctx, verifier, docId, versions)
		if err != nil {
			logger.Sugar.Warnf("Error creating attestation record for primitive %s: %w", primitiveName, err)
			continue
		}

		// Post the attestation record
		err = attestationRecord.PostAttestationRecordForPrimitive(ctx, h.DefraNode, primitiveName)
		if err != nil {
			logger.Sugar.Warnf("Error posting attestation record for primitive %s: %w", primitiveName, err)
			continue
		}
		postedCount++
	}

	if postedCount > 0 {
		logger.Sugar.Infof("Posted %d attestation records for primitive %s", postedCount, primitiveName)
	}

	return nil
}
