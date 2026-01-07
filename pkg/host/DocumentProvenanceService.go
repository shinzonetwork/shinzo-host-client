package host

import (
	"fmt"
	"strings"
)

// ProcessingDecision represents whether a document should be processed
type ProcessingDecision struct {
	ShouldProcess bool
	Reason        string
	SkipType      string
}

// DocumentProvenanceService manages document metadata and processing decisions
type DocumentProvenanceService struct {
	maxProcessingDepth int
}

// NewDocumentProvenanceService creates a new provenance service
func NewDocumentProvenanceService() *DocumentProvenanceService {
	return &DocumentProvenanceService{
		maxProcessingDepth: 5, // Prevent infinite loops
	}
}

type PrimitiveDocument struct {
	ID          string
	Type        string
	BlockNumber uint64
	Data        map[string]interface{}
	Metadata    DocumentMetadata
}

// DocumentMetadata tracks the origin and processing history of documents
type DocumentMetadata struct {
	// Origin tracking
	SourceCollection string `json:"sourceCollection"`
	IsViewOutput     bool   `json:"isViewOutput"`
	ViewID           string `json:"viewID,omitempty"`

	// Processing control
	ProcessingDepth int    `json:"processingDepth"`
	ProcessingChain string `json:"processingChain"` // Comma-separated list of view IDs

	// Type information
	DocumentType  string `json:"documentType"`
	OriginalDocID string `json:"originalDocID,omitempty"`
}

// InitializeMetadata creates initial metadata for a source document
func (dps *DocumentProvenanceService) InitializeMetadata(doc PrimitiveDocument) DocumentMetadata {
	if doc.Metadata.ProcessingDepth == 0 {
		doc.Metadata = DocumentMetadata{
			SourceCollection: doc.Type,
			IsViewOutput:     false,
			ProcessingDepth:  0,
			ProcessingChain:  "",
			DocumentType:     doc.Type,
			OriginalDocID:    doc.ID,
		}
	}
	return doc.Metadata
}

// ConvertToPrimitiveDocument converts a Document to a PrimitiveDocument with initialized metadata
func (dps *DocumentProvenanceService) ConvertToPrimitiveDocument(doc Document) PrimitiveDocument {
	return PrimitiveDocument{
		ID:          doc.ID,
		Type:        doc.Type,
		BlockNumber: doc.BlockNumber,
		Data:        doc.Data,
		Metadata:    DocumentMetadata{},
	}
}

// ShouldProcessDocument determines if a document should be processed by a view
func (dps *DocumentProvenanceService) ShouldProcessDocument(doc PrimitiveDocument, viewID string) ProcessingDecision {
	// Initialize metadata if needed
	doc.Metadata = dps.InitializeMetadata(doc)

	// 1. Skip if processing depth exceeded (safety valve)
	if doc.Metadata.ProcessingDepth >= dps.maxProcessingDepth {
		return ProcessingDecision{
			ShouldProcess: false,
			Reason:        fmt.Sprintf("Processing depth exceeded: %d >= %d", doc.Metadata.ProcessingDepth, dps.maxProcessingDepth),
			SkipType:      "depth_limit",
		}
	}

	// 2. Skip if document is from the same view (prevent direct loop)
	if doc.Metadata.ViewID == viewID {
		return ProcessingDecision{
			ShouldProcess: false,
			Reason:        fmt.Sprintf("Document already processed by view %s", viewID),
			SkipType:      "same_view_loop",
		}
	}

	// 3. Skip if document has already been through this view (prevent indirect loop)
	if strings.Contains(doc.Metadata.ProcessingChain, viewID) {
		return ProcessingDecision{
			ShouldProcess: false,
			Reason:        fmt.Sprintf("Document already processed by view %s in chain: %s", viewID, doc.Metadata.ProcessingChain),
			SkipType:      "processing_chain_loop",
		}
	}

	// 4. Allow processing of view outputs, but with tracking
	if doc.Metadata.IsViewOutput {
		return ProcessingDecision{
			ShouldProcess: true,
			Reason:        fmt.Sprintf("View output from %s, allowing processing by %s", doc.Metadata.ViewID, viewID),
			SkipType:      "",
		}
	}

	// 5. Default: allow processing of source documents
	return ProcessingDecision{
		ShouldProcess: true,
		Reason:        "Source document ready for processing",
		SkipType:      "",
	}
}

// CreateViewOutputDocument creates metadata for a document output by a view
func (dps *DocumentProvenanceService) CreateViewOutputDocument(sourceDoc PrimitiveDocument, viewID string, outputData map[string]interface{}) PrimitiveDocument {
	// Build processing chain
	chain := sourceDoc.Metadata.ProcessingChain
	if chain == "" {
		chain = viewID
	} else {
		chain = fmt.Sprintf("%s,%s", chain, viewID)
	}

	return PrimitiveDocument{
		ID:          dps.generateOutputID(sourceDoc.ID, viewID),
		Type:        sourceDoc.Type, // Keep same type for consistency
		Data:        outputData,
		BlockNumber: sourceDoc.BlockNumber,
		Metadata: DocumentMetadata{
			SourceCollection: sourceDoc.Metadata.SourceCollection,
			IsViewOutput:     true,
			ViewID:           viewID,
			ProcessingDepth:  sourceDoc.Metadata.ProcessingDepth + 1,
			ProcessingChain:  chain,
			DocumentType:     sourceDoc.Metadata.DocumentType,
			OriginalDocID:    sourceDoc.Metadata.OriginalDocID,
		},
	}
}

// PreserveFilterFields ensures required fields are preserved in view outputs
func (dps *DocumentProvenanceService) PreserveFilterFields(sourceData, outputData map[string]interface{}, requiredFields []string) map[string]interface{} {
	// Copy output data to avoid mutation
	result := make(map[string]interface{})
	for k, v := range outputData {
		result[k] = v
	}

	// Preserve required fields from source if they're missing in output
	for _, field := range requiredFields {
		if _, exists := result[field]; !exists {
			if sourceValue, sourceExists := sourceData[field]; sourceExists {
				result[field] = sourceValue
			}
		}
	}

	return result
}

// GetRequiredFieldsByType returns the required filter fields for each document type
func (dps *DocumentProvenanceService) GetRequiredFieldsByType() map[string][]string {
	return map[string][]string{
		"Ethereum__Mainnet__Transaction":     {"from", "to", "hash", "blockNumber"},
		"Ethereum__Mainnet__Block":           {"hash", "number"},
		"Ethereum__Mainnet__Log":             {"address", "topics", "blockNumber"},
		"Ethereum__Mainnet__AccessListEntry": {"address", "storageKeys"},
		"Transaction":                        {"from", "to", "hash", "blockNumber"},
		"Block":                              {"hash", "number"},
		"Log":                                {"address", "topics", "blockNumber"},
		"AccessListEntry":                    {"address", "storageKeys"},
	}
}

// ValidateDocumentForView validates that a document has required fields for a view
func (dps *DocumentProvenanceService) ValidateDocumentForView(doc PrimitiveDocument, viewID string) error {
	// Get required fields for document type
	requiredFields := dps.GetRequiredFieldsByType()
	fields, exists := requiredFields[doc.Type]
	if !exists {
		// Default to common fields if type not found
		fields = []string{"from", "to", "hash", "address"}
	}

	// Check if document has at least one required field
	for _, field := range fields {
		if value, exists := doc.Data[field]; exists && value != nil {
			if strValue, ok := value.(string); ok && strValue != "" {
				return nil // Found at least one valid field
			}
			if _, ok := value.(int); ok {
				return nil // Found numeric field
			}
			if _, ok := value.(uint64); ok {
				return nil // Found numeric field
			}
		}
	}

	return fmt.Errorf("document %s of type %s missing required filter fields: %v", doc.ID, doc.Type, fields)
}

// generateOutputID creates a unique ID for view output documents
func (dps *DocumentProvenanceService) generateOutputID(sourceID, viewID string) string {
	// Create deterministic but unique ID
	return fmt.Sprintf("%s-%s", strings.Replace(sourceID, "bae-", "", 1), strings.Replace(viewID, "WASMView_", "", 1))
}

// GetProcessingStats returns statistics about document processing
func (dps *DocumentProvenanceService) GetProcessingStats(doc PrimitiveDocument) map[string]interface{} {
	return map[string]interface{}{
		"processing_depth":  doc.Metadata.ProcessingDepth,
		"is_view_output":    doc.Metadata.IsViewOutput,
		"source_collection": doc.Metadata.SourceCollection,
		"view_id":           doc.Metadata.ViewID,
		"processing_chain":  doc.Metadata.ProcessingChain,
		"chain_length":      len(strings.Split(doc.Metadata.ProcessingChain, ",")),
		"original_doc_id":   doc.Metadata.OriginalDocID,
	}
}
