package host

import (
	"strings"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

func TestNewDocumentProvenanceService(t *testing.T) {
	dps := NewDocumentProvenanceService()
	require.NotNil(t, dps)
	require.Equal(t, 5, dps.maxProcessingDepth)
}

func TestInitializeMetadata(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name     string
		doc      PrimitiveDocument
		expected DocumentMetadata
	}{
		{
			name: "zero processing depth initializes fresh metadata",
			doc: PrimitiveDocument{
				ID:   "bae-123",
				Type: "Transaction",
				Data: map[string]interface{}{"from": "0xabc"},
				Metadata: DocumentMetadata{
					ProcessingDepth: 0,
				},
			},
			expected: DocumentMetadata{
				SourceCollection: "Transaction",
				IsViewOutput:     false,
				ProcessingDepth:  0,
				ProcessingChain:  "",
				DocumentType:     "Transaction",
				OriginalDocID:    "bae-123",
			},
		},
		{
			name: "non-zero processing depth preserves existing metadata",
			doc: PrimitiveDocument{
				ID:   "bae-456",
				Type: "Block",
				Data: map[string]interface{}{"hash": "0xdef"},
				Metadata: DocumentMetadata{
					SourceCollection: "Transaction",
					IsViewOutput:     true,
					ViewID:           "view-A",
					ProcessingDepth:  2,
					ProcessingChain:  "view-A,view-B",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-orig",
				},
			},
			expected: DocumentMetadata{
				SourceCollection: "Transaction",
				IsViewOutput:     true,
				ViewID:           "view-A",
				ProcessingDepth:  2,
				ProcessingChain:  "view-A,view-B",
				DocumentType:     "Transaction",
				OriginalDocID:    "bae-orig",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.InitializeMetadata(tt.doc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToPrimitiveDocument(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name     string
		doc      Document
		expected PrimitiveDocument
	}{
		{
			name: "converts document with all fields",
			doc: Document{
				ID:          "bae-abc",
				Type:        "Transaction",
				BlockNumber: 100,
				Data:        map[string]any{"from": "0x1", "to": "0x2"},
			},
			expected: PrimitiveDocument{
				ID:          "bae-abc",
				Type:        "Transaction",
				BlockNumber: 100,
				Data:        map[string]interface{}{"from": "0x1", "to": "0x2"},
				Metadata:    DocumentMetadata{},
			},
		},
		{
			name: "converts document with nil data",
			doc: Document{
				ID:          "bae-empty",
				Type:        "Block",
				BlockNumber: 0,
				Data:        nil,
			},
			expected: PrimitiveDocument{
				ID:          "bae-empty",
				Type:        "Block",
				BlockNumber: 0,
				Data:        nil,
				Metadata:    DocumentMetadata{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.ConvertToPrimitiveDocument(tt.doc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldProcessDocument(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name     string
		doc      PrimitiveDocument
		viewID   string
		expected ProcessingDecision
	}{
		{
			name: "source document with zero depth should be processed",
			doc: PrimitiveDocument{
				ID:   "bae-1",
				Type: "Transaction",
				Data: map[string]interface{}{},
				Metadata: DocumentMetadata{
					ProcessingDepth: 0,
				},
			},
			viewID: "view-A",
			expected: ProcessingDecision{
				ShouldProcess: true,
				Reason:        "Source document ready for processing",
				SkipType:      "",
			},
		},
		{
			name: "depth limit exceeded",
			doc: PrimitiveDocument{
				ID:   "bae-2",
				Type: "Transaction",
				Data: map[string]interface{}{},
				Metadata: DocumentMetadata{
					ProcessingDepth: 5,
					ProcessingChain: "v1,v2,v3,v4,v5",
					SourceCollection: "Transaction",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-2",
				},
			},
			viewID: "view-B",
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Processing depth exceeded: 5 >= 5",
				SkipType:      "depth_limit",
			},
		},
		{
			name: "same view loop detected",
			doc: PrimitiveDocument{
				ID:   "bae-3",
				Type: "Transaction",
				Data: map[string]interface{}{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  1,
					ViewID:           "view-A",
					IsViewOutput:     true,
					ProcessingChain:  "view-A",
					SourceCollection: "Transaction",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-3",
				},
			},
			viewID: "view-A",
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Document already processed by view view-A",
				SkipType:      "same_view_loop",
			},
		},
		{
			name: "processing chain loop detected",
			doc: PrimitiveDocument{
				ID:   "bae-4",
				Type: "Transaction",
				Data: map[string]interface{}{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  2,
					ViewID:           "view-B",
					IsViewOutput:     true,
					ProcessingChain:  "view-A,view-B",
					SourceCollection: "Transaction",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-4",
				},
			},
			viewID: "view-A",
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Document already processed by view view-A in chain: view-A,view-B",
				SkipType:      "processing_chain_loop",
			},
		},
		{
			name: "view output from different view allowed",
			doc: PrimitiveDocument{
				ID:   "bae-5",
				Type: "Transaction",
				Data: map[string]interface{}{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  1,
					ViewID:           "view-A",
					IsViewOutput:     true,
					ProcessingChain:  "view-A",
					SourceCollection: "Transaction",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-5",
				},
			},
			viewID: "view-B",
			expected: ProcessingDecision{
				ShouldProcess: true,
				Reason:        "View output from view-A, allowing processing by view-B",
				SkipType:      "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.ShouldProcessDocument(tt.doc, tt.viewID)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateViewOutputDocument(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name       string
		sourceDoc  PrimitiveDocument
		viewID     string
		outputData map[string]interface{}
		expected   PrimitiveDocument
	}{
		{
			name: "creates output from source doc with empty chain",
			sourceDoc: PrimitiveDocument{
				ID:          "bae-src1",
				Type:        "Transaction",
				BlockNumber: 42,
				Data:        map[string]interface{}{"from": "0x1"},
				Metadata: DocumentMetadata{
					SourceCollection: "Transaction",
					ProcessingDepth:  0,
					ProcessingChain:  "",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-src1",
				},
			},
			viewID:     "WASMView_myview",
			outputData: map[string]interface{}{"result": "processed"},
			expected: PrimitiveDocument{
				ID:          "src1-myview",
				Type:        "Transaction",
				BlockNumber: 42,
				Data:        map[string]interface{}{"result": "processed"},
				Metadata: DocumentMetadata{
					SourceCollection: "Transaction",
					IsViewOutput:     true,
					ViewID:           "WASMView_myview",
					ProcessingDepth:  1,
					ProcessingChain:  "WASMView_myview",
					DocumentType:     "Transaction",
					OriginalDocID:    "bae-src1",
				},
			},
		},
		{
			name: "creates output from source doc with existing chain",
			sourceDoc: PrimitiveDocument{
				ID:          "bae-src2",
				Type:        "Log",
				BlockNumber: 99,
				Data:        map[string]interface{}{"address": "0xabc"},
				Metadata: DocumentMetadata{
					SourceCollection: "Log",
					IsViewOutput:     true,
					ViewID:           "view-A",
					ProcessingDepth:  1,
					ProcessingChain:  "view-A",
					DocumentType:     "Log",
					OriginalDocID:    "bae-orig",
				},
			},
			viewID:     "view-B",
			outputData: map[string]interface{}{"enriched": true},
			expected: PrimitiveDocument{
				ID:          "src2-view-B",
				Type:        "Log",
				BlockNumber: 99,
				Data:        map[string]interface{}{"enriched": true},
				Metadata: DocumentMetadata{
					SourceCollection: "Log",
					IsViewOutput:     true,
					ViewID:           "view-B",
					ProcessingDepth:  2,
					ProcessingChain:  "view-A,view-B",
					DocumentType:     "Log",
					OriginalDocID:    "bae-orig",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.CreateViewOutputDocument(tt.sourceDoc, tt.viewID, tt.outputData)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPreserveFilterFields(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name           string
		sourceData     map[string]interface{}
		outputData     map[string]interface{}
		requiredFields []string
		expected       map[string]interface{}
	}{
		{
			name:           "preserves missing required fields from source",
			sourceData:     map[string]interface{}{"from": "0x1", "to": "0x2", "hash": "0xabc"},
			outputData:     map[string]interface{}{"result": "done"},
			requiredFields: []string{"from", "to"},
			expected:       map[string]interface{}{"result": "done", "from": "0x1", "to": "0x2"},
		},
		{
			name:           "does not overwrite existing output fields",
			sourceData:     map[string]interface{}{"from": "0x1", "to": "0x2"},
			outputData:     map[string]interface{}{"from": "0xoverride", "result": "done"},
			requiredFields: []string{"from", "to"},
			expected:       map[string]interface{}{"from": "0xoverride", "result": "done", "to": "0x2"},
		},
		{
			name:           "handles field missing from both source and output",
			sourceData:     map[string]interface{}{"from": "0x1"},
			outputData:     map[string]interface{}{"result": "done"},
			requiredFields: []string{"from", "to", "hash"},
			expected:       map[string]interface{}{"result": "done", "from": "0x1"},
		},
		{
			name:           "empty required fields returns copy of output",
			sourceData:     map[string]interface{}{"from": "0x1"},
			outputData:     map[string]interface{}{"result": "done"},
			requiredFields: []string{},
			expected:       map[string]interface{}{"result": "done"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.PreserveFilterFields(tt.sourceData, tt.outputData, tt.requiredFields)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetRequiredFieldsByType(t *testing.T) {
	dps := NewDocumentProvenanceService()

	result := dps.GetRequiredFieldsByType()
	require.NotNil(t, result)

	tests := []struct {
		name           string
		docType        string
		expectedFields []string
	}{
		{
			name:           "Ethereum Transaction",
			docType:        "Ethereum__Mainnet__Transaction",
			expectedFields: []string{"from", "to", "hash", "blockNumber"},
		},
		{
			name:           "Ethereum Block",
			docType:        "Ethereum__Mainnet__Block",
			expectedFields: []string{"hash", "number"},
		},
		{
			name:           "Ethereum Log",
			docType:        "Ethereum__Mainnet__Log",
			expectedFields: []string{"address", "topics", "blockNumber"},
		},
		{
			name:           "Ethereum AccessListEntry",
			docType:        "Ethereum__Mainnet__AccessListEntry",
			expectedFields: []string{"address", "storageKeys"},
		},
		{
			name:           "short Transaction",
			docType:        "Transaction",
			expectedFields: []string{"from", "to", "hash", "blockNumber"},
		},
		{
			name:           "short Block",
			docType:        "Block",
			expectedFields: []string{"hash", "number"},
		},
		{
			name:           "short Log",
			docType:        "Log",
			expectedFields: []string{"address", "topics", "blockNumber"},
		},
		{
			name:           "short AccessListEntry",
			docType:        "AccessListEntry",
			expectedFields: []string{"address", "storageKeys"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, exists := result[tt.docType]
			require.True(t, exists, "expected key %s to exist", tt.docType)
			require.Equal(t, tt.expectedFields, fields)
		})
	}
}

func TestValidateDocumentForView(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name        string
		doc         PrimitiveDocument
		viewID      string
		expectError bool
	}{
		{
			name: "valid transaction with string field",
			doc: PrimitiveDocument{
				ID:   "bae-1",
				Type: "Transaction",
				Data: map[string]interface{}{"from": "0xabc", "to": "0xdef"},
			},
			viewID:      "view-1",
			expectError: false,
		},
		{
			name: "valid block with int field",
			doc: PrimitiveDocument{
				ID:   "bae-2",
				Type: "Block",
				Data: map[string]interface{}{"number": int(42)},
			},
			viewID:      "view-2",
			expectError: false,
		},
		{
			name: "valid block with uint64 field",
			doc: PrimitiveDocument{
				ID:   "bae-3",
				Type: "Block",
				Data: map[string]interface{}{"number": uint64(100)},
			},
			viewID:      "view-3",
			expectError: false,
		},
		{
			name: "missing all required fields",
			doc: PrimitiveDocument{
				ID:   "bae-4",
				Type: "Transaction",
				Data: map[string]interface{}{"unrelated": "value"},
			},
			viewID:      "view-4",
			expectError: true,
		},
		{
			name: "empty string field does not satisfy requirement",
			doc: PrimitiveDocument{
				ID:   "bae-5",
				Type: "Transaction",
				Data: map[string]interface{}{"from": "", "to": ""},
			},
			viewID:      "view-5",
			expectError: true,
		},
		{
			name: "nil value does not satisfy requirement",
			doc: PrimitiveDocument{
				ID:   "bae-6",
				Type: "Transaction",
				Data: map[string]interface{}{"from": nil},
			},
			viewID:      "view-6",
			expectError: true,
		},
		{
			name: "unknown type falls back to default required fields",
			doc: PrimitiveDocument{
				ID:   "bae-7",
				Type: "UnknownType",
				Data: map[string]interface{}{"address": "0x123"},
			},
			viewID:      "view-7",
			expectError: false,
		},
		{
			name: "unknown type with no matching fields errors",
			doc: PrimitiveDocument{
				ID:   "bae-8",
				Type: "UnknownType",
				Data: map[string]interface{}{"unrelated": "value"},
			},
			viewID:      "view-8",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dps.ValidateDocumentForView(tt.doc, tt.viewID)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "missing required filter fields")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGenerateOutputID(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name     string
		sourceID string
		viewID   string
		expected string
	}{
		{
			name:     "strips bae- prefix and WASMView_ prefix",
			sourceID: "bae-abc123",
			viewID:   "WASMView_myview",
			expected: "abc123-myview",
		},
		{
			name:     "no prefixes to strip",
			sourceID: "plain-id",
			viewID:   "plain-view",
			expected: "plain-id-plain-view",
		},
		{
			name:     "only bae- prefix present",
			sourceID: "bae-xyz",
			viewID:   "regular-view",
			expected: "xyz-regular-view",
		},
		{
			name:     "only WASMView_ prefix present",
			sourceID: "no-prefix",
			viewID:   "WASMView_special",
			expected: "no-prefix-special",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.generateOutputID(tt.sourceID, tt.viewID)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProcessingStats(t *testing.T) {
	dps := NewDocumentProvenanceService()

	tests := []struct {
		name     string
		doc      PrimitiveDocument
		expected map[string]interface{}
	}{
		{
			name: "zero depth source document",
			doc: PrimitiveDocument{
				ID:   "bae-1",
				Type: "Transaction",
				Metadata: DocumentMetadata{
					ProcessingDepth:  0,
					IsViewOutput:     false,
					SourceCollection: "Transaction",
					ViewID:           "",
					ProcessingChain:  "",
					OriginalDocID:    "bae-1",
				},
			},
			expected: map[string]interface{}{
				"processing_depth":  0,
				"is_view_output":    false,
				"source_collection": "Transaction",
				"view_id":           "",
				"processing_chain":  "",
				"chain_length":      1, // splitting empty string gives [""]
				"original_doc_id":   "bae-1",
			},
		},
		{
			name: "multi-depth view output document",
			doc: PrimitiveDocument{
				ID:   "derived-1",
				Type: "Transaction",
				Metadata: DocumentMetadata{
					ProcessingDepth:  3,
					IsViewOutput:     true,
					SourceCollection: "Transaction",
					ViewID:           "view-C",
					ProcessingChain:  "view-A,view-B,view-C",
					OriginalDocID:    "bae-orig",
				},
			},
			expected: map[string]interface{}{
				"processing_depth":  3,
				"is_view_output":    true,
				"source_collection": "Transaction",
				"view_id":           "view-C",
				"processing_chain":  "view-A,view-B,view-C",
				"chain_length":      len(strings.Split("view-A,view-B,view-C", ",")),
				"original_doc_id":   "bae-orig",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dps.GetProcessingStats(tt.doc)
			require.Equal(t, tt.expected, result)
		})
	}
}
