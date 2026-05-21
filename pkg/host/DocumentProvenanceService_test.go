package host

import (
	"strings"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
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
				Type: docTypeTransaction,
				Data: map[string]any{gqlFieldFrom: "0xabc"},
				Metadata: DocumentMetadata{
					ProcessingDepth: 0,
				},
			},
			expected: DocumentMetadata{
				SourceCollection: docTypeTransaction,
				IsViewOutput:     false,
				ProcessingDepth:  0,
				ProcessingChain:  "",
				DocumentType:     docTypeTransaction,
				OriginalDocID:    "bae-123",
			},
		},
		{
			name: "non-zero processing depth preserves existing metadata",
			doc: PrimitiveDocument{
				ID:   "bae-456",
				Type: docTypeBlock,
				Data: map[string]any{gqlFieldHash: "0xdef"},
				Metadata: DocumentMetadata{
					SourceCollection: docTypeTransaction,
					IsViewOutput:     true,
					ViewID:           testViewA,
					ProcessingDepth:  2,
					ProcessingChain:  testViewAB,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBaeOrig,
				},
			},
			expected: DocumentMetadata{
				SourceCollection: docTypeTransaction,
				IsViewOutput:     true,
				ViewID:           testViewA,
				ProcessingDepth:  2,
				ProcessingChain:  testViewAB,
				DocumentType:     docTypeTransaction,
				OriginalDocID:    testBaeOrig,
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
				Type:        docTypeTransaction,
				BlockNumber: 100,
				Data:        map[string]any{gqlFieldFrom: "0x1", gqlFieldTo: "0x2"},
			},
			expected: PrimitiveDocument{
				ID:          "bae-abc",
				Type:        docTypeTransaction,
				BlockNumber: 100,
				Data:        map[string]any{gqlFieldFrom: "0x1", gqlFieldTo: "0x2"},
				Metadata:    DocumentMetadata{},
			},
		},
		{
			name: "converts document with nil data",
			doc: Document{
				ID:          "bae-empty",
				Type:        docTypeBlock,
				BlockNumber: 0,
				Data:        nil,
			},
			expected: PrimitiveDocument{
				ID:          "bae-empty",
				Type:        docTypeBlock,
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
				ID:   testBae1,
				Type: docTypeTransaction,
				Data: map[string]any{},
				Metadata: DocumentMetadata{
					ProcessingDepth: 0,
				},
			},
			viewID: testViewA,
			expected: ProcessingDecision{
				ShouldProcess: true,
				Reason:        "Source document ready for processing",
				SkipType:      "",
			},
		},
		{
			name: "depth limit exceeded",
			doc: PrimitiveDocument{
				ID:   testBae2,
				Type: docTypeTransaction,
				Data: map[string]any{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  5,
					ProcessingChain:  "v1,v2,v3,v4,v5",
					SourceCollection: docTypeTransaction,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBae2,
				},
			},
			viewID: testViewB,
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Processing depth exceeded: 5 >= 5",
				SkipType:      "depth_limit",
			},
		},
		{
			name: "same view loop detected",
			doc: PrimitiveDocument{
				ID:   testBae3,
				Type: docTypeTransaction,
				Data: map[string]any{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  1,
					ViewID:           testViewA,
					IsViewOutput:     true,
					ProcessingChain:  testViewA,
					SourceCollection: docTypeTransaction,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBae3,
				},
			},
			viewID: testViewA,
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Document already processed by view view-A",
				SkipType:      "same_view_loop",
			},
		},
		{
			name: "processing chain loop detected",
			doc: PrimitiveDocument{
				ID:   testBae4,
				Type: docTypeTransaction,
				Data: map[string]any{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  2,
					ViewID:           testViewB,
					IsViewOutput:     true,
					ProcessingChain:  testViewAB,
					SourceCollection: docTypeTransaction,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBae4,
				},
			},
			viewID: testViewA,
			expected: ProcessingDecision{
				ShouldProcess: false,
				Reason:        "Document already processed by view view-A in chain: view-A,view-B",
				SkipType:      "processing_chain_loop",
			},
		},
		{
			name: "view output from different view allowed",
			doc: PrimitiveDocument{
				ID:   testBae5,
				Type: docTypeTransaction,
				Data: map[string]any{},
				Metadata: DocumentMetadata{
					ProcessingDepth:  1,
					ViewID:           testViewA,
					IsViewOutput:     true,
					ProcessingChain:  testViewA,
					SourceCollection: docTypeTransaction,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBae5,
				},
			},
			viewID: testViewB,
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
		outputData map[string]any
		expected   PrimitiveDocument
	}{
		{
			name: "creates output from source doc with empty chain",
			sourceDoc: PrimitiveDocument{
				ID:          testBaeSrc1,
				Type:        docTypeTransaction,
				BlockNumber: 42,
				Data:        map[string]any{gqlFieldFrom: "0x1"},
				Metadata: DocumentMetadata{
					SourceCollection: docTypeTransaction,
					ProcessingDepth:  0,
					ProcessingChain:  "",
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBaeSrc1,
				},
			},
			viewID:     testWASMViewID,
			outputData: map[string]any{testJSONFieldResult: "processed"},
			expected: PrimitiveDocument{
				ID:          "src1-myview",
				Type:        docTypeTransaction,
				BlockNumber: 42,
				Data:        map[string]any{testJSONFieldResult: "processed"},
				Metadata: DocumentMetadata{
					SourceCollection: docTypeTransaction,
					IsViewOutput:     true,
					ViewID:           testWASMViewID,
					ProcessingDepth:  1,
					ProcessingChain:  testWASMViewID,
					DocumentType:     docTypeTransaction,
					OriginalDocID:    testBaeSrc1,
				},
			},
		},
		{
			name: "creates output from source doc with existing chain",
			sourceDoc: PrimitiveDocument{
				ID:          "bae-src2",
				Type:        docTypeLog,
				BlockNumber: 99,
				Data:        map[string]any{gqlFieldAddress: "0xabc"},
				Metadata: DocumentMetadata{
					SourceCollection: docTypeLog,
					IsViewOutput:     true,
					ViewID:           testViewA,
					ProcessingDepth:  1,
					ProcessingChain:  testViewA,
					DocumentType:     docTypeLog,
					OriginalDocID:    testBaeOrig,
				},
			},
			viewID:     testViewB,
			outputData: map[string]any{"enriched": true},
			expected: PrimitiveDocument{
				ID:          "src2-view-B",
				Type:        docTypeLog,
				BlockNumber: 99,
				Data:        map[string]any{"enriched": true},
				Metadata: DocumentMetadata{
					SourceCollection: docTypeLog,
					IsViewOutput:     true,
					ViewID:           testViewB,
					ProcessingDepth:  2,
					ProcessingChain:  testViewAB,
					DocumentType:     docTypeLog,
					OriginalDocID:    testBaeOrig,
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
		sourceData     map[string]any
		outputData     map[string]any
		requiredFields []string
		expected       map[string]any
	}{
		{
			name:           "preserves missing required fields from source",
			sourceData:     map[string]any{gqlFieldFrom: "0x1", gqlFieldTo: "0x2", gqlFieldHash: "0xabc"},
			outputData:     map[string]any{testJSONFieldResult: testDone},
			requiredFields: []string{gqlFieldFrom, gqlFieldTo},
			expected:       map[string]any{testJSONFieldResult: testDone, gqlFieldFrom: "0x1", gqlFieldTo: "0x2"},
		},
		{
			name:           "does not overwrite existing output fields",
			sourceData:     map[string]any{gqlFieldFrom: "0x1", gqlFieldTo: "0x2"},
			outputData:     map[string]any{gqlFieldFrom: "0xoverride", testJSONFieldResult: testDone},
			requiredFields: []string{gqlFieldFrom, gqlFieldTo},
			expected:       map[string]any{gqlFieldFrom: "0xoverride", testJSONFieldResult: testDone, gqlFieldTo: "0x2"},
		},
		{
			name:           "handles field missing from both source and output",
			sourceData:     map[string]any{gqlFieldFrom: "0x1"},
			outputData:     map[string]any{testJSONFieldResult: testDone},
			requiredFields: []string{gqlFieldFrom, gqlFieldTo, gqlFieldHash},
			expected:       map[string]any{testJSONFieldResult: testDone, gqlFieldFrom: "0x1"},
		},
		{
			name:           "empty required fields returns copy of output",
			sourceData:     map[string]any{gqlFieldFrom: "0x1"},
			outputData:     map[string]any{testJSONFieldResult: testDone},
			requiredFields: []string{},
			expected:       map[string]any{testJSONFieldResult: testDone},
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
			docType:        constants.CollectionTransaction,
			expectedFields: []string{gqlFieldFrom, gqlFieldTo, gqlFieldHash, gqlFieldBlockNumber},
		},
		{
			name:           "Ethereum Block",
			docType:        constants.CollectionBlock,
			expectedFields: []string{gqlFieldHash, gqlFieldNumber},
		},
		{
			name:           "Ethereum Log",
			docType:        constants.CollectionLog,
			expectedFields: []string{gqlFieldAddress, gqlFieldTopics, gqlFieldBlockNumber},
		},
		{
			name:           "Ethereum AccessListEntry",
			docType:        constants.CollectionAccessListEntry,
			expectedFields: []string{gqlFieldAddress, gqlFieldStorageKeys},
		},
		{
			name:           "short Transaction",
			docType:        docTypeTransaction,
			expectedFields: []string{gqlFieldFrom, gqlFieldTo, gqlFieldHash, gqlFieldBlockNumber},
		},
		{
			name:           "short Block",
			docType:        docTypeBlock,
			expectedFields: []string{gqlFieldHash, gqlFieldNumber},
		},
		{
			name:           "short Log",
			docType:        docTypeLog,
			expectedFields: []string{gqlFieldAddress, gqlFieldTopics, gqlFieldBlockNumber},
		},
		{
			name:           "short AccessListEntry",
			docType:        docTypeAccessListEntry,
			expectedFields: []string{gqlFieldAddress, gqlFieldStorageKeys},
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
				ID:   testBae1,
				Type: docTypeTransaction,
				Data: map[string]any{gqlFieldFrom: "0xabc", gqlFieldTo: "0xdef"},
			},
			viewID:      "view-1",
			expectError: false,
		},
		{
			name: "valid block with int field",
			doc: PrimitiveDocument{
				ID:   testBae2,
				Type: docTypeBlock,
				Data: map[string]any{gqlFieldNumber: int(42)},
			},
			viewID:      "view-2",
			expectError: false,
		},
		{
			name: "valid block with uint64 field",
			doc: PrimitiveDocument{
				ID:   testBae3,
				Type: docTypeBlock,
				Data: map[string]any{gqlFieldNumber: uint64(100)},
			},
			viewID:      "view-3",
			expectError: false,
		},
		{
			name: "missing all required fields",
			doc: PrimitiveDocument{
				ID:   testBae4,
				Type: docTypeTransaction,
				Data: map[string]any{"unrelated": testValue},
			},
			viewID:      "view-4",
			expectError: true,
		},
		{
			name: "empty string field does not satisfy requirement",
			doc: PrimitiveDocument{
				ID:   testBae5,
				Type: docTypeTransaction,
				Data: map[string]any{gqlFieldFrom: "", gqlFieldTo: ""},
			},
			viewID:      "view-5",
			expectError: true,
		},
		{
			name: "nil value does not satisfy requirement",
			doc: PrimitiveDocument{
				ID:   "bae-6",
				Type: docTypeTransaction,
				Data: map[string]any{gqlFieldFrom: nil},
			},
			viewID:      "view-6",
			expectError: true,
		},
		{
			name: "unknown type falls back to default required fields",
			doc: PrimitiveDocument{
				ID:   "bae-7",
				Type: "UnknownType",
				Data: map[string]any{gqlFieldAddress: "0x123"},
			},
			viewID:      "view-7",
			expectError: false,
		},
		{
			name: "unknown type with no matching fields errors",
			doc: PrimitiveDocument{
				ID:   "bae-8",
				Type: "UnknownType",
				Data: map[string]any{"unrelated": testValue},
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
			viewID:   testWASMViewID,
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
		expected map[string]any
	}{
		{
			name: "zero depth source document",
			doc: PrimitiveDocument{
				ID:   testBae1,
				Type: docTypeTransaction,
				Metadata: DocumentMetadata{
					ProcessingDepth:  0,
					IsViewOutput:     false,
					SourceCollection: docTypeTransaction,
					ViewID:           "",
					ProcessingChain:  "",
					OriginalDocID:    testBae1,
				},
			},
			expected: map[string]any{
				provenanceFieldProcessingDepth: 0,
				provenanceFieldIsViewOutput:    false,
				provenanceFieldSourceColl:      docTypeTransaction,
				provenanceFieldViewID:          "",
				provenanceFieldProcessingChain: "",
				provenanceFieldChainLength:     1, // splitting empty string gives [""]
				provenanceFieldOriginalDocID:   testBae1,
			},
		},
		{
			name: "multi-depth view output document",
			doc: PrimitiveDocument{
				ID:   "derived-1",
				Type: docTypeTransaction,
				Metadata: DocumentMetadata{
					ProcessingDepth:  3,
					IsViewOutput:     true,
					SourceCollection: docTypeTransaction,
					ViewID:           "view-C",
					ProcessingChain:  "view-A,view-B,view-C",
					OriginalDocID:    testBaeOrig,
				},
			},
			expected: map[string]any{
				provenanceFieldProcessingDepth: 3,
				provenanceFieldIsViewOutput:    true,
				provenanceFieldSourceColl:      docTypeTransaction,
				provenanceFieldViewID:          "view-C",
				provenanceFieldProcessingChain: "view-A,view-B,view-C",
				provenanceFieldChainLength:     len(strings.Split("view-A,view-B,view-C", ",")),
				provenanceFieldOriginalDocID:   testBaeOrig,
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
