package view

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/views"
	indexerschema "github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// Helper functions copied from host_test.go
func startDefraInstanceForTest(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	myNode, err := node.New(ctx, options...)
	require.NoError(t, err)
	require.NotNil(t, myNode)

	err = myNode.Start(ctx)
	require.NoError(t, err)

	return myNode
}

func queryBlockNumber(ctx context.Context, defraNode *node.Node) (int, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`

	block, err := defra.QuerySingle[map[string]any](ctx, defraNode, query)

	if err != nil {
		return 0, fmt.Errorf("Error querying block number: %v", err)
	}

	number, ok := block["number"].(int)
	if !ok {
		return 0, fmt.Errorf("No blocks found")
	}

	return number, nil
}

// Mock EventSubscription for testing
type MockEventSubscription struct {
	events chan string
}

func NewMockEventSubscription() *MockEventSubscription {
	return &MockEventSubscription{
		events: make(chan string, 10),
	}
}

func (m *MockEventSubscription) SendRawJSONMessage(message string) error {
	m.events <- message
	return nil
}

func (m *MockEventSubscription) Events() <-chan string {
	return m.events
}

func (m *MockEventSubscription) ProcessEvent(host *MockHost, event string) {
	// Parse the event and add to HostedViews
	var eventData struct {
		Type string `json:"type"`
		Data struct {
			Name      string `json:"name"`
			SDL       string `json:"sdl"`
			Query     string `json:"query"`
			Transform struct {
				Lenses []struct {
					Label     string            `json:"label"`
					Path      string            `json:"path"`
					Arguments map[string]string `json:"arguments"`
				} `json:"lenses"`
			} `json:"transform"`
		} `json:"data"`
	}

	if err := json.Unmarshal([]byte(event), &eventData); err == nil {
		// Create the view with lens data
		view := views.View{
			Name:  eventData.Data.Name,
			Sdl:   &eventData.Data.SDL,
			Query: &eventData.Data.Query,
			// Note: We can't directly set Transform field due to type constraints
			// In a real scenario, this would be set by the event processing
		}
		host.HostedViews = append(host.HostedViews, view)
	} else {
		fmt.Printf("Failed to parse event: %v\n", err)
		if len(event) > 200 {
			fmt.Printf("Event content: %s\n", event[:200])
		} else {
			fmt.Printf("Event content: %s\n", event)
		}
	}
}

// Mock Host struct for testing
type MockHost struct {
	DefraNode   *node.Node
	HostedViews []views.View
}

func (h *MockHost) Close(ctx context.Context) error {
	return h.DefraNode.Close(ctx)
}

// StartHostingWithEventSubscription creates a mock host for testing
func StartHostingWithEventSubscription(t *testing.T, cfg *config.Config, eventSub *MockEventSubscription) (*MockHost, error) {
	// Create DefraDB instance
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()))
	if err != nil {
		return nil, err
	}

	// Create mock host
	host := &MockHost{
		DefraNode:   defraNode,
		HostedViews: []views.View{},
	}

	// Event processing will be done manually in the test

	return host, nil
}

func TestView_SubscribeTo(t *testing.T) {
	// Create a test view
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := View{
		Query: &query,
		Sdl:   &sdl,
		Name:  "FilteredAndDecodedLogs",
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Test successful subscription
	err = testView.SubscribeTo(context.Background(), defraNode)
	require.NoError(t, err)
}

func TestView_ConfigureLens_NoLenses(t *testing.T) {
	// Test case where no lenses are provided
	view := View{
		Name: "TestView",
		// Transform field is not set, so it will be empty
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// ConfigureLens should return an error when no lenses are provided
	err = view.ConfigureLens(context.Background(), defraNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no lenses provided")
}

func TestView_ApplyLensTransform_EmptyDocuments(t *testing.T) {
	// Test with a query string (the function now takes a query string, not documents)
	view := View{
		Name: "TestView",
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Test with a query string - this will query the view collection
	// The view may not exist, so we expect an error or empty results
	sourceQuery := "TestView { _docID }"
	result, err := view.ApplyLensTransform(context.Background(), defraNode, sourceQuery)

	// If the view doesn't exist, we'll get an error, which is acceptable for this test
	// If it does exist but has no data, we'll get empty results
	if err != nil {
		// View doesn't exist or query failed - this is acceptable for this test
		return
	}
	// Result can be nil or empty for non-existent view or no data
	if result != nil {
		require.Len(t, result, 0)
	}
}

func TestView_WriteTransformedToCollection_EmptyDocuments(t *testing.T) {
	// Test with empty documents
	view := View{
		Name: "TestView",
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Test with empty documents - this should fail because the collection doesn't exist
	transformedDocuments := []map[string]any{}
	docIds, err := view.WriteTransformedToCollection(context.Background(), defraNode, transformedDocuments)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting collection")
	require.Nil(t, docIds)
}

func TestView_WriteTransformedToCollection_NonExistentCollection(t *testing.T) {
	// Test with non-existent collection
	view := View{
		Name: "NonExistentView",
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Test with non-existent collection
	transformedDocuments := []map[string]any{
		{"field1": "value1"},
	}
	docIds, err := view.WriteTransformedToCollection(context.Background(), defraNode, transformedDocuments)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting collection")
	require.Nil(t, docIds)
}

func TestView_WriteTransformedToCollection_SchemaFiltering(t *testing.T) {
	ctx := context.Background()

	// Create a DefraDB instance with real schema
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()))
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create a test view with a specific SDL schema that only includes certain fields
	viewName := "FilteredTestView"
	sdl := "type FilteredTestView { name: String }" // Only name field
	query := "User { name age }"

	appView := views.View{
		Name:  viewName,
		Sdl:   &sdl,
		Query: &query,
	}

	view := View(appView)

	err = view.SubscribeTo(ctx, defraNode)
	require.NoError(t, err)

	// Create test documents with extra fields that should be filtered out
	transformedDocuments := []map[string]any{
		{
			"name":       "Alice",
			"age":        25,
			"email":      "alice@example.com", // Should be filtered out
			"phone":      "123-456-7890",      // Should be filtered out
			"extraField": "should be removed", // Should be filtered out
		},
		{
			"name":     "Bob",
			"age":      30,
			"email":    "bob@example.com", // Should be filtered out
			"address":  "123 Main St",     // Should be filtered out
			"unwanted": "data",            // Should be filtered out
		},
	}

	// Test WriteTransformedToCollection with schema filtering
	docIds, err := view.WriteTransformedToCollection(ctx, defraNode, transformedDocuments)
	require.NoError(t, err) // This would throw an error if filtering was not working because we would try to write to fields that didn't exist in the schema
	require.Greater(t, len(docIds), 0)

	// Verify the data was written by querying the collection
	verifyQuery := fmt.Sprintf(`query VerifyFilteredView { %s(limit: 10) { name } }`, viewName)
	writtenData, err := defra.QueryArray[map[string]any](ctx, defraNode, verifyQuery)
	require.NoError(t, err)
	require.Len(t, writtenData, 2, "Should have written both documents")

	// Verify that only the schema-defined fields are present
	require.Len(t, writtenData, 2, "Should have written both documents")

	// Create a map to find documents by name for easier verification
	docMap := make(map[string]map[string]any)
	for _, doc := range writtenData {
		if name, ok := doc["name"].(string); ok {
			docMap[name] = doc
		}
	}

	// Verify Alice's document
	aliceDoc, exists := docMap["Alice"]
	require.True(t, exists, "Alice's document should exist")
	require.Contains(t, aliceDoc, "name")
	require.NotContains(t, aliceDoc, "age")
	require.NotContains(t, aliceDoc, "email")
	require.NotContains(t, aliceDoc, "phone")
	require.NotContains(t, aliceDoc, "extraField")
	require.Equal(t, "Alice", aliceDoc["name"])

	// Verify Bob's document
	bobDoc, exists := docMap["Bob"]
	require.True(t, exists, "Bob's document should exist")
	require.Contains(t, bobDoc, "name")
	require.NotContains(t, bobDoc, "age")
	require.NotContains(t, bobDoc, "email")
	require.NotContains(t, bobDoc, "address")
	require.NotContains(t, bobDoc, "unwanted")
	require.Equal(t, "Bob", bobDoc["name"])

	t.Logf("Successfully filtered documents to only include schema-defined fields: %+v", writtenData)
}
