package view

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestBasicViewNoLens tests that a basic DefraDB view works without any lens
func TestBasicViewNoLens(t *testing.T) {
	ctx := context.Background()
	defraNode := startTestDefraNode(t, ctx)
	defer defraNode.Close(ctx)

	// Add schema
	_, err := defraNode.DB.AddSchema(ctx, `type Item { name: String, value: Int }`)
	require.NoError(t, err)

	// Insert a document
	mut := `mutation { create_Item(input: { name: "test", value: 42 }) { _docID } }`
	type R struct{ DocID string `json:"_docID"` }
	r, err := defra.PostMutation[R](ctx, defraNode, mut)
	require.NoError(t, err)
	t.Logf("Created doc: %s", r.DocID)

	// Create view directly via AddView - NO lens
	viewSDL := `type ItemView @materialized(if: true) { name: String, value: Int }`
	cvs, err := defraNode.DB.AddView(ctx, "Item { name value }", viewSDL)
	require.NoError(t, err)
	t.Logf("AddView returned %d collection versions", len(cvs))
	for i, cv := range cvs {
		t.Logf("  cv[%d]: VersionID=%s, Name=%s, CollectionID=%s", i, cv.VersionID, cv.Name, cv.CollectionID)
	}

	// Query the view
	results, err := defra.QueryArray[map[string]interface{}](ctx, defraNode, `query { ItemView { name value } }`)
	require.NoError(t, err)
	t.Logf("Results: %d", len(results))
	for i, doc := range results {
		t.Logf("  doc[%d]: name=%v, value=%v", i, doc["name"], doc["value"])
	}
	require.NotEmpty(t, results)
	require.Equal(t, "test", results[0]["name"])
	require.Equal(t, float64(42), results[0]["value"])
	t.Log("Basic view works!")
}

// TestUSDCEventDecoding tests the decode_event WASM lens with USDC contract events
func TestUSDCEventDecoding(t *testing.T) {
	ctx := context.Background()

	// Start DefraDB
	defraNode := startTestDefraNode(t, ctx)
	defer defraNode.Close(ctx)

	// Add Ethereum Log schema
	logSchema := `type Ethereum__Mainnet__Log {
    address: String
    topics: [String]
    data: String
    transactionHash: String
    blockHash: String
    blockNumber: Int
    transactionIndex: Int
    logIndex: Int
    removed: String
}
	`
	_, err := defraNode.DB.AddSchema(ctx, logSchema)
	require.NoError(t, err)
	t.Log("Schema added")

	// Insert a real USDC Transfer event log
	// Transfer(address,address,uint256) => keccak256 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
	// topic[0] = signature hash
	// topic[1] = from (indexed address, 32-byte padded)
	// topic[2] = to   (indexed address, 32-byte padded)
	// data     = value (non-indexed uint256) = 25000000 (0x17d7840) = 25 USDC
	mutation := `
		mutation {
			create_Ethereum__Mainnet__Log(input: {
				address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
				topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000003d93f3d4d59cf7944b46fa1f6fbd3ad378febb89","0x0000000000000000000000000b14cfb3e84fab09c6e2b24af64a60aba7d2d14f"]
				data: "0x00000000000000000000000000000000000000000000000000000000017d7840"
				transactionHash: "0xc5cea56344c881aa5c4030e62b827b87d0bcd097f33b1c9824cb88db8c259289"
				blockNumber: 24630257

			}) {
				_docID
			}
		}
	`
	type CreateResult struct {
		DocID string `json:"_docID"`
	}
	result, err := defra.PostMutation[CreateResult](ctx, defraNode, mutation)
	require.NoError(t, err)
	require.NotEmpty(t, result.DocID)
	t.Logf("Log document created: %s", result.DocID)

	// Read the real decode_event WASM
	wasmBytes, err := os.ReadFile("./lens/decode_event.wasm")
	require.NoError(t, err)
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmBytes)
	t.Logf("WASM loaded: %d bytes", len(wasmBytes))

	// Minimal ABI with just the Transfer event (full ABI is too large for WASM params)
	transferABI := `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`
	argument := fmt.Sprintf(`{"abi": %s}`, transferABI)

	// Create view name
	viewName := fmt.Sprintf("USDCEvents%d", time.Now().UnixNano()%10000)

	sdl := fmt.Sprintf(`type %s @materialized(if: true) {
		address: String
		topics: [String]
		data: String
		transactionHash: String
		blockHash: String
		blockNumber: Int
		transactionIndex: Int
		logIndex: Int
		removed: String
	}`, viewName)

	// Create view with the real WASM and ABI
	view := View{
		Name: viewName,
		Data: viewbundle.View{
			Query: "Ethereum__Mainnet__Log { address topics data transactionHash blockNumber }",
			Sdl:   sdl,
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{
						Path:      wasmBase64,
						Arguments: argument,
					},
				},
			},
		},
	}

	// Register view with ViewManager
	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)
	require.NotNil(t, vm)

	t.Logf("View name: %s", viewName)
	t.Logf("Lens count: %d", len(view.Data.Transform.Lenses))

	err = vm.RegisterView(ctx, view)
	require.NoError(t, err)
	t.Log("View registered")

	// Wait for the lens to process
	time.Sleep(2 * time.Second)

	// Query the decoded events
	query := fmt.Sprintf(`query { %s { address topics data transactionHash blockNumber } }`, viewName)
	t.Logf("Query: %s", query)
	results, err := defra.QueryArray[map[string]interface{}](ctx, defraNode, query)
	require.NoError(t, err)

	t.Logf("Results: %d documents", len(results))
	require.NotEmpty(t, results, "Should have decoded event results")

	r := results[0]
	t.Logf("  address: %v", r["address"])
	t.Logf("  topics:  %v", r["topics"])
	t.Logf("  data:    %v", r["data"])
	t.Logf("  transactionHash:  %v", r["transactionHash"])
	t.Logf("  blockNumber:   %v", r["blockNumber"])

	// Verify topics[0] = decoded signature
	topics := r["topics"].([]interface{})
	require.Equal(t, "Transfer(address,address,uint256)", topics[0])
	t.Log("topics[0] = Transfer signature")

	// Verify data = decoded args string
	data := r["data"].(string)
	require.Contains(t, data, "from:")
	require.Contains(t, data, "to:")
	require.Contains(t, data, "value:")
	t.Logf("data = %s", data)
}
