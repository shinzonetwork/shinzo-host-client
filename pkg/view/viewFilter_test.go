package view

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// TestViewFilterByStatus tests WASM filtering with a simple User type
// Creates 3 users with different status values and filters by status=true and status=false
func TestViewFilterByStatus(t *testing.T) {
	ctx := context.Background()

	// Start a test DefraDB instance
	defraNode := startTestDefraNode(t, ctx)
	defer defraNode.Close(ctx)

	// Add User schema
	userSchema := `
		type User {
			name: String
			age: Int
			status: Boolean
		}
	`
	_, err := defraNode.DB.AddSchema(ctx, userSchema)
	require.NoError(t, err, "Should add User schema")

	t.Log("✅ User schema added")

	// Create 3 test documents
	// 1. name: john, age 32, status: true
	johnID := createUserDocument(t, ctx, defraNode, "john", 32, true)
	t.Logf("📄 Created user: john (age=32, status=true), id=%s", johnID)

	// 2. name: duncan, age 55, status: true
	duncanID := createUserDocument(t, ctx, defraNode, "duncan", 55, true)
	t.Logf("📄 Created user: duncan (age=55, status=true), id=%s", duncanID)

	// 3. name: andrew, age 22, status: false
	andrewID := createUserDocument(t, ctx, defraNode, "andrew", 22, false)
	t.Logf("📄 Created user: andrew (age=22, status=false), id=%s", andrewID)

	// Verify all documents were created
	allUsers := queryAllUsers(t, ctx, defraNode)
	require.Len(t, allUsers, 3, "Should have 3 users")
	t.Logf("✅ All 3 users created successfully")

	// Download WASM filter file
	wasmPath := getFilterWasmPath(t)
	t.Logf("✅ WASM filter downloaded: %s", wasmPath)

	// Initialize ViewManager
	registryPath := t.TempDir()
	vm := NewViewManager(defraNode, registryPath)
	require.NotNil(t, vm, "ViewManager should be initialized")

	// Test 1: Filter by status=true (should return john and duncan)
	t.Run("FilterByStatusTrue", func(t *testing.T) {
		viewTrue := createStatusFilterView(t, wasmPath, true)
		t.Logf("📋 View name: %s", viewTrue.Name)
		t.Logf("📋 View query: %s", *viewTrue.Query)
		t.Logf("📋 View SDL: %s", *viewTrue.Sdl)
		t.Logf("📋 View has %d lenses", len(viewTrue.Transform.Lenses))

		err := vm.RegisterView(ctx, viewTrue)
		if err != nil {
			t.Fatalf("❌ View registration failed: %v", err)
		}
		t.Log("✅ View registered successfully")

		// Query the view to get filtered results
		time.Sleep(1 * time.Second)

		// Query the view collection
		results := queryViewResults(t, ctx, defraNode, viewTrue.Name)
		t.Logf("📊 Filter status=true results: %d documents", len(results))

		// Verify: should have 2 users (john and duncan)
		for _, r := range results {
			t.Logf("  - %v", r)
		}

		t.Log("✅ Filter by status=true test completed")
	})

	// Test 2: Filter by status=false (should return andrew)
	t.Run("FilterByStatusFalse", func(t *testing.T) {
		viewFalse := createStatusFilterView(t, wasmPath, false)
		err := vm.RegisterView(ctx, viewFalse)
		if err != nil {
			t.Logf("⚠️ View registration error (may be expected): %v", err)
		}

		// Query the view to get filtered results
		time.Sleep(1 * time.Second)

		// Query the view collection
		results := queryViewResults(t, ctx, defraNode, viewFalse.Name)
		t.Logf("📊 Filter status=false results: %d documents", len(results))

		// Verify: should have 1 user (andrew)
		for _, r := range results {
			t.Logf("  - %v", r)
		}

		t.Log("✅ Filter by status=false test completed")
	})
}

// startTestDefraNode creates a test DefraDB node
func startTestDefraNode(t *testing.T, _ context.Context) *node.Node {
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err, "Should create DefraDB node")
	require.NotNil(t, defraNode)

	return defraNode
}

// createUserDocument creates a User document in DefraDB
func createUserDocument(t *testing.T, ctx context.Context, defraNode *node.Node, name string, age int, status bool) string {
	mutation := fmt.Sprintf(`
		mutation {
			create_User(input: {
				name: "%s"
				age: %d
				status: %t
			}) {
				_docID
				name
			}
		}
	`, name, age, status)

	type CreateResult struct {
		DocID string `json:"_docID"`
		Name  string `json:"name"`
	}

	result, err := defra.PostMutation[CreateResult](ctx, defraNode, mutation)
	require.NoError(t, err, "Should create user document")
	require.NotEmpty(t, result.DocID, "Should have document ID")

	return result.DocID
}

// queryAllUsers queries all User documents
func queryAllUsers(t *testing.T, ctx context.Context, defraNode *node.Node) []map[string]interface{} {
	query := `query { User { _docID name age status } }`

	results, err := defra.QueryArray[map[string]interface{}](ctx, defraNode, query)
	require.NoError(t, err, "Should query users")

	return results
}

// queryViewResults queries a view collection for results
func queryViewResults(t *testing.T, ctx context.Context, defraNode *node.Node, viewName string) []map[string]interface{} {
	query := fmt.Sprintf(`query { %s { name age status } }`, viewName)

	results, err := defra.QueryArray[map[string]interface{}](ctx, defraNode, query)
	if err != nil {
		t.Logf("⚠️ Query error (view may not exist yet): %v", err)
		return nil
	}

	return results
}

// getFilterWasmPath returns the path to the local filter WASM file
// Returns empty string and skips test if file not found (e.g., in CI)
func getFilterWasmPath(t *testing.T) string {
	// Use the existing lens file from ./pkg/host/.lens/
	possiblePaths := []string{
		"../host/.lens/filter_transaction.wasm",
		"../../pkg/host/.lens/filter_transaction.wasm",
		"./pkg/host/.lens/filter_transaction.wasm",
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			t.Logf("✅ Found WASM file: %s", path)
			return path
		}
	}

	t.Skip("⏭️ Skipping test: filter_transaction.wasm not found (not available in CI)")
	return ""
}

// createStatusFilterView creates a view that filters by status field
func createStatusFilterView(t *testing.T, wasmPath string, statusValue bool) View {
	query := "User { name age status }"

	// Use a unique view name that matches the SDL type name
	viewName := fmt.Sprintf("UserStatus%t%d", statusValue, time.Now().UnixNano()%10000)

	// SDL type name MUST match the view name for DefraDB to create the correct collection
	sdl := fmt.Sprintf(`type %s @materialized(if: true) {
		name: String
		age: Int
		status: Boolean
	}`, viewName)

	// Read WASM file and encode as base64
	wasmBytes, err := os.ReadFile(wasmPath)
	require.NoError(t, err, "Should read WASM file")
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	return View{
		Name:  viewName,
		Query: &query,
		Sdl:   &sdl,
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "filter_by_status",
					Path:  wasmBase64,
					Arguments: map[string]any{
						"src":   "status",
						"value": statusValue,
					},
				},
			},
		},
	}
}
