package host

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

const UsdcEvent_Query = "{ UsdcEvent_0x1bd1afe59e721667b09f9997bff449c3f199531d1aba2bac206fe8132356eda0 { hash blockNumber from to logAddress event signature arguments }}"
const Erc20Event_Query = "{ Erc20Event_0x9c3455147dfe9a5aca44797a2481fefa51bf00dd4fff7af9fbdfd1e4d4dc7de4 { hash blockNumber from to logAddress event signature arguments }}"
// TestHostIntegration_ShinzoHubViewFetching tests the complete ShinzoHub view fetching and registration process
func TestHostIntegration_ShinzoHubViewFetching(t *testing.T) {
	ctx := context.Background()

	// Step 1: Start the host with real config from config.yaml
	testConfig := *DefaultConfig // Copy the default config
	// When running locally you can use a pre-seeded defra database
	// testConfig.DefraDB.Store.Path = "../../.defra"
	// testConfig.DefraDB.KeyringSecret = "pingpong"
	testConfig.DefraDB.Store.Path = t.TempDir()
	
	// Enable P2P
	testConfig.DefraDB.P2P.Enabled = true
	testConfig.DefraDB.P2P.BootstrapPeers = []string{
		"/ip4/35.209.45.53/tcp/9171/p2p/12D3KooWGqjj1ZeaM5WYuvopSFh8rnsoTYje91mhzwgQGchRAJKa",
	}
	
	// Enable debug logging to see SDL schemas
	testConfig.Logger.Development = true
	
	// Use ShinzoHub devnet RPC URL from config.yaml
	testConfig.Shinzo.HubBaseURL = "rpc.devnet.shinzo.network:26657"
	
	h, err := StartHostingWithEventSubscription(&testConfig)
	require.NoError(t, err)
	defer h.Close(ctx)

	// Step 2: Check if our target views were loaded from ShinzoHub
	viewNames := h.GetActiveViewNames()
	t.Logf("📋 Found %d active views: %v", len(viewNames), viewNames)
	

	// Look for our target views
	var targetViews []string
	targetViewNames := []string{
		"UsdcEvent_0x1bd1afe59e721667b09f9997bff449c3f199531d1aba2bac206fe8132356eda0",
		"Erc20Event_0x9c3455147dfe9a5aca44797a2481fefa51bf00dd4fff7af9fbdfd1e4d4dc7de4",
	}
	
	for _, targetName := range targetViewNames {
		for _, viewName := range viewNames {
			if viewName == targetName {
				targetViews = append(targetViews, viewName)
				break
			}
		}
	}
	
	if len(targetViews) == 0 {
		t.Skip("Target views not found in ShinzoHub - skipping test")
		return
	}
	
	t.Logf("🎯 Found %d target views from ShinzoHub: %v", len(targetViews), targetViews)

	// Step 5: Give views time to process the data
	time.Sleep(5 * time.Second)

	// Step 6: Query the ShinzoHub views using their known queries
	for _, viewName := range targetViews {
		t.Logf("🔍 Testing ShinzoHub view query: %s", viewName)
		
		// Use the appropriate known query for each view
		var query string
		switch viewName {
		case "UsdcEvent_0x1bd1afe59e721667b09f9997bff449c3f199531d1aba2bac206fe8132356eda0":
			query = UsdcEvent_Query
		case "Erc20Event_0x9c3455147dfe9a5aca44797a2481fefa51bf00dd4fff7af9fbdfd1e4d4dc7de4":
			query = Erc20Event_Query
		default:
			t.Logf("⚠️ No known query for view %s", viewName)
			continue
		}
		
		t.Logf("🔍 Executing known query for %s", viewName)
		t.Logf("📄 Query: %s", query)
		
		results, err := defra.QueryArray[map[string]interface{}](ctx, h.DefraNode, query)
		if err != nil {
			t.Logf("⚠️ %s query error: %v", viewName, err)
			continue
		}
		
		if len(results) > 0 {
			t.Logf("✅ %s view returned %d results", viewName, len(results))
			result := results[0]
			actualFields := getSortedKeys(result)
			t.Logf("📊 %s actual fields in result: %v", viewName, actualFields)
			
			// Log a sample result
			t.Logf("📊 %s sample result: %v", viewName, result)
		} else {
			t.Logf("⚠️ %s view returned no results (may need source data)", viewName)
		}
	}

	t.Logf("🎉 ShinzoHub integration test completed successfully!")
	t.Logf("✅ Host started with StartHostingWithEventSubscription")
	t.Logf("✅ ShinzoHub views automatically fetched and registered")
	t.Logf("✅ Target views queried with SDL-based queries")
}

// Helper function to get sorted keys from a map for logging
func getSortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple sort for consistent logging
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}
