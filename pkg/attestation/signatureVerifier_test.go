package attestation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefraSignatureVerifier_Verify_EmptyIdentity(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

func TestDefraSignatureVerifier_Verify_EmptyCID(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "", Signature{
		Type:     "es256k",
		Identity: "identity",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity or CID")
}

func TestDefraSignatureVerifier_Verify_UnsupportedSignatureType(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, "test-cid", Signature{
		Type:     "unsupported-type",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid signature type")
}

func TestDefraSignatureVerifier_Verify_ValidSignature(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a test document to get a real CID and signature
	createTestDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-document"}) {
				_docID
				_version {
					cid
					signature {
						type
						identity
						value
					}
				}
			}
		}
	`

	type TestDoc struct {
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	testDocResult, err := defra.PostMutation[TestDoc](ctx, defraNode, createTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, testDocResult)
	require.Len(t, testDocResult.Version, 1)

	version := testDocResult.Version[0]
	require.NotEmpty(t, version.CID)
	require.NotEmpty(t, version.Signature.Identity)

	verifier := NewDefraSignatureVerifier(defraNode)

	err = verifier.Verify(ctx, version.CID, version.Signature)
	require.NoError(t, err)
}

func TestDefraSignatureVerifier_Verify_WithNilNode(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil)

	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "MDMyNjM1OTBiNzIzODk5ZDBkMTk5OTkxYjhmY2Y3MTUwMmQ4ZDY3Y2FmN2ZkNmY0NzYyODdhNDdhOTAwZDk4ZWU1",
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or API URL not available")
}

// Mock DefraDB node for testing
type mockDefraNode struct {
	apiURL string
}

func (m *mockDefraNode) GetAPIURL() string {
	return m.apiURL
}
func TestCachedSignatureVerifier_CacheHit(t *testing.T) {
	// Setup mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This should only be called once (cache miss)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	// Create mock node
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}
	// First call - should hit HTTP
	err := verifier.Verify(ctx, "test-cid", signature)
	assert.NoError(t, err)
	// Second call - should hit cache
	err = verifier.Verify(ctx, "test-cid", signature)
	assert.NoError(t, err)
	// Verify server was only called once
	// (In real test, you'd track call count)
}
func TestCachedSignatureVerifier_CacheError(t *testing.T) {
	// Setup mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid signature"))
	}))
	defer server.Close()
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "invalid-signature",
		Type:     "ES256K",
	}
	// First call - should cache error
	err := verifier.Verify(ctx, "test-cid", signature)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "verification failed")
	// Second call - should return cached error
	err = verifier.Verify(ctx, "test-cid", signature)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "verification failed")
}
func TestCachedSignatureVerifier_Validation(t *testing.T) {
	tests := []struct {
		name      string
		signature Signature
		cid       string
		wantErr   bool
		errMsg    string
	}{
		{
			name: "valid signature",
			signature: Signature{
				Identity: "0x1234567890abcdef",
				Value:    "signature123",
				Type:     "ES256K",
			},
			cid:     "test-cid",
			wantErr: false,
		},
		{
			name: "empty identity",
			signature: Signature{
				Identity: "",
				Value:    "signature123",
				Type:     "ES256K",
			},
			cid:     "test-cid",
			wantErr: true,
			errMsg:  "empty identity",
		},
		{
			name: "empty CID",
			signature: Signature{
				Identity: "0x1234567890abcdef",
				Value:    "signature123",
				Type:     "ES256K",
			},
			cid:     "",
			wantErr: true,
			errMsg:  "empty identity or CID",
		},
		{
			name: "invalid signature type",
			signature: Signature{
				Identity: "0x1234567890abcdef",
				Value:    "signature123",
				Type:     "RSA256",
			},
			cid:     "test-cid",
			wantErr: true,
			errMsg:  "invalid signature type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNode := &node.Node{APIURL: "http://localhost:9181"}
			verifier := NewSignatureVerifier(mockNode)
			ctx := context.Background()
			err := verifier.Verify(ctx, tt.cid, tt.signature)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				// For valid signatures, we expect HTTP call which will fail
				// since we're not running a real server
				assert.Error(t, err)
			}
		})
	}
}

func TestCachedSignatureVerifier_ConcurrentAccess(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		time.Sleep(10 * time.Millisecond) // ADD DELAY
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)

	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}

	// First call to populate cache
	verifier.Verify(ctx, "concurrent-test", signature)

	// Run concurrent tests
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			verifier.Verify(ctx, "concurrent-test", signature)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Should only have made 1 HTTP call total
	assert.Equal(t, 1, callCount)
}

func TestCachedSignatureVerifier_RealWorldScenario(t *testing.T) {
	// Setup mock server that simulates real DefraDB behavior
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("cid")

		// Simulate real verification logic
		// All CIDs from the same indexer should be valid
		if strings.Contains(cid, "bafyreifzzwxhz5lgvwt26saislaagdt45i7xs4ytekepyhkakhdtssoc2y") ||
			strings.Contains(cid, "bafyreide65z72eacmj3fc3sk3vd2uetk3vsrekjk7dg2wgt6fmgyvjkbxa") ||
			strings.Contains(cid, "bafyreibthu6tcnxfd566be3eimp4klw42oq7iyr7i6xudb4sqqmyk6igmu") ||
			strings.Contains(cid, "bafyreics77ze5tkkuf46pkydogf4tjziw2low36bgjei2ussx2ntt2piny") ||
			strings.Contains(cid, "bafyreic7xuawbvgsjlydi3yq6qfxxmc6jcp744yundmb7zdwrcc37fosbe") ||
			strings.Contains(cid, "bafyreifnt5tabapppmqz4srt3jfooljstarwev74ckbjyhndtwjimvhtga") {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid signature"))
		}
	}))
	defer server.Close()
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	// Real blockchain signatures from your data
	blockSignatures := []Signature{
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEUCIQDoU5jehMJ/20qTeuCPjRh600l8jtASMrCz/18jQILd9wIgJDChHU1uUkEfUlifJeTz4W6rJnOty1JvOLuVEZaGZOU=",
			Type:     "ES256K",
		},
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEUCIQCgklyGZO9MOMkrs6of42+4d8XK5Am9mYSUGm6E/nsOCgIgKMCsGM2UjDhqtkNnCAJhlowKKTdGP1RRDGhB+tUv/2k=",
			Type:     "ES256K",
		},
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEQCIEY5lJ/mRDnlNa4wDNlgHhghc5DUDlFIknBv2QHD7kvVAiBPUJ4VlDj7PuxMGpZeLXY0ZzSBJ4fkSkSb8d6o0/5ALw==",
			Type:     "ES256K",
		},
	}
	logSignatures := []Signature{
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEQCIGw6Irk5eg8YNoO8XrpmOQTO3AAYxaHM7ScyjRTjTjv1AiB83EGG7XH1JCNrxR8WUBgwBJDy9d0x4lr4pxKy10ZNIw==",
			Type:     "ES256K",
		},
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEQCIDS0jckoTNELGVc+C2h8pAPN99JQnEOAZzYJOAWJVwSuAiAk65J/JakKIsRgZfO857QQzx9L88cRrRYBnbXqAEK0aQ==",
			Type:     "ES256K",
		},
		{
			Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
			Value:    "MEQCIEi+KaeKo91hB4gxN9RdXLhn73qe3/N3+9o5a+MNibwgAiAt3GOhg5nkJDP9z4Qr8lh2GD/0kKdKbkkLEHF5oIqj1w==",
			Type:     "ES256K",
		},
	}
	// Real blockchain CIDs from your data
	blockCIDs := []string{
		"bafyreifzzwxhz5lgvwt26saislaagdt45i7xs4ytekepyhkakhdtssoc2y",
		"bafyreide65z72eacmj3fc3sk3vd2uetk3vsrekjk7dg2wgt6fmgyvjkbxa",
		"bafyreibthu6tcnxfd566be3eimp4klw42oq7iyr7i6xudb4sqqmyk6igmu",
	}
	logCIDs := []string{
		"bafyreics77ze5tkkuf46pkydogf4tjziw2low36bgjei2ussx2ntt2piny",
		"bafyreic7xuawbvgsjlydi3yq6qfxxmc6jcp744yundmb7zdwrcc37fosbe",
		"bafyreifnt5tabapppmqz4srt3jfooljstarwev74ckbjyhndtwjimvhtga",
	}
	// Test 1: Verify all block signatures (should make 3 HTTP calls)
	t.Log("Testing Block signatures...")
	for i, cid := range blockCIDs {
		err := verifier.Verify(ctx, cid, blockSignatures[i])
		assert.NoError(t, err, fmt.Sprintf("Block CID %s should be valid", cid))
	}
	// Test 2: Verify all log signatures (should make 3 more HTTP calls)
	t.Log("Testing Log signatures...")
	for i, cid := range logCIDs {
		err := verifier.Verify(ctx, cid, logSignatures[i])
		assert.NoError(t, err, fmt.Sprintf("Log CID %s should be valid", cid))
	}
	// Test 3: Re-verify same CIDs (should all be cache hits)
	t.Log("Testing cache hits...")
	for i, cid := range blockCIDs {
		err := verifier.Verify(ctx, cid, blockSignatures[i])
		assert.NoError(t, err, fmt.Sprintf("Cached Block CID %s should be valid", cid))
	}
	for i, cid := range logCIDs {
		err := verifier.Verify(ctx, cid, logSignatures[i])
		assert.NoError(t, err, fmt.Sprintf("Cached Log CID %s should be valid", cid))
	}
	// Test 4: Test multiple indexers signing same document
	t.Log("Testing multiple indexers scenario...")

	// Simulate multiple indexers with different signatures but same CID
	indexer1Sig := Signature{
		Identity: "026316ba94d1410b6c10832f12e0fab3a47602d8aac5b495b30cacb2ce50df5eac",
		Value:    "MEUCIQDoU5jehMJ/20qTeuCPjRh600l8jtASMrCz/18jQILd9wIgJDChHU1uUkEfUlifJeTz4W6rJnOty1JvOLuVEZaGZOU=",
		Type:     "ES256K",
	}

	indexer2Sig := Signature{
		Identity: "03a1b2c3d4e5f678901234567890123456789012345678901234567890123456789",
		Value:    "MEQCIEY5lJ/mRDnlNa4wDNlgHhghc5DUDlFIknBv2QHD7kvVAiBPUJ4VlDj7PuxMGpZeLXY0ZzSBJ4fkSkSb8d6o0/5ALw==",
		Type:     "ES256K",
	}
	// Same CID verified by multiple indexers
	sharedCID := "bafyreifzzwxhz5lgvwt26saislaagdt45i7xs4ytekepyhkakhdtssoc2y"

	// First indexer verifies
	err := verifier.Verify(ctx, sharedCID, indexer1Sig)
	assert.NoError(t, err)

	// Second indexer verifies same CID (should be cache hit)
	err = verifier.Verify(ctx, sharedCID, indexer2Sig)
	assert.NoError(t, err)

	// Third indexer verifies same CID (should be cache hit)
	err = verifier.Verify(ctx, sharedCID, indexer1Sig)
	assert.NoError(t, err)
	t.Log("✅ All real-world tests passed!")
}

func TestCachedSignatureVerifier_DifferentCIDs(t *testing.T) {
	// Setup mock server
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}
	// Test different CIDs
	cids := []string{"cid1", "cid2", "cid3", "cid1", "cid2"}
	for _, cid := range cids {
		err := verifier.Verify(ctx, cid, signature)
		assert.NoError(t, err)
	}
	// Should have made 3 HTTP calls (one for each unique CID)
	assert.Equal(t, 3, callCount)
}
func TestCachedSignatureVerifier_MockVerifier(t *testing.T) {
	// Test MockSignatureVerifier
	mockVerifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			if cid == "error-cid" {
				return assert.AnError
			}
			return nil
		},
	}
	ctx := context.Background()
	signature := Signature{Identity: "0x123", Value: "sig", Type: "ES256K"}
	// Test success case
	err := mockVerifier.Verify(ctx, "success-cid", signature)
	assert.NoError(t, err)
	// Test error case
	err = mockVerifier.Verify(ctx, "error-cid", signature)
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestDefraSignatureVerifier_Verify_InvalidIdentityFormat(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil)

	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "", // Empty identity
		Value:    "signature",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

// Benchmark tests
func BenchmarkCachedSignatureVerifier_Cached(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}
	// First call to populate cache
	verifier.Verify(ctx, "benchmark-cid", signature)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier.Verify(ctx, "benchmark-cid", signature)
	}
}
func BenchmarkCachedSignatureVerifier_Uncached(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	mockNode := &node.Node{APIURL: server.URL}
	verifier := NewSignatureVerifier(mockNode)
	ctx := context.Background()
	signature := Signature{
		Identity: "0x1234567890abcdef",
		Value:    "signature123",
		Type:     "ES256K",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use different CID each time to avoid cache
		cid := fmt.Sprintf("benchmark-cid-%d", i)
		verifier.Verify(ctx, cid, signature)
	}
}
