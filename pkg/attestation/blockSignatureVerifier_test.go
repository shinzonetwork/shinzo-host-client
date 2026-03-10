package attestation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	gocid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

// makeCID creates a valid CIDv1 string from arbitrary data.
func makeCID(data string) string {
	hash, _ := mh.Sum([]byte(data), mh.SHA2_256, -1)
	c := gocid.NewCidV1(gocid.Raw, hash)
	return c.String()
}

// ---- BlockSignatureCache tests ----

func TestNewBlockSignatureCache(t *testing.T) {
	tests := []struct {
		name            string
		maxSize         int
		expectedMaxSize int
	}{
		{
			name:            "zero size defaults to 10000",
			maxSize:         0,
			expectedMaxSize: 10000,
		},
		{
			name:            "negative size defaults to 10000",
			maxSize:         -5,
			expectedMaxSize: 10000,
		},
		{
			name:            "positive size is used as-is",
			maxSize:         42,
			expectedMaxSize: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlockSignatureCache(tt.maxSize)
			require.NotNil(t, cache)
			require.NotNil(t, cache.signatures)
			require.Equal(t, tt.expectedMaxSize, cache.maxSize)
		})
	}
}

func TestBlockSignatureCache_Add(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(c *BlockSignatureCache)
		sig           *BlockSignature
		expectLen     int
		expectBlockIn int64
		expectMissing int64
	}{
		{
			name:      "nil signature is ignored",
			setup:     func(c *BlockSignatureCache) {},
			sig:       nil,
			expectLen: 0,
		},
		{
			name:  "add normal signature",
			setup: func(c *BlockSignatureCache) {},
			sig: &BlockSignature{
				BlockNumber: 10,
				BlockHash:   "abc",
			},
			expectLen:     1,
			expectBlockIn: 10,
		},
		{
			name: "eviction removes lowest block number",
			setup: func(c *BlockSignatureCache) {
				// Cache maxSize is 2. Pre-fill with blocks 5 and 7.
				c.Add(&BlockSignature{BlockNumber: 5})
				c.Add(&BlockSignature{BlockNumber: 7})
			},
			sig: &BlockSignature{
				BlockNumber: 9,
			},
			// After adding block 9, cache has 3 entries which exceeds maxSize=2,
			// so the lowest (block 5) is evicted.
			expectLen:     2,
			expectBlockIn: 9,
			expectMissing: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlockSignatureCache(2)
			tt.setup(cache)
			cache.Add(tt.sig)
			require.Equal(t, tt.expectLen, len(cache.signatures))

			if tt.expectBlockIn > 0 {
				_, ok := cache.signatures[tt.expectBlockIn]
				require.True(t, ok, "expected block %d to be present", tt.expectBlockIn)
			}
			if tt.expectMissing > 0 {
				_, ok := cache.signatures[tt.expectMissing]
				require.False(t, ok, "expected block %d to be evicted", tt.expectMissing)
			}
		})
	}
}

func TestBlockSignatureCache_Get(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(c *BlockSignatureCache)
		blockNumber int64
		expectFound bool
	}{
		{
			name: "get existing entry",
			setup: func(c *BlockSignatureCache) {
				c.Add(&BlockSignature{BlockNumber: 42, BlockHash: "hash42"})
			},
			blockNumber: 42,
			expectFound: true,
		},
		{
			name:        "get missing entry",
			setup:       func(c *BlockSignatureCache) {},
			blockNumber: 99,
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewBlockSignatureCache(100)
			tt.setup(cache)
			sig, ok := cache.Get(tt.blockNumber)
			require.Equal(t, tt.expectFound, ok)
			if tt.expectFound {
				require.NotNil(t, sig)
				require.Equal(t, tt.blockNumber, sig.BlockNumber)
			} else {
				require.Nil(t, sig)
			}
		})
	}
}

// ---- BlockSignatureVerifier delegation tests ----

func TestNewBlockSignatureVerifier(t *testing.T) {
	v := NewBlockSignatureVerifier(50)
	require.NotNil(t, v)
	require.NotNil(t, v.cache)
}

func TestBlockSignatureVerifier_AddAndGet(t *testing.T) {
	v := NewBlockSignatureVerifier(100)

	// Get non-existent
	sig, ok := v.GetBlockSignature(1)
	require.False(t, ok)
	require.Nil(t, sig)

	// Add and retrieve
	expected := &BlockSignature{BlockNumber: 1, BlockHash: "hash1"}
	v.AddBlockSignature(expected)

	sig, ok = v.GetBlockSignature(1)
	require.True(t, ok)
	require.Equal(t, expected, sig)
}

// ---- VerifyBlockSignature tests (error paths) ----

func TestBlockSignatureVerifier_VerifyBlockSignature(t *testing.T) {
	tests := []struct {
		name    string
		sig     *BlockSignature
		wantErr string
	}{
		{
			name:    "nil signature",
			sig:     nil,
			wantErr: "block signature is nil",
		},
		{
			name: "invalid merkle root hex",
			sig: &BlockSignature{
				MerkleRoot:     "not-valid-hex!",
				SignatureValue: "aabb",
				SignatureType:  "Ed25519",
			},
			wantErr: "failed to decode merkle root",
		},
		{
			name: "invalid signature value hex",
			sig: &BlockSignature{
				MerkleRoot:     "aabb",
				SignatureValue: "zzzz",
				SignatureType:  "Ed25519",
			},
			wantErr: "failed to decode signature value",
		},
		{
			name: "unsupported signature type",
			sig: &BlockSignature{
				MerkleRoot:     "aabb",
				SignatureValue: "ccdd",
				SignatureType:  "RSA2048",
			},
			wantErr: "unsupported signature type: RSA2048",
		},
		{
			name: "invalid public key for ES256K",
			sig: &BlockSignature{
				MerkleRoot:        "aabb",
				SignatureValue:    "ccdd",
				SignatureType:     "ES256K",
				SignatureIdentity: "not-a-real-key",
			},
			wantErr: "failed to parse public key",
		},
		{
			name: "invalid public key for ecdsa-256k alias",
			sig: &BlockSignature{
				MerkleRoot:        "aabb",
				SignatureValue:    "ccdd",
				SignatureType:     "ecdsa-256k",
				SignatureIdentity: "bad-key",
			},
			wantErr: "failed to parse public key",
		},
		{
			name: "invalid public key for Ed25519",
			sig: &BlockSignature{
				MerkleRoot:        "aabb",
				SignatureValue:    "ccdd",
				SignatureType:     "Ed25519",
				SignatureIdentity: "bad-key-data",
			},
			wantErr: "failed to parse public key",
		},
		{
			name: "invalid public key for ed25519 lowercase alias",
			sig: &BlockSignature{
				MerkleRoot:        "aabb",
				SignatureValue:    "ccdd",
				SignatureType:     "ed25519",
				SignatureIdentity: "bad-key-data",
			},
			wantErr: "failed to parse public key",
		},
	}

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.VerifyBlockSignature(ctx, tt.sig)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// ---- VerifyCIDsAgainstBlockSignature tests ----

func TestBlockSignatureVerifier_VerifyCIDsAgainstBlockSignature(t *testing.T) {
	cid1 := makeCID("data-1")
	cid2 := makeCID("data-2")

	// Compute the real merkle root for cid1 and cid2 so we can test the match path.
	matchingRoot := ComputeMerkleRootFromStrings([]string{cid1, cid2})
	matchingRootHex := hex.EncodeToString(matchingRoot)

	tests := []struct {
		name      string
		cids      []string
		sig       *BlockSignature
		wantMatch bool
		wantErr   string
	}{
		{
			name:    "nil signature",
			cids:    []string{cid1},
			sig:     nil,
			wantErr: "block signature is nil",
		},
		{
			name: "empty CIDs produces nil root",
			cids: []string{},
			sig: &BlockSignature{
				MerkleRoot: matchingRootHex,
			},
			wantErr: "failed to compute merkle root from CIDs",
		},
		{
			name: "all invalid CID strings produces nil root",
			cids: []string{"not-a-cid", "also-bad"},
			sig: &BlockSignature{
				MerkleRoot: matchingRootHex,
			},
			wantErr: "failed to compute merkle root from CIDs",
		},
		{
			name: "invalid hex in merkle root",
			cids: []string{cid1},
			sig: &BlockSignature{
				MerkleRoot: "zzzz-bad-hex",
			},
			wantErr: "failed to decode merkle root",
		},
		{
			name: "root length mismatch",
			cids: []string{cid1, cid2},
			sig: &BlockSignature{
				MerkleRoot: "aabb", // 2 bytes vs 32-byte SHA-256
			},
			wantMatch: false,
		},
		{
			name: "root content mismatch",
			cids: []string{cid1, cid2},
			sig: &BlockSignature{
				// 32-byte hex that does not match the computed root.
				MerkleRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			},
			wantMatch: false,
		},
		{
			name: "matching root",
			cids: []string{cid1, cid2},
			sig: &BlockSignature{
				MerkleRoot: matchingRootHex,
			},
			wantMatch: true,
		},
	}

	v := NewBlockSignatureVerifier(10)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := v.VerifyCIDsAgainstBlockSignature(tt.cids, tt.sig)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantMatch, match)
			}
		})
	}
}

// ---- VerifyCIDListAgainstMerkleRoot tests ----

func TestBlockSignatureVerifier_VerifyCIDListAgainstMerkleRoot(t *testing.T) {
	cid1 := makeCID("list-data-1")
	cid2 := makeCID("list-data-2")
	root := ComputeMerkleRootFromStrings([]string{cid1, cid2})
	rootHex := hex.EncodeToString(root)

	tests := []struct {
		name      string
		sig       *BlockSignature
		wantMatch bool
		wantErr   string
	}{
		{
			name:      "nil signature returns false",
			sig:       nil,
			wantMatch: false,
		},
		{
			name:      "empty CIDs returns false",
			sig:       &BlockSignature{CIDs: []string{}},
			wantMatch: false,
		},
		{
			name: "non-empty CIDs with matching root",
			sig: &BlockSignature{
				CIDs:       []string{cid1, cid2},
				MerkleRoot: rootHex,
			},
			wantMatch: true,
		},
		{
			name: "non-empty CIDs with mismatched root",
			sig: &BlockSignature{
				CIDs:       []string{cid1, cid2},
				MerkleRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			},
			wantMatch: false,
		},
	}

	v := NewBlockSignatureVerifier(10)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := v.VerifyCIDListAgainstMerkleRoot(tt.sig)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantMatch, match)
			}
		})
	}
}

// ---- ComputeMerkleRootFromStrings tests ----

func TestComputeMerkleRootFromStrings(t *testing.T) {
	cid1 := makeCID("merkle-a")
	cid2 := makeCID("merkle-b")
	cid3 := makeCID("merkle-c")
	cid4 := makeCID("merkle-d")

	tests := []struct {
		name       string
		cidStrings []string
		expectNil  bool
	}{
		{
			name:       "empty input returns nil",
			cidStrings: []string{},
			expectNil:  true,
		},
		{
			name:       "all invalid CIDs returns nil",
			cidStrings: []string{"garbage", "not-a-cid", "???"},
			expectNil:  true,
		},
		{
			name:       "single valid CID",
			cidStrings: []string{cid1},
			expectNil:  false,
		},
		{
			name:       "two valid CIDs (even count)",
			cidStrings: []string{cid1, cid2},
			expectNil:  false,
		},
		{
			name:       "three valid CIDs (odd count - exercises carry-over branch)",
			cidStrings: []string{cid1, cid2, cid3},
			expectNil:  false,
		},
		{
			name:       "four valid CIDs (even count)",
			cidStrings: []string{cid1, cid2, cid3, cid4},
			expectNil:  false,
		},
		{
			name:       "mix of valid and invalid CIDs",
			cidStrings: []string{cid1, "bad-cid", cid2},
			expectNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeMerkleRootFromStrings(tt.cidStrings)
			if tt.expectNil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, 32, len(result), "SHA-256 root should be 32 bytes")
			}
		})
	}
}

func TestComputeMerkleRootFromStrings_Deterministic(t *testing.T) {
	cid1 := makeCID("det-1")
	cid2 := makeCID("det-2")

	// Order should not matter because the function sorts CIDs internally.
	rootA := ComputeMerkleRootFromStrings([]string{cid1, cid2})
	rootB := ComputeMerkleRootFromStrings([]string{cid2, cid1})

	require.NotNil(t, rootA)
	require.NotNil(t, rootB)
	require.Equal(t, rootA, rootB, "merkle root must be deterministic regardless of input order")
}

func TestComputeMerkleRootFromStrings_DifferentInputsDifferentRoots(t *testing.T) {
	cid1 := makeCID("unique-1")
	cid2 := makeCID("unique-2")
	cid3 := makeCID("unique-3")

	rootAB := ComputeMerkleRootFromStrings([]string{cid1, cid2})
	rootAC := ComputeMerkleRootFromStrings([]string{cid1, cid3})

	require.NotNil(t, rootAB)
	require.NotNil(t, rootAC)
	require.NotEqual(t, rootAB, rootAC, "different CID sets should produce different roots")
}

// ---- BlockCIDCollector tests ----

func TestNewBlockCIDCollector(t *testing.T) {
	c := NewBlockCIDCollector()
	require.NotNil(t, c)
	require.NotNil(t, c.blockCIDs)
	require.NotNil(t, c.blockDocs)
	require.NotNil(t, c.docTypes)
}

func TestBlockCIDCollector_AddDocumentCID(t *testing.T) {
	c := NewBlockCIDCollector()
	c.AddDocumentCID(1, "doc-a", "typeA", "cid-a1")
	c.AddDocumentCID(1, "doc-b", "typeB", "cid-b1")
	c.AddDocumentCID(2, "doc-c", "typeC", "cid-c1")

	require.Equal(t, []string{"cid-a1", "cid-b1"}, c.blockCIDs[1])
	require.Equal(t, []string{"cid-c1"}, c.blockCIDs[2])
	require.Equal(t, []string{"doc-a", "doc-b"}, c.blockDocs[1])
	require.Equal(t, "typeA", c.docTypes["doc-a"])
	require.Equal(t, "typeB", c.docTypes["doc-b"])
	require.Equal(t, "typeC", c.docTypes["doc-c"])
}

func TestBlockCIDCollector_GetBlockCIDs(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(c *BlockCIDCollector)
		blockNumber int64
		expect      []string
	}{
		{
			name:        "get missing block returns nil",
			setup:       func(c *BlockCIDCollector) {},
			blockNumber: 99,
			expect:      nil,
		},
		{
			name: "get existing block returns copy",
			setup: func(c *BlockCIDCollector) {
				c.AddDocumentCID(5, "d1", "t1", "cid-1")
				c.AddDocumentCID(5, "d2", "t2", "cid-2")
			},
			blockNumber: 5,
			expect:      []string{"cid-1", "cid-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewBlockCIDCollector()
			tt.setup(c)
			result := c.GetBlockCIDs(tt.blockNumber)
			require.Equal(t, tt.expect, result)

			// If result is non-nil, verify it is a copy (mutating it should not
			// affect internal state).
			if result != nil {
				result[0] = "mutated"
				internal := c.GetBlockCIDs(tt.blockNumber)
				require.NotEqual(t, "mutated", internal[0], "GetBlockCIDs must return a copy")
			}
		})
	}
}

func TestBlockCIDCollector_ClearBlock(t *testing.T) {
	c := NewBlockCIDCollector()
	c.AddDocumentCID(10, "doc-x", "typeX", "cid-x")
	c.AddDocumentCID(10, "doc-y", "typeY", "cid-y")
	c.AddDocumentCID(20, "doc-z", "typeZ", "cid-z")

	c.ClearBlock(10)

	// Block 10 data should be gone.
	require.Nil(t, c.GetBlockCIDs(10))
	require.Empty(t, c.blockDocs[10])
	require.Empty(t, c.docTypes["doc-x"])
	require.Empty(t, c.docTypes["doc-y"])

	// Block 20 should be unaffected.
	require.Equal(t, []string{"cid-z"}, c.GetBlockCIDs(20))
	require.Equal(t, "typeZ", c.docTypes["doc-z"])
}

func TestBlockCIDCollector_ClearBlock_NonExistent(t *testing.T) {
	c := NewBlockCIDCollector()
	// Clearing a block that was never added should not panic.
	require.NotPanics(t, func() {
		c.ClearBlock(999)
	})
}

// ---- VerifyBlockSignature success and verification-failure tests ----

// signMerkleRoot generates a real signature over merkleRoot bytes using the given key type.
func signMerkleRoot(t *testing.T, merkleRoot []byte, keyType crypto.KeyType) (pubKeyHex, sigHex string) {
	t.Helper()
	privKey, err := crypto.GenerateKey(keyType)
	require.NoError(t, err)

	sig, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	pubKeyHex = privKey.GetPublic().String()
	sigHex = hex.EncodeToString(sig)
	return pubKeyHex, sigHex
}

func TestBlockSignatureVerifier_VerifyBlockSignature_Ed25519_Success(t *testing.T) {
	// Generate a real Ed25519 key pair, sign a merkle root, verify it succeeds.
	merkleRoot := sha256.Sum256([]byte("test-merkle-data"))
	pubKeyHex, sigHex := signMerkleRoot(t, merkleRoot[:], crypto.KeyTypeEd25519)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       42,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.NoError(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_Ed25519Lowercase_Success(t *testing.T) {
	merkleRoot := sha256.Sum256([]byte("test-ed25519-lowercase"))
	pubKeyHex, sigHex := signMerkleRoot(t, merkleRoot[:], crypto.KeyTypeEd25519)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       43,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "ed25519",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.NoError(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_Secp256k1_Success(t *testing.T) {
	// Generate a real secp256k1 key pair, sign a merkle root, verify it succeeds.
	merkleRoot := sha256.Sum256([]byte("test-secp256k1-data"))
	pubKeyHex, sigHex := signMerkleRoot(t, merkleRoot[:], crypto.KeyTypeSecp256k1)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       44,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "ES256K",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.NoError(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_EcdsaAlias_Success(t *testing.T) {
	merkleRoot := sha256.Sum256([]byte("test-ecdsa-alias"))
	pubKeyHex, sigHex := signMerkleRoot(t, merkleRoot[:], crypto.KeyTypeSecp256k1)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       45,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "ecdsa-256k",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.NoError(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_InvalidSignature(t *testing.T) {
	// Valid key but signature does not match the merkle root.
	// This covers the "signature verification error" or "block signature verification failed" path.
	merkleRoot := sha256.Sum256([]byte("real-merkle-root"))
	differentData := sha256.Sum256([]byte("different-data"))

	// Sign different data, but present the real merkle root for verification
	pubKeyHex, sigHex := signMerkleRoot(t, differentData[:], crypto.KeyTypeEd25519)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       46,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.Error(t, err)
	// Should be either "signature verification error" or "block signature verification failed"
	errMsg := err.Error()
	require.True(t,
		strings.Contains(errMsg, "block signature verification failed") ||
			strings.Contains(errMsg, "signature verification error"),
		"unexpected error: %s", errMsg,
	)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_Secp256k1_InvalidSignature(t *testing.T) {
	// Valid secp256k1 key but wrong signature data.
	merkleRoot := sha256.Sum256([]byte("secp-merkle-root"))
	differentData := sha256.Sum256([]byte("secp-different-data"))

	pubKeyHex, sigHex := signMerkleRoot(t, differentData[:], crypto.KeyTypeSecp256k1)

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       47,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    sigHex,
		SignatureType:     "ES256K",
		SignatureIdentity: pubKeyHex,
	}

	err := v.VerifyBlockSignature(ctx, sig)
	require.Error(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_MalformedSignatureBytes(t *testing.T) {
	// Valid key, valid hex for signature, but the signature bytes are not
	// valid DER-encoded or otherwise malformed. This may trigger the
	// "signature verification error" path (pubKey.Verify returning an error).
	merkleRoot := sha256.Sum256([]byte("malformed-sig-test"))

	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	pubKeyHex := privKey.GetPublic().String()

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       48,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    "0102030405", // Valid hex but not a valid DER signature
		SignatureType:     "ES256K",
		SignatureIdentity: pubKeyHex,
	}

	err = v.VerifyBlockSignature(ctx, sig)
	require.Error(t, err)
}

func TestBlockSignatureVerifier_VerifyBlockSignature_Ed25519_MalformedSignatureBytes(t *testing.T) {
	// Valid Ed25519 key, but completely wrong signature bytes (not 64 bytes).
	// Ed25519 Verify may return error or (false, nil) depending on implementation.
	merkleRoot := sha256.Sum256([]byte("ed25519-malformed-sig-test"))

	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)
	pubKeyHex := privKey.GetPublic().String()

	v := NewBlockSignatureVerifier(10)
	ctx := context.Background()

	sig := &BlockSignature{
		BlockNumber:       49,
		MerkleRoot:        hex.EncodeToString(merkleRoot[:]),
		SignatureValue:    "0102", // Only 2 bytes, not a valid Ed25519 signature
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyHex,
	}

	err = v.VerifyBlockSignature(ctx, sig)
	require.Error(t, err)
	errMsg := err.Error()
	require.True(t,
		strings.Contains(errMsg, "signature verification error") ||
			strings.Contains(errMsg, "block signature verification failed"),
		"unexpected error: %s", errMsg,
	)
}

func TestComputeMerkleRootFromStrings_FiveCIDs(t *testing.T) {
	// Five CIDs exercise multiple rounds of the merkle tree with odd carry-over
	// at different tree levels.
	cid1 := makeCID("five-a")
	cid2 := makeCID("five-b")
	cid3 := makeCID("five-c")
	cid4 := makeCID("five-d")
	cid5 := makeCID("five-e")

	result := ComputeMerkleRootFromStrings([]string{cid1, cid2, cid3, cid4, cid5})
	require.NotNil(t, result)
	require.Equal(t, 32, len(result))

	// Verify determinism with different order
	result2 := ComputeMerkleRootFromStrings([]string{cid5, cid3, cid1, cid4, cid2})
	require.Equal(t, result, result2)
}

func TestComputeMerkleRootFromStrings_SingleCIDEqualsHash(t *testing.T) {
	// With a single CID, the merkle root should be the SHA-256 hash of that CID's bytes.
	cid1 := makeCID("single-cid-hash-check")
	c, err := gocid.Decode(cid1)
	require.NoError(t, err)

	expected := sha256.Sum256(c.Bytes())
	result := ComputeMerkleRootFromStrings([]string{cid1})
	require.NotNil(t, result)
	require.Equal(t, expected[:], result)
}

func TestBlockCIDCollector_ConcurrentAccess(t *testing.T) {
	// Verify BlockCIDCollector is safe for concurrent use.
	c := NewBlockCIDCollector()
	const goroutines = 10
	done := make(chan bool, goroutines*3)

	for i := range goroutines {
		go func(idx int) {
			c.AddDocumentCID(1, fmt.Sprintf("doc-%d", idx), "type", fmt.Sprintf("cid-%d", idx))
			done <- true
		}(i)
		go func(idx int) {
			_ = c.GetBlockCIDs(1)
			done <- true
		}(i)
		go func() {
			c.ClearBlock(2) // clear a different block concurrently
			done <- true
		}()
	}

	for range goroutines * 3 {
		<-done
	}

	// Just verify no panics occurred
	cids := c.GetBlockCIDs(1)
	require.Len(t, cids, goroutines)
}
