package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Init(true, "")
}

func TestComputeSnapshotMerkleRoot_Empty(t *testing.T) {
	result := computeSnapshotMerkleRoot(nil)
	require.Nil(t, result)

	result = computeSnapshotMerkleRoot([][]byte{})
	require.Nil(t, result)
}

func TestComputeSnapshotMerkleRoot_SingleRoot(t *testing.T) {
	root := []byte("block-sig-merkle-root-1")
	result := computeSnapshotMerkleRoot([][]byte{root})

	// Single root: hash of SHA256(root)
	expected := sha256.Sum256(root)
	require.Equal(t, expected[:], result)
}

func TestComputeSnapshotMerkleRoot_TwoRoots(t *testing.T) {
	root1 := []byte("root-1")
	root2 := []byte("root-2")
	result := computeSnapshotMerkleRoot([][]byte{root1, root2})
	require.NotNil(t, result)
	require.Len(t, result, 32)

	// Verify manually: hash each root, then combine
	h1 := sha256.Sum256(root1)
	h2 := sha256.Sum256(root2)
	combined := make([]byte, 64)
	copy(combined[:32], h1[:])
	copy(combined[32:], h2[:])
	expected := sha256.Sum256(combined)
	require.Equal(t, expected[:], result)
}

func TestComputeSnapshotMerkleRoot_ThreeRoots(t *testing.T) {
	roots := [][]byte{
		[]byte("root-a"),
		[]byte("root-b"),
		[]byte("root-c"),
	}
	result := computeSnapshotMerkleRoot(roots)
	require.NotNil(t, result)
	require.Len(t, result, 32)

	// Three roots: pair (a,b) and carry c
	h := [3][32]byte{}
	for i, r := range roots {
		h[i] = sha256.Sum256(r)
	}
	combined := make([]byte, 64)
	copy(combined[:32], h[0][:])
	copy(combined[32:], h[1][:])
	pair := sha256.Sum256(combined)

	// Second level: combine pair with h[2]
	copy(combined[:32], pair[:])
	copy(combined[32:], h[2][:])
	expected := sha256.Sum256(combined)
	require.Equal(t, expected[:], result)
}

func TestComputeSnapshotMerkleRoot_Deterministic(t *testing.T) {
	roots := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	r1 := computeSnapshotMerkleRoot(roots)
	r2 := computeSnapshotMerkleRoot(roots)
	require.Equal(t, r1, r2)
}

func TestComputeSnapshotMerkleRoot_DifferentInputs(t *testing.T) {
	r1 := computeSnapshotMerkleRoot([][]byte{[]byte("a")})
	r2 := computeSnapshotMerkleRoot([][]byte{[]byte("b")})
	require.NotEqual(t, r1, r2)
}

func TestVerifySignature_UnsupportedType(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:    "abcd1234",
		SignatureValue: "abcd1234",
		SignatureType:  "RSA",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported signature type")
}

func TestVerifySignature_InvalidMerkleRootHex(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:    "not-hex!!!",
		SignatureValue: "abcd",
		SignatureType:  "Ed25519",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode merkle root")
}

func TestVerifySignature_InvalidSignatureHex(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:    "abcd1234",
		SignatureValue: "not-hex!!!",
		SignatureType:  "Ed25519",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode signature")
}

func TestVerifySignature_InvalidPublicKey(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:        "abcd1234",
		SignatureValue:    "abcd1234",
		SignatureType:     "Ed25519",
		SignatureIdentity: "not-a-valid-public-key",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse public key")
}

func TestVerifySignature_ES256K_InvalidPublicKey(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:        "abcd1234",
		SignatureValue:    "abcd1234",
		SignatureType:     "ES256K",
		SignatureIdentity: "bad-key",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse public key")
}

func TestVerifySignature_Ecdsa256k_InvalidPublicKey(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:        "abcd1234",
		SignatureValue:    "abcd1234",
		SignatureType:     "ecdsa-256k",
		SignatureIdentity: "bad-key",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse public key")
}

func TestVerifySignature_Ed25519Lowercase_InvalidPublicKey(t *testing.T) {
	sig := &SnapshotSignatureData{
		MerkleRoot:        "abcd1234",
		SignatureValue:    "abcd1234",
		SignatureType:     "ed25519",
		SignatureIdentity: "bad-key",
	}
	err := verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse public key")
}

// generateEd25519TestSig creates a valid Ed25519 signature for testing.
// Returns the public key string, merkle root hex, and signature hex.
func generateEd25519TestSig(t *testing.T, merkleRoot []byte) (pubKeyStr, merkleRootHex, sigHex string) {
	t.Helper()
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	sigBytes, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	pubKeyStr = privKey.GetPublic().String()
	merkleRootHex = hex.EncodeToString(merkleRoot)
	sigHex = hex.EncodeToString(sigBytes)
	return
}

func TestVerifySignature_ValidEd25519(t *testing.T) {
	merkleRoot := []byte("test-merkle-root-data-for-signing")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}
	err := verifySignature(sig)
	require.NoError(t, err)
}

func TestVerifySignature_ValidEd25519Lowercase(t *testing.T) {
	merkleRoot := []byte("test-merkle-root-lowercase")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "ed25519",
		SignatureIdentity: pubKeyStr,
	}
	err := verifySignature(sig)
	require.NoError(t, err)
}

func TestVerifySignature_InvalidSignature_Ed25519(t *testing.T) {
	// Generate a valid key but use a wrong signature value to trigger "invalid signature".
	merkleRoot := []byte("some-merkle-root")
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	// Sign different data so the signature does not match the merkle root.
	sigBytes, err := privKey.Sign([]byte("different-data"))
	require.NoError(t, err)

	sig := &SnapshotSignatureData{
		MerkleRoot:        hex.EncodeToString(merkleRoot),
		SignatureValue:    hex.EncodeToString(sigBytes),
		SignatureType:     "Ed25519",
		SignatureIdentity: privKey.GetPublic().String(),
	}
	err = verifySignature(sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid signature")
}

func TestVerifySignature_ValidSecp256k1(t *testing.T) {
	merkleRoot := []byte("test-merkle-root-secp256k1")
	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)

	sigBytes, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	sig := &SnapshotSignatureData{
		MerkleRoot:        hex.EncodeToString(merkleRoot),
		SignatureValue:    hex.EncodeToString(sigBytes),
		SignatureType:     "ES256K",
		SignatureIdentity: privKey.GetPublic().String(),
	}
	err = verifySignature(sig)
	require.NoError(t, err)
}

func TestVerifySignature_ValidSecp256k1_EcdsaAlias(t *testing.T) {
	merkleRoot := []byte("test-merkle-root-ecdsa-alias")
	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)

	sigBytes, err := privKey.Sign(merkleRoot)
	require.NoError(t, err)

	sig := &SnapshotSignatureData{
		MerkleRoot:        hex.EncodeToString(merkleRoot),
		SignatureValue:    hex.EncodeToString(sigBytes),
		SignatureType:     "ecdsa-256k",
		SignatureIdentity: privKey.GetPublic().String(),
	}
	err = verifySignature(sig)
	require.NoError(t, err)
}

func TestVerifySignature_InvalidSignature_Secp256k1(t *testing.T) {
	merkleRoot := []byte("some-secp-merkle-root")
	privKey, err := crypto.GenerateKey(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)

	// Sign different data to produce a valid but non-matching signature.
	sigBytes, err := privKey.Sign([]byte("wrong-data"))
	require.NoError(t, err)

	sig := &SnapshotSignatureData{
		MerkleRoot:        hex.EncodeToString(merkleRoot),
		SignatureValue:    hex.EncodeToString(sigBytes),
		SignatureType:     "ES256K",
		SignatureIdentity: privKey.GetPublic().String(),
	}
	err = verifySignature(sig)
	require.Error(t, err)
	// Could be "invalid signature" or "verify" depending on the crypto implementation.
}

// buildTestSnapshot creates a gzipped KV snapshot file for testing and returns
// the path along with the header used.
func buildTestSnapshot(t *testing.T, header kvSnapshotHeader) string {
	t.Helper()

	headerBytes, err := json.Marshal(header)
	require.NoError(t, err)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(headerBytes)))
	_, err = gw.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = gw.Write(headerBytes)
	require.NoError(t, err)

	// Write empty KV data (no actual key-value pairs).
	require.NoError(t, gw.Close())

	path := filepath.Join(t.TempDir(), "test-snapshot.gz")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))
	return path
}

// buildValidSigForBlockRoots creates a valid Ed25519 signature that signs the
// computed merkle root of the given block sig merkle root hex strings.
func buildValidSigForBlockRoots(t *testing.T, blockSigRootHexes []string) *SnapshotSignatureData {
	t.Helper()

	roots := make([][]byte, 0, len(blockSigRootHexes))
	for _, h := range blockSigRootHexes {
		b, err := hex.DecodeString(h)
		require.NoError(t, err)
		roots = append(roots, b)
	}

	computedRoot := computeSnapshotMerkleRoot(roots)
	merkleRootHex := hex.EncodeToString(computedRoot)

	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	sigBytes, err := privKey.Sign(computedRoot)
	require.NoError(t, err)

	return &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    hex.EncodeToString(sigBytes),
		SignatureType:     "Ed25519",
		SignatureIdentity: privKey.GetPublic().String(),
	}
}

func TestImportWithVerification_SignatureVerificationFail(t *testing.T) {
	ctx := context.Background()
	sig := &SnapshotSignatureData{
		MerkleRoot:    "not-hex!!!",
		SignatureValue: "abcd",
		SignatureType:  "Ed25519",
	}
	_, err := ImportWithVerification(ctx, nil, "/nonexistent", sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature verification failed")
}

func TestImportWithVerification_FileOpenError(t *testing.T) {
	ctx := context.Background()
	// Use a valid signature to get past verification, but a nonexistent file path.
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}
	_, err := ImportWithVerification(ctx, nil, "/nonexistent/path/snapshot.gz", sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open snapshot")
}

func TestImportWithVerification_GzipReaderError(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	// Write a file that is NOT gzip compressed.
	path := filepath.Join(t.TempDir(), "not-gzip.gz")
	require.NoError(t, os.WriteFile(path, []byte("this is not gzip data"), 0644))

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gzip reader")
}

func TestImportWithVerification_ReadHeaderLengthError(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	// Write a valid gzip file but with only 2 bytes (not enough for the 4-byte header length).
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte{0x00, 0x01})
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	path := filepath.Join(t.TempDir(), "short-header.gz")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

	_, err = ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read header length")
}

func TestImportWithVerification_ReadHeaderError(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	// Write header length claiming 1000 bytes, but only provide 5 bytes of header data.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 1000)
	_, err := gw.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = gw.Write([]byte("short"))
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	path := filepath.Join(t.TempDir(), "truncated-header.gz")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

	_, err = ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read header")
}

func TestImportWithVerification_InvalidHeaderJSON(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	// Write valid gzip with a header that is not valid JSON.
	headerBytes := []byte("{not valid json!!!")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(headerBytes)))
	_, err := gw.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = gw.Write(headerBytes)
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	path := filepath.Join(t.TempDir(), "bad-json-header.gz")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

	_, err = ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse header")
}

func TestImportWithVerification_InvalidMagic(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:      "BAAD",
		Version:    1,
		StartBlock: 0,
		EndBlock:   10,
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid snapshot magic")
}

func TestImportWithVerification_NoBlockSigMerkleRoots(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		BlockSigMerkleRoots: []string{}, // empty
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no block signatures found")
}

func TestImportWithVerification_InvalidBlockSigRootHex(t *testing.T) {
	ctx := context.Background()
	merkleRoot := []byte("some-root")
	pubKeyStr, merkleRootHex, sigHex := generateEd25519TestSig(t, merkleRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        merkleRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		BlockSigMerkleRoots: []string{"not-valid-hex!!!"},
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode block sig root")
}

func TestImportWithVerification_MerkleRootMismatch(t *testing.T) {
	ctx := context.Background()

	// Create block sig roots in the file.
	blockRoot1 := hex.EncodeToString([]byte("block-root-1"))
	blockRoot2 := hex.EncodeToString([]byte("block-root-2"))

	// Sign a DIFFERENT merkle root than what's computed from the block roots.
	differentRoot := []byte("completely-different-root")
	pubKeyStr, differentRootHex, sigHex := generateEd25519TestSig(t, differentRoot)

	sig := &SnapshotSignatureData{
		MerkleRoot:        differentRootHex,
		SignatureValue:    sigHex,
		SignatureType:     "Ed25519",
		SignatureIdentity: pubKeyStr,
	}

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		BlockSigMerkleRoots: []string{blockRoot1, blockRoot2},
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "merkle root mismatch")
}

func TestImportWithVerification_BlockSigRootCountMismatch(t *testing.T) {
	ctx := context.Background()

	blockRoot1 := hex.EncodeToString([]byte("block-root-1"))
	blockRoot2 := hex.EncodeToString([]byte("block-root-2"))
	headerRoots := []string{blockRoot1, blockRoot2}

	// Build a valid signature that matches the computed merkle root from the header roots.
	sig := buildValidSigForBlockRoots(t, headerRoots)

	// Set the signature's block sig roots to a different count.
	sig.BlockSigMerkleRoots = []string{blockRoot1} // only 1 instead of 2

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		BlockSigMerkleRoots: headerRoots,
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block sig root count mismatch")
}

func TestImportWithVerification_BlockSigRootContentMismatch(t *testing.T) {
	ctx := context.Background()

	blockRoot1 := hex.EncodeToString([]byte("block-root-1"))
	blockRoot2 := hex.EncodeToString([]byte("block-root-2"))
	headerRoots := []string{blockRoot1, blockRoot2}

	sig := buildValidSigForBlockRoots(t, headerRoots)

	// Set the signature's block sig roots with same count but different content at index 1.
	sig.BlockSigMerkleRoots = []string{blockRoot1, hex.EncodeToString([]byte("different-root"))}

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		BlockSigMerkleRoots: headerRoots,
	})

	_, err := ImportWithVerification(ctx, nil, path, sig)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block sig root mismatch at index")
}

func TestImportWithVerification_HappyPath(t *testing.T) {
	ctx := context.Background()

	blockRoot1 := hex.EncodeToString([]byte("block-root-1"))
	headerRoots := []string{blockRoot1}

	sig := buildValidSigForBlockRoots(t, headerRoots)

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            10,
		CreatedAt:           "2025-01-01T00:00:00Z",
		BlockSigMerkleRoots: headerRoots,
	})

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	result, err := ImportWithVerification(ctx, defraNode, path, sig)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(0), result.StartBlock)
	require.Equal(t, int64(10), result.EndBlock)
}

func TestImportWithVerification_HappyPathWithMatchingSigBlockRoots(t *testing.T) {
	ctx := context.Background()

	blockRoot1 := hex.EncodeToString([]byte("block-root-1"))
	blockRoot2 := hex.EncodeToString([]byte("block-root-2"))
	headerRoots := []string{blockRoot1, blockRoot2}

	sig := buildValidSigForBlockRoots(t, headerRoots)
	// Set signature block sig roots matching the header ones.
	sig.BlockSigMerkleRoots = headerRoots

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          5,
		EndBlock:            20,
		CreatedAt:           "2025-01-01T00:00:00Z",
		BlockSigMerkleRoots: headerRoots,
	})

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	result, err := ImportWithVerification(ctx, defraNode, path, sig)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(5), result.StartBlock)
	require.Equal(t, int64(20), result.EndBlock)
}

func TestImportWithVerification_HappyPathWithFieldMappings(t *testing.T) {
	ctx := context.Background()

	blockRoot1 := hex.EncodeToString([]byte("block-root-fm"))
	headerRoots := []string{blockRoot1}

	sig := buildValidSigForBlockRoots(t, headerRoots)

	path := buildTestSnapshot(t, kvSnapshotHeader{
		Magic:               "DFKV",
		Version:             1,
		StartBlock:          0,
		EndBlock:            5,
		CreatedAt:           "2025-01-01T00:00:00Z",
		BlockSigMerkleRoots: headerRoots,
		FieldMappings: []*client.CollectionFieldMapping{
			{
				CollectionID:      "bafkreitest",
				CollectionShortID: 1,
				FieldIDMapping:    map[uint32]string{1: "bafkreifield1"},
			},
		},
	})

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	result, err := ImportWithVerification(ctx, defraNode, path, sig)
	// Field mapping remapping requires real collection IDs registered in DefraDB;
	// with mock IDs we exercise the ImportRawKVsWithMapping branch but get a remap error.
	require.Error(t, err)
	require.Contains(t, err.Error(), "import raw KVs")
	require.Nil(t, result)
}

func TestVerifySignature_VerifyReturnsError(t *testing.T) {
	// Provide a valid Ed25519 public key but a signature value that has the wrong
	// length, causing pubKey.Verify to return an error rather than (false, nil).
	privKey, err := crypto.GenerateKey(crypto.KeyTypeEd25519)
	require.NoError(t, err)

	merkleRoot := []byte("test-merkle-root")

	sig := &SnapshotSignatureData{
		MerkleRoot:        hex.EncodeToString(merkleRoot),
		SignatureValue:    hex.EncodeToString([]byte("too-short")), // wrong length for Ed25519 sig
		SignatureType:     "Ed25519",
		SignatureIdentity: privKey.GetPublic().String(),
	}
	err = verifySignature(sig)
	require.Error(t, err)
	// The error may be "verify:" or "invalid signature" depending on the crypto library
}

func TestComputeSnapshotMerkleRoot_FourRoots(t *testing.T) {
	// Four roots: all pairs are even, no carry needed
	roots := [][]byte{
		[]byte("root-1"),
		[]byte("root-2"),
		[]byte("root-3"),
		[]byte("root-4"),
	}
	result := computeSnapshotMerkleRoot(roots)
	require.NotNil(t, result)
	require.Len(t, result, 32)

	// Verify: hash each, pair (1,2) and (3,4), then combine
	h := [4][32]byte{}
	for i, r := range roots {
		h[i] = sha256.Sum256(r)
	}
	combined := make([]byte, 64)
	copy(combined[:32], h[0][:])
	copy(combined[32:], h[1][:])
	pair01 := sha256.Sum256(combined)

	copy(combined[:32], h[2][:])
	copy(combined[32:], h[3][:])
	pair23 := sha256.Sum256(combined)

	copy(combined[:32], pair01[:])
	copy(combined[32:], pair23[:])
	expected := sha256.Sum256(combined)
	require.Equal(t, expected[:], result)
}

func TestComputeSnapshotMerkleRoot_FiveRoots(t *testing.T) {
	// Five roots: exercises odd carry at the first level
	roots := [][]byte{
		[]byte("r1"),
		[]byte("r2"),
		[]byte("r3"),
		[]byte("r4"),
		[]byte("r5"),
	}
	result := computeSnapshotMerkleRoot(roots)
	require.NotNil(t, result)
	require.Len(t, result, 32)
}

func TestRebuildAllIndexes_EmptyCollections(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	// Empty collections list should succeed without doing anything.
	err = RebuildAllIndexes(ctx, defraNode, []string{})
	require.NoError(t, err)
}

func TestRebuildAllIndexes_NonexistentCollection(t *testing.T) {
	ctx := context.Background()

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	err = RebuildAllIndexes(ctx, defraNode, []string{"nonexistent_collection"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rebuild indexes for nonexistent_collection")
}
