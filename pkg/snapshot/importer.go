package snapshot

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
)

// ImportResult holds the block range of an import operation.
type ImportResult struct {
	StartBlock int64 `json:"start_block"`
	EndBlock   int64 `json:"end_block"`
}

// kvSnapshotHeader matches the header written by the indexer's KV snapshot format.
type kvSnapshotHeader struct {
	Magic               string                          `json:"magic"`
	Version             int                             `json:"version"`
	StartBlock          int64                           `json:"start_block"`
	EndBlock            int64                           `json:"end_block"`
	CreatedAt           string                          `json:"created_at"`
	BatchSigMerkleRoots []string                        `json:"batch_sig_merkle_roots,omitempty"`
	FieldMappings       []*client.CollectionFieldMapping `json:"field_mappings,omitempty"`
}

// ImportWithVerification verifies the cryptographic signature and Merkle root,
// then imports raw KV pairs into DefraDB. Does NOT rebuild indexes — the caller
// should call RebuildAllIndexes once after all snapshots are imported.
func ImportWithVerification(ctx context.Context, defraNode *node.Node, snapshotPath string, sig *SnapshotSignatureData) (*ImportResult, error) {
	if err := verifySignature(sig); err != nil {
		return nil, fmt.Errorf("signature verification failed: %w", err)
	}

	f, err := os.Open(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	var lenBuf [4]byte
	if _, err := io.ReadFull(gr, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read header length: %w", err)
	}
	headerLen := binary.BigEndian.Uint32(lenBuf[:])
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(gr, headerBytes); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	var header kvSnapshotHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}
	if header.Magic != "DFKV" {
		return nil, fmt.Errorf("invalid snapshot magic: %q", header.Magic)
	}

	if len(header.BatchSigMerkleRoots) == 0 {
		return nil, fmt.Errorf("no batch signatures found in KV snapshot header")
	}

	roots := make([][]byte, 0, len(header.BatchSigMerkleRoots))
	for _, rootHex := range header.BatchSigMerkleRoots {
		rootBytes, err := hex.DecodeString(rootHex)
		if err != nil {
			return nil, fmt.Errorf("decode batch sig root: %w", err)
		}
		roots = append(roots, rootBytes)
	}

	computedRoot := computeSnapshotMerkleRoot(roots)
	computedRootHex := hex.EncodeToString(computedRoot)
	if computedRootHex != sig.MerkleRoot {
		return nil, fmt.Errorf("merkle root mismatch: computed %s, expected %s", computedRootHex, sig.MerkleRoot)
	}

	var count int
	if len(header.FieldMappings) > 0 {
		count, err = defraNode.DB.ImportRawKVsWithMapping(ctx, gr, header.FieldMappings)
	} else {
		count, err = defraNode.DB.ImportRawKVs(ctx, gr)
	}
	if err != nil {
		return nil, fmt.Errorf("import raw KVs: %w", err)
	}

	logger.Sugar.Infof("KV snapshot imported: blocks %d-%d (%d KV pairs, signer: %s)",
		header.StartBlock, header.EndBlock, count, sig.SignatureIdentity[:16]+"...")

	return &ImportResult{
		StartBlock: header.StartBlock,
		EndBlock:   header.EndBlock,
	}, nil
}

// RebuildAllIndexes rebuilds indexes for all collections after bulk KV import.
// Call this once after importing all snapshots, not after each individual import.
func RebuildAllIndexes(ctx context.Context, defraNode *node.Node, collections []string) error {
	for _, colName := range collections {
		if err := defraNode.DB.RebuildCollectionIndexes(ctx, colName); err != nil {
			return fmt.Errorf("rebuild indexes for %s: %w", colName, err)
		}
	}
	return nil
}

// verifySignature verifies the cryptographic signature on a snapshot.
func verifySignature(sig *SnapshotSignatureData) error {
	merkleRootBytes, err := hex.DecodeString(sig.MerkleRoot)
	if err != nil {
		return fmt.Errorf("decode merkle root: %w", err)
	}

	sigValueBytes, err := hex.DecodeString(sig.SignatureValue)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	var keyType crypto.KeyType
	switch sig.SignatureType {
	case "ES256K", "ecdsa-256k":
		keyType = crypto.KeyTypeSecp256k1
	case "Ed25519", "ed25519":
		keyType = crypto.KeyTypeEd25519
	default:
		return fmt.Errorf("unsupported signature type: %s", sig.SignatureType)
	}

	pubKey, err := crypto.PublicKeyFromString(keyType, sig.SignatureIdentity)
	if err != nil {
		return fmt.Errorf("parse public key: %w", err)
	}

	valid, err := pubKey.Verify(merkleRootBytes, sigValueBytes)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid signature")
	}

	return nil
}

// computeSnapshotMerkleRoot computes a Merkle root from per-block batch sig roots.
func computeSnapshotMerkleRoot(batchSigMerkleRoots [][]byte) []byte {
	if len(batchSigMerkleRoots) == 0 {
		return nil
	}

	hashes := make([][]byte, len(batchSigMerkleRoots))
	for i, root := range batchSigMerkleRoots {
		hash := sha256.Sum256(root)
		hashes[i] = hash[:]
	}

	combined := make([]byte, 64)
	for len(hashes) > 1 {
		newLen := (len(hashes) + 1) / 2
		newHashes := make([][]byte, 0, newLen)
		for i := 0; i < len(hashes); i += 2 {
			if i+1 < len(hashes) {
				copy(combined[:32], hashes[i])
				copy(combined[32:], hashes[i+1])
				hash := sha256.Sum256(combined)
				newHashes = append(newHashes, hash[:])
			} else {
				newHashes = append(newHashes, hashes[i])
			}
		}
		hashes = newHashes
	}

	return hashes[0]
}
