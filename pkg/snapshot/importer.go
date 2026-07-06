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
	"path/filepath"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/node"
)

// kvImportFieldMapping matches the JSON format expected by DefraDB's ImportRawKVsWithMapping.
type kvImportFieldMapping struct {
	CollectionName    string               `json:"collectionName"`
	CollectionShortID uint32               `json:"collectionShortID"`
	Fields            []kvImportFieldEntry `json:"fields"`
}

type kvImportFieldEntry struct {
	Name    string `json:"name"`
	FieldID string `json:"fieldID"`
	ShortID uint32 `json:"shortID"`
}

// ImportResult holds the block range of an import operation.
type ImportResult struct {
	StartBlock int64 `json:"start_block"`
	EndBlock   int64 `json:"end_block"`
}

// kvSnapshotHeader matches the header written by the indexer's KV snapshot format.
type kvSnapshotHeader struct {
	Magic               string                           `json:"magic"`
	Version             int                              `json:"version"`
	StartBlock          int64                            `json:"start_block"`
	EndBlock            int64                            `json:"end_block"`
	CreatedAt           string                           `json:"created_at"`
	BlockSigMerkleRoots []string                         `json:"block_sig_merkle_roots,omitempty"`
	FieldMappings       []*client.CollectionFieldMapping `json:"field_mappings,omitempty"`
}

// ImportWithVerification verifies the cryptographic signature and Merkle root,
// then imports raw KV pairs into DefraDB. Does NOT rebuild indexes — the caller
// should call RebuildAllIndexes once after all snapshots are imported.
// ImportWithVerification verifies and imports a signed KV snapshot.
func ImportWithVerification(ctx context.Context, defraNode *node.Node, snapshotPath string, sig *SignatureData) (*ImportResult, error) {
	if err := verifySignature(sig); err != nil {
		return nil, fmt.Errorf("signature verification failed: %w", err)
	}

	header, gr, cleanup, err := openAndReadHeader(snapshotPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := verifyHeaderAgainstSig(header, sig); err != nil {
		return nil, err
	}

	count, err := importKVs(ctx, defraNode, gr, header)
	if err != nil {
		return nil, err
	}

	logger.Sugar.Infof("KV snapshot imported: blocks %d-%d (%d KV pairs, signer: %s)",
		header.StartBlock, header.EndBlock, count, sig.SignatureIdentity[:16]+"...")

	return &ImportResult{
		StartBlock: header.StartBlock,
		EndBlock:   header.EndBlock,
	}, nil
}

// openAndReadHeader opens the snapshot file and reads the header, returning a cleanup func.
func openAndReadHeader(snapshotPath string) (*kvSnapshotHeader, *gzip.Reader, func(), error) {
	f, err := os.Open(filepath.Clean(snapshotPath))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open snapshot: %w", err)
	}

	gr, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, nil, fmt.Errorf("gzip reader: %w", err)
	}

	cleanup := func() {
		_ = gr.Close()
		_ = f.Close()
	}

	header, err := readSnapshotHeader(gr)
	if err != nil {
		cleanup()
		return nil, nil, nil, err
	}

	return header, gr, cleanup, nil
}

// readSnapshotHeader reads and parses the length-prefixed JSON header from the gzip stream.
func readSnapshotHeader(gr *gzip.Reader) (*kvSnapshotHeader, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(gr, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read header length: %w", err)
	}

	headerBytes := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(gr, headerBytes); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	var header kvSnapshotHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}
	if header.Magic != snapshotMagic {
		return nil, fmt.Errorf("magic %q: %w", header.Magic, ErrInvalidSnapshotMagic)
	}
	if len(header.BlockSigMerkleRoots) == 0 {
		return nil, ErrNoBlockSignatures
	}

	return &header, nil
}

// verifyHeaderAgainstSig verifies the header's merkle root and block sig roots match the signature.
func verifyHeaderAgainstSig(header *kvSnapshotHeader, sig *SignatureData) error {
	roots := make([][]byte, 0, len(header.BlockSigMerkleRoots))
	for _, rootHex := range header.BlockSigMerkleRoots {
		rootBytes, err := hex.DecodeString(rootHex)
		if err != nil {
			return fmt.Errorf("decode block sig root: %w", err)
		}
		roots = append(roots, rootBytes)
	}

	computedRootHex := hex.EncodeToString(computeSnapshotMerkleRoot(roots))
	if computedRootHex != sig.MerkleRoot {
		return fmt.Errorf("computed %s, expected %s: %w", computedRootHex, sig.MerkleRoot, ErrMerkleRootMismatch)
	}

	if len(sig.BlockSigMerkleRoots) == 0 {
		return nil
	}

	if len(sig.BlockSigMerkleRoots) != len(header.BlockSigMerkleRoots) {
		return fmt.Errorf("signature has %d, file header has %d: %w",
			len(sig.BlockSigMerkleRoots), len(header.BlockSigMerkleRoots), ErrBlockSigCountMismatch)
	}

	for i, sigRoot := range sig.BlockSigMerkleRoots {
		if sigRoot != header.BlockSigMerkleRoots[i] {
			return fmt.Errorf("index %d, signature %s, header %s: %w",
				i, sigRoot, header.BlockSigMerkleRoots[i], ErrBlockSigRootMismatch)
		}
	}

	return nil
}

func buildImportMappingJSON(ctx context.Context, defraNode *node.Node, mappings []*client.CollectionFieldMapping) ([]byte, error) {
	if len(mappings) != 1 {
		return nil, fmt.Errorf("got %d: %w", len(mappings), ErrExpectedOneFieldMapping)
	}
	m := mappings[0]

	cols, err := defraNode.DB.GetCollections(ctx, options.GetCollections().SetCollectionID(m.CollectionID))
	if err != nil {
		return nil, fmt.Errorf("resolve collection %q: %w", m.CollectionID, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("collection %q: %w", m.CollectionID, ErrCollectionNotFound)
	}

	importMapping := kvImportFieldMapping{
		CollectionName:    cols[0].Name(),
		CollectionShortID: m.CollectionShortID,
	}
	for shortID, fieldID := range m.FieldIDMapping {
		importMapping.Fields = append(importMapping.Fields, kvImportFieldEntry{
			FieldID: fieldID,
			ShortID: shortID,
		})
	}

	return json.Marshal(importMapping)
}

// importKVs imports raw KV pairs from the gzip reader into DefraDB.
func importKVs(ctx context.Context, defraNode *node.Node, gr *gzip.Reader, header *kvSnapshotHeader) (int, error) {
	var count int
	var err error

	var mappingJSON []byte
	if len(header.FieldMappings) > 0 {
		mappingJSON, err = buildImportMappingJSON(ctx, defraNode, header.FieldMappings)
		if err != nil {
			return 0, fmt.Errorf("build field mapping: %w", err)
		}
		count, err = defraNode.DB.ImportRawKVsWithMapping(ctx, gr, mappingJSON)
	} else {
		count, err = defraNode.DB.ImportRawKVs(ctx, gr)
	}
	if err != nil {
		return 0, fmt.Errorf("import raw KVs: %w", err)
	}

	return count, nil
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
func verifySignature(sig *SignatureData) error {
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
	case sigTypeES256K, sigTypeES256KLower:
		keyType = crypto.KeyTypeSecp256k1
	case sigTypeEd25519, sigTypeEd25519Lower:
		keyType = crypto.KeyTypeEd25519
	default:
		return fmt.Errorf("signature type %s: %w", sig.SignatureType, ErrUnsupportedSigType)
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
		return ErrInvalidSignature
	}

	return nil
}

// computeSnapshotMerkleRoot computes a Merkle root from per-block block sig roots.
//
//nolint:gomnd
func computeSnapshotMerkleRoot(blockSigMerkleRoots [][]byte) []byte {
	if len(blockSigMerkleRoots) == 0 {
		return nil
	}

	hashes := make([][]byte, len(blockSigMerkleRoots))
	for i, root := range blockSigMerkleRoots {
		hash := sha256.Sum256(root)
		hashes[i] = hash[:]
	}

	combined := make([]byte, sha256CombinedByteSize)
	for len(hashes) > 1 {
		newLen := (len(hashes) + 1) / merkleArity
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
