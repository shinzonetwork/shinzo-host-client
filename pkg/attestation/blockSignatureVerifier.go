package attestation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	gocid "github.com/ipfs/go-cid"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/crypto"
)

// BlockSignature represents a block signature from an indexer
type BlockSignature struct {
	BlockNumber       int64    `json:"blockNumber"`
	BlockHash         string   `json:"blockHash"`
	MerkleRoot        string   `json:"merkleRoot"` // Hex-encoded merkle root
	CIDCount          int      `json:"cidCount"`
	CIDs              []string `json:"cids"`              // Sorted CID strings for Merkle verification
	SignatureType     string   `json:"signatureType"`     // ES256K or Ed25519
	SignatureIdentity string   `json:"signatureIdentity"` // Hex-encoded public key
	SignatureValue    string   `json:"signatureValue"`    // Hex-encoded signature
	CreatedAt         string   `json:"createdAt"`
}

// BlockSignatureCache stores block signatures indexed by block number
type BlockSignatureCache struct {
	mu         sync.RWMutex
	signatures map[int64]*BlockSignature
	maxSize    int
}

// NewBlockSignatureCache creates a new block signature cache
func NewBlockSignatureCache(maxSize int) *BlockSignatureCache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &BlockSignatureCache{
		signatures: make(map[int64]*BlockSignature),
		maxSize:    maxSize,
	}
}

// Add adds a block signature to the cache
func (c *BlockSignatureCache) Add(sig *BlockSignature) {
	if sig == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signatures[sig.BlockNumber] = sig
	if len(c.signatures) > c.maxSize {
		var minBlock int64 = -1
		for blockNum := range c.signatures {
			if minBlock == -1 || blockNum < minBlock {
				minBlock = blockNum
			}
		}
		if minBlock >= 0 {
			delete(c.signatures, minBlock)
		}
	}
}

// Get retrieves a block signature by block number
func (c *BlockSignatureCache) Get(blockNumber int64) (*BlockSignature, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sig, ok := c.signatures[blockNumber]
	return sig, ok
}

// BlockSignatureVerifier verifies block signatures and their CIDs
type BlockSignatureVerifier struct {
	cache *BlockSignatureCache
}

// NewBlockSignatureVerifier creates a new block signature verifier
func NewBlockSignatureVerifier(cacheSize int) *BlockSignatureVerifier {
	return &BlockSignatureVerifier{
		cache: NewBlockSignatureCache(cacheSize),
	}
}

// AddBlockSignature adds a block signature to the verifier's cache
func (v *BlockSignatureVerifier) AddBlockSignature(sig *BlockSignature) {
	v.cache.Add(sig)
}

// GetBlockSignature retrieves a block signature by block number
func (v *BlockSignatureVerifier) GetBlockSignature(blockNumber int64) (*BlockSignature, bool) {
	return v.cache.Get(blockNumber)
}

// VerifyBlockSignature verifies that a block signature is valid
func (v *BlockSignatureVerifier) VerifyBlockSignature(ctx context.Context, sig *BlockSignature) error {
	if sig == nil {
		return fmt.Errorf("block signature is nil")
	}

	merkleRoot, err := hex.DecodeString(sig.MerkleRoot)
	if err != nil {
		return fmt.Errorf("failed to decode merkle root: %w", err)
	}

	sigValue, err := hex.DecodeString(sig.SignatureValue)
	if err != nil {
		return fmt.Errorf("failed to decode signature value: %w", err)
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
		return fmt.Errorf("failed to parse public key: %w", err)
	}

	valid, err := pubKey.Verify(merkleRoot, sigValue)
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return fmt.Errorf("block signature verification failed for block %d", sig.BlockNumber)
	}

	return nil
}

// VerifyCIDsAgainstBlockSignature verifies that a list of CIDs matches a block signature's merkle root
func (v *BlockSignatureVerifier) VerifyCIDsAgainstBlockSignature(cids []string, sig *BlockSignature) (bool, error) {
	if sig == nil {
		return false, fmt.Errorf("block signature is nil")
	}

	computedRoot := ComputeMerkleRootFromStrings(cids)
	if computedRoot == nil {
		return false, fmt.Errorf("failed to compute merkle root from CIDs")
	}

	expectedRoot, err := hex.DecodeString(sig.MerkleRoot)
	if err != nil {
		return false, fmt.Errorf("failed to decode merkle root: %w", err)
	}

	if len(computedRoot) != len(expectedRoot) {
		return false, nil
	}
	for i := range computedRoot {
		if computedRoot[i] != expectedRoot[i] {
			return false, nil
		}
	}

	return true, nil
}

// VerifyCIDListAgainstMerkleRoot verifies that the CID list stored on a
// BlockSignature matches its Merkle root.
func (v *BlockSignatureVerifier) VerifyCIDListAgainstMerkleRoot(sig *BlockSignature) (bool, error) {
	if sig == nil || len(sig.CIDs) == 0 {
		return false, nil
	}
	return v.VerifyCIDsAgainstBlockSignature(sig.CIDs, sig)
}

// ComputeMerkleRootFromStrings computes a merkle root from CID strings
// This must match defradb/internal/core/block/block_signing.go
func ComputeMerkleRootFromStrings(cidStrings []string) []byte {
	if len(cidStrings) == 0 {
		return nil
	}

	parsedCids := make([]gocid.Cid, 0, len(cidStrings))
	for _, cidStr := range cidStrings {
		c, err := gocid.Decode(cidStr)
		if err != nil {
			logger.Sugar.Warnf("Failed to parse CID %s: %v", cidStr, err)
			continue
		}
		parsedCids = append(parsedCids, c)
	}

	if len(parsedCids) == 0 {
		return nil
	}

	sort.Slice(parsedCids, func(i, j int) bool {
		return bytes.Compare(parsedCids[i].Bytes(), parsedCids[j].Bytes()) < 0
	})

	hashes := make([][]byte, len(parsedCids))
	for i, c := range parsedCids {
		hash := sha256.Sum256(c.Bytes())
		hashes[i] = hash[:]
	}

	for len(hashes) > 1 {
		var newHashes [][]byte
		for i := 0; i < len(hashes); i += 2 {
			if i+1 < len(hashes) {
				combined := append(hashes[i], hashes[i+1]...)
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

// BlockCIDCollector collects CIDs for documents in a block for block signature verification
type BlockCIDCollector struct {
	mu        sync.Mutex
	blockCIDs map[int64][]string
	blockDocs map[int64][]string
	docTypes  map[string]string
}

// NewBlockCIDCollector creates a new block CID collector
func NewBlockCIDCollector() *BlockCIDCollector {
	return &BlockCIDCollector{
		blockCIDs: make(map[int64][]string),
		blockDocs: make(map[int64][]string),
		docTypes:  make(map[string]string),
	}
}

// AddDocumentCID adds a document's CID to the collector
func (c *BlockCIDCollector) AddDocumentCID(blockNumber int64, docID string, docType string, cid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blockCIDs[blockNumber] = append(c.blockCIDs[blockNumber], cid)
	c.blockDocs[blockNumber] = append(c.blockDocs[blockNumber], docID)
	c.docTypes[docID] = docType
}

// GetBlockCIDs returns all CIDs collected for a block
func (c *BlockCIDCollector) GetBlockCIDs(blockNumber int64) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	cids, ok := c.blockCIDs[blockNumber]
	if !ok {
		return nil
	}
	result := make([]string, len(cids))
	copy(result, cids)
	return result
}

// ClearBlock removes all collected data for a block (call after processing)
func (c *BlockCIDCollector) ClearBlock(blockNumber int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	docs := c.blockDocs[blockNumber]
	for _, docID := range docs {
		delete(c.docTypes, docID)
	}
	delete(c.blockCIDs, blockNumber)
	delete(c.blockDocs, blockNumber)
}
