package attestation

import (
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

// BatchSignature represents a batch signature from an indexer
type BatchSignature struct {
	BlockNumber       int64  `json:"blockNumber"`
	BlockHash         string `json:"blockHash"`
	MerkleRoot        string `json:"merkleRoot"` // Hex-encoded merkle root
	CIDCount          int    `json:"cidCount"`
	SignatureType     string `json:"signatureType"`     // ES256K or Ed25519
	SignatureIdentity string `json:"signatureIdentity"` // Hex-encoded public key
	SignatureValue    string `json:"signatureValue"`    // Hex-encoded signature
	CreatedAt         string `json:"createdAt"`
}

// BatchSignatureCache stores batch signatures indexed by block number
type BatchSignatureCache struct {
	mu         sync.RWMutex
	signatures map[int64]*BatchSignature
	maxSize    int
}

// NewBatchSignatureCache creates a new batch signature cache
func NewBatchSignatureCache(maxSize int) *BatchSignatureCache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &BatchSignatureCache{
		signatures: make(map[int64]*BatchSignature),
		maxSize:    maxSize,
	}
}

// Add adds a batch signature to the cache
func (c *BatchSignatureCache) Add(sig *BatchSignature) {
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

// Get retrieves a batch signature by block number
func (c *BatchSignatureCache) Get(blockNumber int64) (*BatchSignature, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sig, ok := c.signatures[blockNumber]
	return sig, ok
}

// BatchSignatureVerifier verifies batch signatures and their CIDs
type BatchSignatureVerifier struct {
	cache *BatchSignatureCache
}

// NewBatchSignatureVerifier creates a new batch signature verifier
func NewBatchSignatureVerifier(cacheSize int) *BatchSignatureVerifier {
	return &BatchSignatureVerifier{
		cache: NewBatchSignatureCache(cacheSize),
	}
}

// AddBatchSignature adds a batch signature to the verifier's cache
func (v *BatchSignatureVerifier) AddBatchSignature(sig *BatchSignature) {
	v.cache.Add(sig)
}

// GetBatchSignature retrieves a batch signature by block number
func (v *BatchSignatureVerifier) GetBatchSignature(blockNumber int64) (*BatchSignature, bool) {
	return v.cache.Get(blockNumber)
}

// VerifyBatchSignature verifies that a batch signature is valid
func (v *BatchSignatureVerifier) VerifyBatchSignature(ctx context.Context, sig *BatchSignature) error {
	if sig == nil {
		return fmt.Errorf("batch signature is nil")
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
		return fmt.Errorf("batch signature verification failed for block %d", sig.BlockNumber)
	}

	return nil
}

// VerifyCIDsAgainstBatchSignature verifies that a list of CIDs matches a batch signature's merkle root
func (v *BatchSignatureVerifier) VerifyCIDsAgainstBatchSignature(cids []string, sig *BatchSignature) (bool, error) {
	if sig == nil {
		return false, fmt.Errorf("batch signature is nil")
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

// ComputeMerkleRootFromStrings computes a merkle root from CID strings
// This must match defradb/internal/core/block/batch_signing.go
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
		return parsedCids[i].String() < parsedCids[j].String()
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

// BlockCIDCollector collects CIDs for documents in a block for batch verification
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
