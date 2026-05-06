package pruner

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"
)

// eventEntry is a single tracked document from a P2P replication event.
// Uses packed UUID (16 bytes) + 1 byte collection type for compact storage.
type eventEntry struct {
	Collection byte           // collection type enum
	UUID       [uuidSize]byte // packed UUID (16 bytes)
}

// Collection type constants for compact storage.
const (
	colBlock    byte = 0
	colTx       byte = 1
	colLog      byte = 2
	colALE      byte = 3
	colBatchSig byte = 4
)

// eventQueueSnapshot is the serializable form of the event queue.
type eventQueueSnapshot struct {
	DocIDPrefix     string
	Entries         []eventEntry
	BlockCount      int
	CollectionNames map[byte]string // maps enum → collection name
}

// EventQueue is a FIFO queue that tracks document IDs from P2P replication events.
// Documents are stored in arrival order. When pruning, oldest entries are drained
// until enough Block entries have been consumed.
// Storage: 17 bytes per entry (1 byte collection type + 16 bytes UUID).
type EventQueue struct {
	mu              sync.Mutex
	entries         []eventEntry
	blockCount      int
	filePath        string
	collectionNames map[byte]string // maps enum → collection name
	collectionEnums map[string]byte // maps collection name → enum
}

// NewEventQueue creates a new empty event queue.
// blockCollection is the name of the Block collection (used to identify block entries).
func NewEventQueue(collections CollectionConfig) *EventQueue {
	q := &EventQueue{
		entries:         make([]eventEntry, 0, 1024),
		collectionNames: make(map[byte]string),
		collectionEnums: make(map[string]byte),
	}

	// Register block collection
	q.collectionNames[colBlock] = collections.BlockCollection
	q.collectionEnums[collections.BlockCollection] = colBlock

	// Register dependent collections with known enums
	knownCollections := map[string]byte{
		"Ethereum__Mainnet__Transaction":     colTx,
		"Ethereum__Mainnet__Log":             colLog,
		"Ethereum__Mainnet__AccessListEntry": colALE,
		"Ethereum__Mainnet__BatchSignature":  colBatchSig,
	}

	for _, name := range collections.DependentCollections {
		if enum, ok := knownCollections[name]; ok {
			q.collectionNames[enum] = name
			q.collectionEnums[name] = enum
		}
	}

	return q
}

// Push adds a document to the queue. O(1) operation, no doc fetch needed.
func (q *EventQueue) Push(collectionName, docID string) {
	uuid, err := extractUUID(docID)
	if err != nil {
		return // skip invalid docIDs silently
	}

	colType, ok := q.collectionEnums[collectionName]
	if !ok {
		return // skip unknown collections
	}

	q.mu.Lock()
	q.entries = append(q.entries, eventEntry{
		Collection: colType,
		UUID:       uuid,
	})
	if colType == colBlock {
		q.blockCount++
	}
	q.mu.Unlock()
}

// DrainDocs removes `count` entries from the front of the FIFO queue.
func (q *EventQueue) DrainDocs(count int) *DrainResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.entries) == 0 || count <= 0 {
		return nil
	}

	if count > len(q.entries) {
		count = len(q.entries)
	}

	drained := make([]eventEntry, count)
	copy(drained, q.entries[:count])

	remaining := make([]eventEntry, len(q.entries)-count)
	copy(remaining, q.entries[count:])
	q.entries = remaining

	// Count blocks in drained set and update blockCount
	blocksSeen := 0
	for _, entry := range drained {
		if entry.Collection == colBlock {
			blocksSeen++
		}
	}
	q.blockCount -= blocksSeen

	// Group by collection name
	result := &DrainResult{
		DocIDsByCollection: make(map[string][]string),
		BlockCount:         blocksSeen,
	}

	for _, entry := range drained {
		name, ok := q.collectionNames[entry.Collection]
		if !ok {
			continue
		}
		docID := RestoreDocID(entry.UUID)
		result.DocIDsByCollection[name] = append(result.DocIDsByCollection[name], docID)
	}

	return result
}

// DrainBlocks removes entries from the front until `count` Block entries
// have been consumed. All non-Block entries between those blocks are also removed.
// Returns a DrainResult with docIDs grouped by collection name.
func (q *EventQueue) DrainBlocks(count int) *DrainResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.blockCount <= 0 || count <= 0 {
		return nil
	}

	// Walk from front, count Block entries
	blocksSeen := 0
	cutoff := 0
	for i, entry := range q.entries {
		if entry.Collection == colBlock {
			blocksSeen++
			if blocksSeen >= count {
				cutoff = i + 1
				break
			}
		}
	}

	if cutoff == 0 {
		return nil
	}

	// Drain entries [0, cutoff)
	drained := make([]eventEntry, cutoff)
	copy(drained, q.entries[:cutoff])

	remaining := make([]eventEntry, len(q.entries)-cutoff)
	copy(remaining, q.entries[cutoff:])
	q.entries = remaining
	q.blockCount -= blocksSeen

	// Group by collection name
	result := &DrainResult{
		DocIDsByCollection: make(map[string][]string),
		BlockCount:         blocksSeen,
	}

	for _, entry := range drained {
		name, ok := q.collectionNames[entry.Collection]
		if !ok {
			continue
		}
		docID := RestoreDocID(entry.UUID)
		result.DocIDsByCollection[name] = append(result.DocIDsByCollection[name], docID)
	}

	return result
}

// Len returns the total number of entries in the queue.
func (q *EventQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// BlockCount returns the number of Block entries in the queue.
func (q *EventQueue) BlockCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.blockCount
}

// LoadFromFile loads queue entries from a gob-encoded file.
func (q *EventQueue) LoadFromFile(path string) (int, error) {
	q.filePath = path

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to open queue file: %w", err)
	}
	defer f.Close()

	var snap eventQueueSnapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return 0, fmt.Errorf("failed to decode queue file: %w", err)
	}

	q.mu.Lock()
	q.entries = snap.Entries
	q.blockCount = snap.BlockCount
	if snap.CollectionNames != nil {
		for k, v := range snap.CollectionNames {
			q.collectionNames[k] = v
			q.collectionEnums[v] = k
		}
	}
	count := len(q.entries)
	q.mu.Unlock()

	if snap.DocIDPrefix != "" {
		docIDPrefixOnce.Do(func() {
			docIDPrefix = snap.DocIDPrefix
		})
	}

	return count, nil
}

// Save persists the queue to disk using atomic write.
func (q *EventQueue) Save() error {
	if q.filePath == "" {
		return nil
	}

	q.mu.Lock()
	snap := eventQueueSnapshot{
		DocIDPrefix:     docIDPrefix,
		Entries:         make([]eventEntry, len(q.entries)),
		BlockCount:      q.blockCount,
		CollectionNames: q.collectionNames,
	}
	copy(snap.Entries, q.entries)
	q.mu.Unlock()

	if len(snap.Entries) == 0 {
		os.Remove(q.filePath)
		return nil
	}

	tmpPath := q.filePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if err := gob.NewEncoder(f).Encode(snap); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode queue: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, q.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}
