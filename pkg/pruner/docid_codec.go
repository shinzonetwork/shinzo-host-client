package pruner

import (
	"encoding/hex"
	"strings"
	"sync"
)

// docID encoding helpers shared by every queue implementation. DefraDB
// document IDs follow the form "<prefix>-<uuid-with-hyphens>"; storing them
// in queue snapshots as fixed-size 16-byte UUIDs avoids the per-entry
// overhead of carrying the prefix string. The prefix is captured the first
// time a docID is parsed and reused thereafter.

const uuidSize = 16

// docIDPrefix is the constant version prefix shared by all DefraDB document IDs.
var (
	docIDPrefix     string    //nolint:gochecknoglobals
	docIDPrefixOnce sync.Once //nolint:gochecknoglobals
)

// extractUUID extracts the 16-byte UUID from a docID string. Captures the
// docID prefix on first call so RestoreDocID can rebuild full IDs later.
func extractUUID(docID string) ([uuidSize]byte, error) {
	idx := strings.IndexByte(docID, '-')
	if idx < 0 {
		return [uuidSize]byte{}, ErrInvalidDocIDFormat
	}
	docIDPrefixOnce.Do(func() {
		docIDPrefix = docID[:idx]
	})
	return parseUUIDHex(docID[idx+1:])
}

// parseUUIDHex parses a UUID string (with hyphens) into 16 raw bytes.
func parseUUIDHex(uuidStr string) ([uuidSize]byte, error) {
	clean := strings.ReplaceAll(uuidStr, "-", "")
	if len(clean) != 32 { //nolint:mnd
		return [uuidSize]byte{}, ErrInvalidUUID
	}
	var result [uuidSize]byte
	_, err := hex.Decode(result[:], []byte(clean))
	return result, err
}

// formatUUID formats 16 raw bytes as a UUID string with hyphens.
func formatUUID(b [uuidSize]byte) string {
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// RestoreDocID reconstructs a full docID string from packed UUID bytes.
// Requires extractUUID to have been called at least once on a real docID
// to capture the prefix; before that, this returns "-<uuid>" with an empty
// prefix.
func RestoreDocID(uuid [uuidSize]byte) string {
	return docIDPrefix + "-" + formatUUID(uuid)
}
