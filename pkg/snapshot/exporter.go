package snapshot

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// WriteSnapshotParams holds everything needed to create a v2 KV snapshot.
type WriteSnapshotParams struct {
	StartBlock          int64
	EndBlock            int64
	CreatedAt           string
	BlockSigMerkleRoots []string
	// Collections is the ordered list of collection names to export.
	Collections []string
	// DocIDsByCollection maps each collection name to the document IDs to export.
	DocIDsByCollection map[string][]string
}

// WriteSnapshot writes a gzip-compressed v2 KV snapshot to w.
//
// Wire format (inside gzip):
//
//	[4-byte header_len][header JSON]
//	[4-byte name_len][name][KV pairs + uint32(0) sentinel]  ← one per collection
//	[uint32(0)]  ← outer terminator
func WriteSnapshot(ctx context.Context, defraNode *node.Node, params WriteSnapshotParams, w io.Writer) (err error) {
	gw := gzip.NewWriter(w)
	defer func() {
		if cerr := gw.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// Collect field mappings for each collection before writing the header.
	fieldMappings := make(map[string]json.RawMessage, len(params.Collections))
	for _, colName := range params.Collections {
		mapping, merr := defraNode.DB.ExportFieldMapping(ctx, colName)
		if merr != nil {
			logger.Sugar.Warnf("failed to export field mapping for %q: %v", colName, merr)
			continue
		}
		fieldMappings[colName] = json.RawMessage(mapping)
	}

	header := kvSnapshotHeader{
		Magic:               snapshotMagic,
		Version:             snapshotVersion,
		StartBlock:          params.StartBlock,
		EndBlock:            params.EndBlock,
		CreatedAt:           params.CreatedAt,
		BlockSigMerkleRoots: params.BlockSigMerkleRoots,
		FieldMappings:       fieldMappings,
	}
	headerBytes, merr := json.Marshal(header)
	if merr != nil {
		return fmt.Errorf("marshal header: %w", merr)
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(headerBytes))) //nolint:gosec
	if _, werr := gw.Write(lenBuf[:]); werr != nil {
		return fmt.Errorf("write header length: %w", werr)
	}
	if _, werr := gw.Write(headerBytes); werr != nil {
		return fmt.Errorf("write header: %w", werr)
	}

	// Write one named section per collection.
	for _, colName := range params.Collections {
		nameBytes := []byte(colName)
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(nameBytes))) //nolint:gosec
		if _, werr := gw.Write(lenBuf[:]); werr != nil {
			return fmt.Errorf("write section name length for %q: %w", colName, werr)
		}
		if _, werr := gw.Write(nameBytes); werr != nil {
			return fmt.Errorf("write section name for %q: %w", colName, werr)
		}

		docIDs := params.DocIDsByCollection[colName]
		if _, werr := defraNode.DB.ExportDocKVs(ctx, colName, docIDs, gw, false); werr != nil {
			return fmt.Errorf("export KVs for %q: %w", colName, werr)
		}
	}

	// Outer terminator: name_len == 0 signals no more sections.
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	if _, werr := gw.Write(lenBuf[:]); werr != nil {
		return fmt.Errorf("write outer terminator: %w", werr)
	}

	return nil
}
