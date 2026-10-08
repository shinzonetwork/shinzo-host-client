package host

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/snapshot"
	"github.com/sourcenetwork/defradb/node"

	hostConfig "github.com/shinzonetwork/shinzo-host-client/config"
)

// bootstrapFromSnapshots imports historical snapshots from an indexer to seed the local DB.
func bootstrapFromSnapshots(ctx context.Context, defraNode *node.Node, collections chain.Collections, snapCfg hostConfig.SnapshotConfig) {
	logger.Sugar.Infof("Bootstrapping from snapshots (indexer: %s, ranges: %d)",
		snapCfg.IndexerURL, len(snapCfg.HistoricalRanges))

	needed, client, err := resolveNeededSnapshots(ctx, defraNode, collections.Block.Name, snapCfg)
	if err != nil || len(needed) == 0 {
		return
	}

	imported := importSnapshots(ctx, defraNode, collections, client, needed)

	if imported > 0 {
		rebuildIndexes(ctx, defraNode, collections.Subscribed())
	}

	logger.Sugar.Infof("Snapshot bootstrap complete: %d/%d snapshots imported", imported, len(needed))
}

// resolveNeededSnapshots lists available snapshots and filters to those not yet imported.
func resolveNeededSnapshots(ctx context.Context, defraNode *node.Node, blockCollection string, snapCfg hostConfig.SnapshotConfig) ([]snapshot.Info, *snapshot.Client, error) {
	client := snapshot.NewClient(snapCfg.IndexerURL)

	available, err := client.ListSnapshots()
	if err != nil {
		logger.Sugar.Warnf("Failed to list snapshots from indexer: %v", err)
		return nil, nil, err
	}
	if len(available) == 0 {
		logger.Sugar.Info("No snapshots available from indexer")
		return nil, nil, nil
	}

	existingMin, existingMax := getExistingBlockRange(ctx, defraNode, blockCollection)
	if existingMax > 0 {
		logger.Sugar.Infof("Existing blocks in DB: %d-%d", existingMin, existingMax)
	}

	needed := findCoveringSnapshots(available, snapCfg.HistoricalRanges, existingMin, existingMax)
	if len(needed) == 0 {
		logger.Sugar.Info("No new snapshots needed (all ranges already covered or no signed snapshots)")
		return nil, nil, nil
	}

	logger.Sugar.Infof("Found %d snapshots to import (skipped already-imported ranges)", len(needed))
	return needed, client, nil
}

// importSnapshots downloads and imports each snapshot, returning the count of successful imports.
func importSnapshots(ctx context.Context, defraNode *node.Node, collections chain.Collections, client *snapshot.Client, needed []snapshot.Info) int {
	tmpDir := os.TempDir()
	var imported int

	for _, snap := range needed {
		if err := importSingleSnapshot(ctx, defraNode, collections, client, snap, tmpDir); err != nil {
			logger.Sugar.Warnf("Failed to import snapshot %s: %v", snap.Filename, err)
			continue
		}
		imported++
	}

	return imported
}

// importSingleSnapshot downloads, verifies, and imports one snapshot.
func importSingleSnapshot(ctx context.Context, defraNode *node.Node, collections chain.Collections, client *snapshot.Client, snap snapshot.Info, tmpDir string) error {
	if snap.Signature == nil {
		logger.Sugar.Warnf("Snapshot %s is marked signed but has no signature data", snap.Filename)
		return fmt.Errorf("missing signature") // nolint:err113
	}

	tmpPath := filepath.Join(tmpDir, snap.Filename)
	logger.Sugar.Infof("Downloading snapshot %s (%d bytes)...", snap.Filename, snap.SizeBytes)
	if err := client.DownloadSnapshot(snap.Filename, tmpPath); err != nil {
		return fmt.Errorf("download: %w", err) // nolint:err113
	}

	result, err := snapshot.ImportWithVerification(ctx, defraNode, tmpPath, snap.Signature)
	_ = os.Remove(tmpPath)
	if err != nil {
		return fmt.Errorf("import: %w", err) // nolint:err113
	}

	logger.Sugar.Infof("Imported snapshot %s: blocks %d-%d", snap.Filename, result.StartBlock, result.EndBlock)
	createSnapshotAttestation(ctx, defraNode, collections.AttestationRecord.Name, snap.Signature)

	return nil
}

// rebuildIndexes rebuilds the indexes of the named collections after snapshot import.
func rebuildIndexes(ctx context.Context, defraNode *node.Node, names []string) {
	logger.Sugar.Infof("Rebuilding indexes for %d collections...", len(names))
	if err := snapshot.RebuildAllIndexes(ctx, defraNode, names); err != nil {
		logger.Sugar.Warnf("Failed to rebuild indexes after snapshot import: %v", err)
	}
}

// findCoveringSnapshots returns signed snapshots overlapping the requested ranges,
// skipping snapshots whose block range is already fully present in the DB.
func findCoveringSnapshots(available []snapshot.Info, ranges []hostConfig.BlockRange, existingMin, existingMax int64) []snapshot.Info {
	var needed []snapshot.Info
	seen := make(map[string]bool)

	for _, r := range ranges {
		for _, snap := range available {
			if snap.EndBlock < r.Start || snap.StartBlock > r.End {
				continue // no overlap with requested range
			}
			if !snap.Signed {
				continue // only import signed snapshots
			}
			// Skip if this snapshot's entire range is already in the DB.
			if existingMax > 0 && snap.StartBlock >= existingMin && snap.EndBlock <= existingMax {
				continue
			}
			if !seen[snap.Filename] {
				seen[snap.Filename] = true
				needed = append(needed, snap)
			}
		}
	}

	sort.Slice(needed, func(i, j int) bool {
		return needed[i].StartBlock < needed[j].StartBlock
	})

	return needed
}

// getExistingBlockRange queries the block collection for the min and max block numbers already
// stored. Returns (0, 0) if no blocks exist or on error.
func getExistingBlockRange(ctx context.Context, defraNode *node.Node, blockCollection string) (int64, int64) {
	type blockResult struct {
		Number int64 `json:"number"`
	}
	// The result is keyed by the name of the collection queried.
	type queryResult map[string][]blockResult

	// Get highest block.
	maxQuery := fmt.Sprintf(`query { %s(order: {number: DESC}, limit: 1) { number } }`, blockCollection)
	maxResult := defraNode.DB.ExecRequest(ctx, maxQuery)
	if maxResult.GQL.Errors != nil {
		return 0, 0
	}
	jsonBytes, err := json.Marshal(maxResult.GQL.Data)
	if err != nil {
		return 0, 0
	}
	var maxQR queryResult
	if err := json.Unmarshal(jsonBytes, &maxQR); err != nil || len(maxQR[blockCollection]) == 0 {
		return 0, 0
	}
	maxBlock := maxQR[blockCollection][0].Number

	// Get lowest block.
	minQuery := fmt.Sprintf(`query { %s(order: {number: ASC}, limit: 1) { number } }`, blockCollection)
	minResult := defraNode.DB.ExecRequest(ctx, minQuery)
	if minResult.GQL.Errors != nil {
		return 0, maxBlock
	}
	jsonBytes, err = json.Marshal(minResult.GQL.Data)
	if err != nil {
		return 0, maxBlock
	}
	var minQR queryResult
	if err := json.Unmarshal(jsonBytes, &minQR); err != nil || len(minQR[blockCollection]) == 0 {
		return 0, maxBlock
	}

	return minQR[blockCollection][0].Number, maxBlock
}

// createSnapshotAttestation records an attestation for an imported snapshot.
func createSnapshotAttestation(ctx context.Context, defraNode *node.Node, collection string, sig *snapshot.SignatureData) {
	if len(sig.BlockSigMerkleRoots) == 0 {
		logger.Sugar.Warnf("Skipping attestation for snapshot %d-%d: no block sig merkle roots",
			sig.StartBlock, sig.EndBlock)
		return
	}

	record := &constants.AttestationRecord{
		AttestedDocID: fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock),
		SourceDocIDs:  []string{sig.SignatureIdentity},
		CIDs:          sig.BlockSigMerkleRoots,
		DocType:       "Snapshot",
		VoteCount:     1,
		// A snapshot covers a range; retention follows the newest block in it.
		BlockNumber: &sig.EndBlock,
	}

	if err := attestationService.PostAttestationRecord(ctx, defraNode, collection, record); err != nil {
		logger.Sugar.Warnf("Failed to create attestation for snapshot %d-%d: %v",
			sig.StartBlock, sig.EndBlock, err)
	} else {
		logger.Sugar.Infof("Created attestation for snapshot %d-%d (signer: %s)",
			sig.StartBlock, sig.EndBlock, truncateString(sig.SignatureIdentity, identityTruncateLength))
	}
}
