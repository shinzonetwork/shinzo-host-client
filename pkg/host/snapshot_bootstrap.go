package host

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/snapshot"
	"github.com/sourcenetwork/defradb/node"

	hostConfig "github.com/shinzonetwork/shinzo-host-client/config"
)

// bootstrapFromSnapshots downloads and imports historical snapshots before P2P starts.
func bootstrapFromSnapshots(ctx context.Context, defraNode *node.Node, snapCfg hostConfig.SnapshotConfig) {
	logger.Sugar.Infof("Bootstrapping from snapshots (indexer: %s, ranges: %d)",
		snapCfg.IndexerURL, len(snapCfg.HistoricalRanges))

	client := snapshot.NewClient(snapCfg.IndexerURL)

	available, err := client.ListSnapshots()
	if err != nil {
		logger.Sugar.Warnf("Failed to list snapshots from indexer: %v", err)
		return
	}

	if len(available) == 0 {
		logger.Sugar.Info("No snapshots available from indexer")
		return
	}

	// Query existing block range to skip already-imported snapshots.
	existingMin, existingMax := getExistingBlockRange(ctx, defraNode)
	if existingMax > 0 {
		logger.Sugar.Infof("Existing blocks in DB: %d-%d", existingMin, existingMax)
	}

	needed := findCoveringSnapshots(available, snapCfg.HistoricalRanges, existingMin, existingMax)
	if len(needed) == 0 {
		logger.Sugar.Info("No new snapshots needed (all ranges already covered or no signed snapshots)")
		return
	}

	logger.Sugar.Infof("Found %d snapshots to import (skipped already-imported ranges)", len(needed))

	tmpDir := os.TempDir()
	var imported int

	for _, snap := range needed {
		sig := snap.Signature
		if sig == nil {
			logger.Sugar.Warnf("Snapshot %s is marked signed but has no signature data", snap.Filename)
			continue
		}

		tmpPath := filepath.Join(tmpDir, snap.Filename)
		logger.Sugar.Infof("Downloading snapshot %s (%d bytes)...", snap.Filename, snap.SizeBytes)
		if err := client.DownloadSnapshot(snap.Filename, tmpPath); err != nil {
			logger.Sugar.Warnf("Failed to download snapshot %s: %v", snap.Filename, err)
			continue
		}

		result, err := snapshot.ImportWithVerification(ctx, defraNode, tmpPath, sig)
		os.Remove(tmpPath)
		if err != nil {
			logger.Sugar.Warnf("Failed to import snapshot %s: %v", snap.Filename, err)
			continue
		}

		logger.Sugar.Infof("Imported snapshot %s: blocks %d-%d",
			snap.Filename, result.StartBlock, result.EndBlock)

		createSnapshotAttestation(ctx, defraNode, sig)
		imported++
	}

	// Rebuild indexes once after all snapshots are imported (not per-snapshot).
	if imported > 0 {
		logger.Sugar.Infof("Rebuilding indexes for %d collections...", len(constants.AllCollections))
		if err := snapshot.RebuildAllIndexes(ctx, defraNode, constants.AllCollections); err != nil {
			logger.Sugar.Warnf("Failed to rebuild indexes after snapshot import: %v", err)
		}
	}

	logger.Sugar.Infof("Snapshot bootstrap complete: %d/%d snapshots imported", imported, len(needed))
}

// findCoveringSnapshots returns signed snapshots overlapping the requested ranges,
// skipping snapshots whose block range is already fully present in the DB.
func findCoveringSnapshots(available []snapshot.SnapshotInfo, ranges []hostConfig.BlockRange, existingMin, existingMax int64) []snapshot.SnapshotInfo {
	var needed []snapshot.SnapshotInfo
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

// getExistingBlockRange queries DefraDB for the min and max block numbers already stored.
// Returns (0, 0) if no blocks exist or on error.
func getExistingBlockRange(ctx context.Context, defraNode *node.Node) (int64, int64) {
	type blockResult struct {
		Number int64 `json:"number"`
	}
	type queryResult struct {
		Block []blockResult `json:"Ethereum__Mainnet__Block"`
	}

	// Get highest block.
	maxQuery := fmt.Sprintf(`query { %s(order: {number: DESC}, limit: 1) { number } }`, constants.CollectionBlock)
	maxResult := defraNode.DB.ExecRequest(ctx, maxQuery)
	if maxResult.GQL.Errors != nil {
		return 0, 0
	}
	jsonBytes, err := json.Marshal(maxResult.GQL.Data)
	if err != nil {
		return 0, 0
	}
	var maxQR queryResult
	if err := json.Unmarshal(jsonBytes, &maxQR); err != nil || len(maxQR.Block) == 0 {
		return 0, 0
	}
	maxBlock := maxQR.Block[0].Number

	// Get lowest block.
	minQuery := fmt.Sprintf(`query { %s(order: {number: ASC}, limit: 1) { number } }`, constants.CollectionBlock)
	minResult := defraNode.DB.ExecRequest(ctx, minQuery)
	if minResult.GQL.Errors != nil {
		return 0, maxBlock
	}
	jsonBytes, err = json.Marshal(minResult.GQL.Data)
	if err != nil {
		return 0, maxBlock
	}
	var minQR queryResult
	if err := json.Unmarshal(jsonBytes, &minQR); err != nil || len(minQR.Block) == 0 {
		return 0, maxBlock
	}

	return minQR.Block[0].Number, maxBlock
}

// createSnapshotAttestation records an attestation for an imported snapshot.
func createSnapshotAttestation(ctx context.Context, defraNode *node.Node, sig *snapshot.SnapshotSignatureData) {
	if len(sig.BlockSigMerkleRoots) == 0 {
		logger.Sugar.Warnf("Skipping attestation for snapshot %d-%d: no block sig merkle roots",
			sig.StartBlock, sig.EndBlock)
		return
	}

	record := &constants.AttestationRecord{
		AttestedDocId: fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock),
		SourceDocId:   sig.SignatureIdentity,
		CIDs:          sig.BlockSigMerkleRoots,
		DocType:       "Snapshot",
		VoteCount:     1,
	}

	if err := attestationService.PostAttestationRecord(ctx, defraNode, record); err != nil {
		logger.Sugar.Warnf("Failed to create attestation for snapshot %d-%d: %v",
			sig.StartBlock, sig.EndBlock, err)
	} else {
		logger.Sugar.Infof("Created attestation for snapshot %d-%d (signer: %s)",
			sig.StartBlock, sig.EndBlock, truncateString(sig.SignatureIdentity, 16))
	}
}
