package host

import (
	"context"
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

	needed := findCoveringSnapshots(available, snapCfg.HistoricalRanges)
	if len(needed) == 0 {
		logger.Sugar.Info("No signed snapshots found for configured ranges")
		return
	}

	logger.Sugar.Infof("Found %d snapshots covering configured ranges", len(needed))

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

// findCoveringSnapshots returns signed snapshots overlapping the requested ranges.
func findCoveringSnapshots(available []snapshot.SnapshotInfo, ranges []hostConfig.BlockRange) []snapshot.SnapshotInfo {
	var needed []snapshot.SnapshotInfo
	seen := make(map[string]bool)

	for _, r := range ranges {
		for _, snap := range available {
			if snap.EndBlock < r.Start || snap.StartBlock > r.End {
				continue // no overlap
			}
			if !snap.Signed {
				continue // only import signed snapshots
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

// createSnapshotAttestation records an attestation for an imported snapshot.
func createSnapshotAttestation(ctx context.Context, defraNode *node.Node, sig *snapshot.SnapshotSignatureData) {
	record := &constants.AttestationRecord{
		AttestedDocId: fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock),
		SourceDocId:   sig.SignatureIdentity,
		CIDs:          []string{sig.MerkleRoot},
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
