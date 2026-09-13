package host

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/snapshot"
)

func (s *defraService) Bootstrap(ctx context.Context) {
	if s.node == nil {
		return
	}
	snapCfg := s.cfg.Snapshot
	if !snapCfg.Enabled || snapCfg.IndexerURL == "" || len(snapCfg.HistoricalRanges) == 0 {
		return
	}

	log := s.log.Sugar()
	log.Infow("bootstrapping from snapshots", "indexer", snapCfg.IndexerURL, "ranges", len(snapCfg.HistoricalRanges))

	needed, snapClient, err := pendingSnapshots(ctx, s.node, snapCfg, log)
	if err != nil || len(needed) == 0 {
		return
	}

	imported := importPendingSnapshots(ctx, s.node, snapClient, needed, log)
	if imported > 0 {
		rebuildSnapshotIndexes(ctx, s.node, log)
	}

	log.Infow("snapshot bootstrap complete", "imported", imported, "needed", len(needed))
}

func pendingSnapshots(
	ctx context.Context,
	defraNode *node.Node,
	snapCfg config.SnapshotConfig,
	log *zap.SugaredLogger,
) ([]snapshot.Info, *snapshot.Client, error) {
	snapClient := snapshot.NewClient(snapCfg.IndexerURL)

	available, err := snapClient.ListSnapshots()
	if err != nil {
		log.Warnw("failed to list snapshots from indexer", "error", err)
		return nil, nil, err
	}
	if len(available) == 0 {
		log.Info("no snapshots available from indexer")
		return nil, nil, nil
	}

	existingMin, existingMax := existingBlockRange(ctx, defraNode)
	if existingMax > 0 {
		log.Infow("existing blocks in db", "min", existingMin, "max", existingMax)
	}

	needed := coveringSnapshots(available, snapCfg.HistoricalRanges, existingMin, existingMax)
	if len(needed) == 0 {
		log.Info("no new snapshots needed, ranges already covered")
		return nil, nil, nil
	}

	log.Infow("found snapshots to import", "count", len(needed))
	return needed, snapClient, nil
}

func coveringSnapshots(available []snapshot.Info, ranges []config.BlockRange, existingMin, existingMax int64) []snapshot.Info {
	var needed []snapshot.Info
	seen := make(map[string]bool)

	for _, r := range ranges {
		for _, snap := range available {
			if snap.EndBlock < r.Start || snap.StartBlock > r.End {
				continue
			}
			if !snap.Signed {
				continue
			}
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

func existingBlockRange(ctx context.Context, defraNode *node.Node) (int64, int64) {
	type blockResult struct {
		Number int64 `json:"number"`
	}
	type queryResult struct {
		Block []blockResult `json:"Ethereum__Mainnet__Block"`
	}

	maxQuery := fmt.Sprintf(`query { %s(order: {number: DESC}, limit: 1) { number } }`, constants.CollectionBlock)
	maxResult := defraNode.DB.ExecRequest(ctx, maxQuery)
	if maxResult.GQL.Errors != nil {
		return 0, 0
	}
	maxJSON, err := json.Marshal(maxResult.GQL.Data)
	if err != nil {
		return 0, 0
	}
	var maxQR queryResult
	if err := json.Unmarshal(maxJSON, &maxQR); err != nil || len(maxQR.Block) == 0 {
		return 0, 0
	}
	maxBlock := maxQR.Block[0].Number

	minQuery := fmt.Sprintf(`query { %s(order: {number: ASC}, limit: 1) { number } }`, constants.CollectionBlock)
	minResult := defraNode.DB.ExecRequest(ctx, minQuery)
	if minResult.GQL.Errors != nil {
		return 0, maxBlock
	}
	minJSON, err := json.Marshal(minResult.GQL.Data)
	if err != nil {
		return 0, maxBlock
	}
	var minQR queryResult
	if err := json.Unmarshal(minJSON, &minQR); err != nil || len(minQR.Block) == 0 {
		return 0, maxBlock
	}

	return minQR.Block[0].Number, maxBlock
}

func importPendingSnapshots(
	ctx context.Context,
	defraNode *node.Node,
	snapClient *snapshot.Client,
	needed []snapshot.Info,
	log *zap.SugaredLogger,
) int {
	tmpDir := os.TempDir()
	var imported int

	for _, snap := range needed {
		if err := importSnapshot(ctx, defraNode, snapClient, snap, tmpDir, log); err != nil {
			log.Warnw("failed to import snapshot", "file", snap.Filename, "error", err)
			continue
		}
		imported++
	}

	return imported
}

func importSnapshot(
	ctx context.Context,
	defraNode *node.Node,
	snapClient *snapshot.Client,
	snap snapshot.Info,
	tmpDir string,
	log *zap.SugaredLogger,
) error {
	if snap.Signature == nil {
		return fmt.Errorf("snapshot %s marked signed but has no signature data", snap.Filename)
	}

	tmpPath := filepath.Join(tmpDir, snap.Filename)
	log.Infow("downloading snapshot", "file", snap.Filename, "bytes", snap.SizeBytes)
	if err := snapClient.DownloadSnapshot(snap.Filename, tmpPath); err != nil {
		return fmt.Errorf("download: %w", err)
	}

	result, err := snapshot.ImportWithVerification(ctx, defraNode, tmpPath, snap.Signature)
	_ = os.Remove(tmpPath)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	log.Infow("imported snapshot", "file", snap.Filename, "start", result.StartBlock, "end", result.EndBlock)
	attestSnapshotImport(ctx, defraNode, snap.Signature, log)

	return nil
}

func rebuildSnapshotIndexes(ctx context.Context, defraNode *node.Node, log *zap.SugaredLogger) {
	if err := snapshot.RebuildAllIndexes(ctx, defraNode, constants.AllCollections); err != nil {
		log.Warnw("failed to rebuild indexes after snapshot import", "error", err)
	}
}

func attestSnapshotImport(ctx context.Context, defraNode *node.Node, sig *snapshot.SignatureData, log *zap.SugaredLogger) {
	if len(sig.BlockSigMerkleRoots) == 0 {
		log.Warnw("skipping attestation for snapshot, no block sig merkle roots", "start", sig.StartBlock, "end", sig.EndBlock)
		return
	}

	record := &attestation.Record{
		AttestedDocID: fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock),
		SourceDocIDs:  []string{sig.SignatureIdentity},
		CIDs:          sig.BlockSigMerkleRoots,
		DocType:       "Snapshot",
		VoteCount:     1,
	}

	if err := attestation.PostAttestationRecord(ctx, defraNode, record); err != nil {
		log.Warnw("failed to create attestation for snapshot", "start", sig.StartBlock, "end", sig.EndBlock, "error", err)
		return
	}
	log.Infow("created attestation for snapshot", "start", sig.StartBlock, "end", sig.EndBlock, "signer", sig.SignatureIdentity)
}
