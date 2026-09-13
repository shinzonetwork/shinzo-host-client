package host

import (
	"context"
	"path/filepath"

	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

func pruneCollections() pruner.CollectionConfig {
	return pruner.CollectionConfig{
		BlockCollection:  constants.CollectionBlock,
		BlockNumberField: "number",
		DependentCollections: []string{
			constants.CollectionAccessListEntry,
			constants.CollectionLog,
			constants.CollectionTransaction,
			constants.CollectionBlockSignature,
			constants.CollectionAttestationRecord,
		},
	}
}

func (s *defraService) PruneDocuments(ctx context.Context) {
	if s.node == nil || !s.cfg.Pruner.Enabled {
		return
	}

	log := s.log.Sugar()
	pcfg := pruner.Config{
		Enabled:         s.cfg.Pruner.Enabled,
		MaxBlocks:       s.cfg.Pruner.MaxBlocks,
		DocsPerBlock:    s.cfg.Pruner.DocsPerBlock,
		IntervalSeconds: s.cfg.Pruner.IntervalSeconds,
		PruneHistory:    s.cfg.Pruner.PruneHistory,
	}
	pcfg.SetDefaults()

	collections := pruneCollections()

	queue := pruner.NewEventQueue(collections)
	queuePath := filepath.Join(s.cfg.Store.Path, "prune_queue.gob")
	if loaded, err := queue.LoadFromFile(queuePath); err != nil {
		log.Warnw("failed to load prune queue from disk", "error", err)
	} else if loaded > 0 {
		log.Infow("restored prune queue from disk", "entries", loaded)
	}

	p := pruner.NewPruner(&pcfg, s.node, collections)
	p.SetQueue(queue)
	if err := p.Start(ctx); err != nil {
		log.Warnw("failed to start pruner", "error", err)
		return
	}
	s.pruner = p

	go s.feedPruneQueue(ctx, s.node, queue, collections)
}

func (s *defraService) feedPruneQueue(ctx context.Context, defraNode *node.Node, queue *pruner.EventQueue, collections pruner.CollectionConfig) {
	log := s.log.Sugar()

	names := append([]string{collections.BlockCollection}, collections.DependentCollections...)
	collectionIDToName := make(map[string]string, len(names))
	for _, name := range names {
		col, err := defraNode.DB.GetCollectionByName(ctx, name)
		if err != nil {
			log.Warnw("pruning: collection unavailable, skipping", "collection", name, "error", err)
			continue
		}
		collectionIDToName[col.CollectionID()] = name
	}
	if len(collectionIDToName) == 0 {
		log.Warn("pruning disabled, no tracked collections available")
		return
	}

	sub, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		log.Warnw("pruning disabled, event subscription failed", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Message():
			if !ok {
				return
			}
			update, ok := msg.Data.(event.Update)
			if !ok {
				continue
			}
			name, known := collectionIDToName[update.CollectionID]
			if !known {
				continue
			}
			queue.Push(name, update.DocID)
		}
	}
}
