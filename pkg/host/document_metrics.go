package host

import (
	"context"

	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
)

func (s *defraService) TrackDocumentMetrics(ctx context.Context) {
	if s.node == nil {
		return
	}
	go s.trackDocumentMetrics(ctx, s.node)
}

func (s *defraService) trackDocumentMetrics(ctx context.Context, defraNode *node.Node) {
	log := s.log.Sugar()

	collectionIDToName := make(map[string]string, len(trackedMetricCollections))
	for _, name := range trackedMetricCollections {
		col, err := defraNode.DB.GetCollectionByName(ctx, name)
		if err != nil {
			log.Warnw("document metrics: collection unavailable, skipping", "collection", name, "error", err)
			continue
		}
		collectionIDToName[col.CollectionID()] = name
	}
	if len(collectionIDToName) == 0 {
		log.Warn("document metrics disabled, no tracked collections available")
		return
	}

	sub, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		log.Warnw("document metrics disabled, event subscription failed", "error", err)
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
			if !ok || !update.IsRelay {
				continue
			}
			name, tracked := collectionIDToName[update.CollectionID]
			if !tracked {
				continue
			}
			s.metrics.IncrementDocumentsReceived()
			s.metrics.IncrementDocumentByType(name)
		}
	}
}
