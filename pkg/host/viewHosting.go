package host

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/graphql"
	"github.com/shinzonetwork/host/pkg/view"
)

func (h *Host) PrepareView(ctx context.Context, v view.View) error {
	err := v.SubscribeTo(ctx, h.DefraNode)
	if err != nil {
		return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
	}

	if v.HasLenses() {
		err = v.PostWasmToFile(ctx, h.LensRegistryPath)
		if err != nil {
			return fmt.Errorf("Error downloading lenses to local env: %w", err)
		}

		err = v.ConfigureLens(ctx, h.DefraNode)
		if err != nil {
			return fmt.Errorf("Error configuring lenses: %w", err)
		}
	}

	return nil
}

func (h *Host) ApplyView(ctx context.Context, v view.View, startingBlockNumber uint64, endingBlockNumber uint64) error {
	query, err := graphql.AddBlockNumberFilter(*v.Query, startingBlockNumber, endingBlockNumber)
	if err != nil {
		return fmt.Errorf("Error assembling query: %w", err)
	}

	sourceDocuments, err := defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
	if err != nil {
		return fmt.Errorf("Error fetching source data with query %s: %w", query, err)
	}

	var transformedDocuments []map[string]any
	if v.HasLenses() {
		transformedDocuments, err = v.ApplyLensTransform(ctx, h.DefraNode, sourceDocuments)
		if err != nil {
			return fmt.Errorf("Error applying lens transforms from view %s: %w", v.Name, err)
		}
	} else {
		transformedDocuments = sourceDocuments
	}

	err = v.WriteTransformedToCollection(ctx, h.DefraNode, transformedDocuments)
	if err != nil {
		return fmt.Errorf("Error writing transformed data to collection %s: %w", v.Name, err)
	}

	return nil
}
