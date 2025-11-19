package host

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/host/pkg/graphql"
	"github.com/shinzonetwork/host/pkg/view"
	"github.com/sourcenetwork/defradb/client"
)

func (h *Host) PrepareView(ctx context.Context, v view.View) error {
	err := v.SubscribeTo(ctx, h.DefraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Error subscribing to view %+v: %w", v, err)
		} else {
			return fmt.Errorf("Error subscribing to view %+v: %w", v, err)
		}
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

	// Store the view to defra so it can be recovered on restart
	err = h.storeView(ctx, v)
	if err != nil {
		return fmt.Errorf("Error storing view to defra: %w", err)
	}

	return nil
}

// storeView saves a view to the defra View collection so it can be recovered on restart
func (h *Host) storeView(ctx context.Context, v view.View) error {
	return h.storeViewWithBlock(ctx, v, 0)
}

// storeViewWithBlock saves a view to the defra View collection with a specific lastProcessedBlock
func (h *Host) storeViewWithBlock(ctx context.Context, v view.View, lastProcessedBlock uint64) error {
	collection, err := h.DefraNode.DB.GetCollectionByName(ctx, "View")
	if err != nil {
		return fmt.Errorf("error getting View collection: %w", err)
	}

	// Prepare the view document with the fields from the schema
	viewDoc := make(map[string]any)
	viewDoc["name"] = v.Name
	if v.Query != nil {
		viewDoc["query"] = *v.Query
	} else {
		viewDoc["query"] = ""
	}
	if v.Sdl != nil {
		viewDoc["sdl"] = *v.Sdl
	} else {
		viewDoc["sdl"] = ""
	}
	viewDoc["lastProcessedBlock"] = int64(lastProcessedBlock)

	// Create a document from the map
	document, err := client.NewDocFromMap(viewDoc, collection.Version())
	if err != nil {
		return fmt.Errorf("failed to create document from map: %w", err)
	}

	// Save will update if document exists (by unique name), create if it doesn't
	err = collection.Save(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to save view document: %w", err)
	}

	return nil
}

// updateViewLastProcessedBlock updates the lastProcessedBlock field for a view in the View collection
func (h *Host) updateViewLastProcessedBlock(ctx context.Context, viewName string, blockNumber uint64) error {
	collection, err := h.DefraNode.DB.GetCollectionByName(ctx, "View")
	if err != nil {
		return fmt.Errorf("error getting View collection: %w", err)
	}

	// Query for the existing view document by name
	query := fmt.Sprintf(`query {
		View(filter: {name: {_eq: "%s"}}) {
			_docID
			name
			query
			sdl
			lastProcessedBlock
		}
	}`, viewName)

	type ViewDoc struct {
		DocID              string `json:"_docID"`
		Name               string `json:"name"`
		Query              string `json:"query"`
		Sdl                string `json:"sdl"`
		LastProcessedBlock int64  `json:"lastProcessedBlock"`
	}

	views, err := defra.QueryArray[ViewDoc](ctx, h.DefraNode, query)
	if err != nil {
		return fmt.Errorf("error querying view: %w", err)
	}

	if len(views) == 0 {
		return fmt.Errorf("view %s not found in View collection", viewName)
	}

	viewDoc := views[0]

	// Update the document with the new lastProcessedBlock
	updateDoc := map[string]any{
		"_docID":            viewDoc.DocID,
		"name":              viewDoc.Name,
		"query":             viewDoc.Query,
		"sdl":               viewDoc.Sdl,
		"lastProcessedBlock": int64(blockNumber),
	}

	document, err := client.NewDocFromMap(updateDoc, collection.Version())
	if err != nil {
		return fmt.Errorf("failed to create document from map: %w", err)
	}

	err = collection.Save(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to update view document: %w", err)
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
	if len(sourceDocuments) == 0 {
		return fmt.Errorf("No source data found using query %s", query)
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
