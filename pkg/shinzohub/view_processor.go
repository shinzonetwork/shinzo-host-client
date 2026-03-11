package shinzohub

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

// ViewProcessor handles view processing without circular imports
type ViewProcessor struct {
	defraNode     *node.Node
	schemaService *view.SchemaService
}

// NewViewProcessor creates a new ViewProcessor
func NewViewProcessor(defraNode *node.Node) *ViewProcessor {
	if defraNode == nil {
		fmt.Println("defraNode cannot be nil")
		return nil
	}
	return &ViewProcessor{
		defraNode: defraNode,
		schemaService: &view.SchemaService{},
	}
}

// ProcessViewFromWire processes a view from wire format (base64 encoded)
func (svp *ViewProcessor) ProcessViewFromWire(ctx context.Context, wireBase64 string) (*view.View, error) {
	// Decode base64 wire format
	wire, err := base64.StdEncoding.DecodeString(wireBase64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 wire: %w", err)
	}

	// Unbundle the view
	newView, err := view.NewViewFromWire(wire)
	if err != nil {
		return nil, fmt.Errorf("failed to create view from wire: %w", err)
	}

	// Validate the view
	if err := newView.Validate(); err != nil {
		return nil, fmt.Errorf("invalid view: %w", err)
	}

	// Log the SDL for debugging
	fmt.Printf("🔍 View SDL: %s\n", newView.Data.Sdl)

	return newView, nil
}

// SetupViewInDefraDB sets up the view in DefraDB
func (svp *ViewProcessor) SetupViewInDefraDB(ctx context.Context, newView *view.View) error {
	// If the view has lenses, set up migration to get the lens CID
	var lensCID string
	if newView.HasLenses() {
		lensConfig, err := newView.BuildLensConfig()
		if err != nil {
			return fmt.Errorf("failed to build lens config: %w", err)
		}
		lensCID, err = svp.defraNode.DB.AddLens(ctx, lensConfig.Lens)
		if err != nil {
			return fmt.Errorf("failed to add lens: %w", err)
		}
		_, err = svp.defraNode.DB.SetMigration(ctx, lensConfig)
		if err != nil {
			return fmt.Errorf("failed to set migration: %w", err)
		}
	}

	// Configure the view in DefraDB with the lens transform CID
	err := newView.ConfigureLens(ctx, svp.defraNode, svp.schemaService, lensCID)
	if err != nil {
		return fmt.Errorf("failed to configure view: %w", err)
	}

	// Subscribe to the view for real-time updates
	err = newView.SubscribeTo(ctx, svp.defraNode)
	if err != nil {
		return fmt.Errorf("failed to subscribe to view: %w", err)
	}

	return nil
}
