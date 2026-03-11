package shinzohub

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

// ViewProcessor orchestrates DefraDB setup for views (lens, migration, configuration)
// For viewbundle operations (decode, validate), use the view package directly
type ViewProcessor struct {
	defraNode     *node.Node
	schemaService *view.SchemaService
}

// NewViewProcessor creates a new ViewProcessor for setting up views in DefraDB
func NewViewProcessor(defraNode *node.Node) *ViewProcessor {
	return &ViewProcessor{
		defraNode:     defraNode,
		schemaService: &view.SchemaService{},
	}
}

// ProcessViewFromWireFormat decodes, unbundles, and validates a view from base64 wire format
func ProcessViewFromWireFormat(wireBase64 string) (*view.View, error) {
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

	return newView, nil
}

// SetupViewInDefraDB sets up the view in DefraDB
func (svp *ViewProcessor) SetupViewInDefraDB(ctx context.Context, newView *view.View) error {
	// Set up lens and migration if needed
	lensCID, err := view.SetupLensInDefraDB(ctx, svp.defraNode, newView)
	if err != nil {
		return err
	}

	// Configure the view in DefraDB with the lens transform CID
	err = newView.ConfigureLens(ctx, svp.defraNode, svp.schemaService, lensCID)
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
