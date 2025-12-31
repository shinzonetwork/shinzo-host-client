package host

import (
	"context"
	"fmt"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegistrationHandler manages view registration from Shinzo Hub events
type ViewRegistrationHandler struct {
	viewManager ViewManagerInterface // Will be injected from host
	startHeight uint64               // Starting block height for new views
}

// NewViewRegistrationHandler creates a new view registration handler
func NewViewRegistrationHandler(startHeight uint64) *ViewRegistrationHandler {
	return &ViewRegistrationHandler{
		startHeight: startHeight,
	}
}

// SetViewManager injects the ViewManager dependency
func (vrh *ViewRegistrationHandler) SetViewManager(vm ViewManagerInterface) {
	vrh.viewManager = vm
}

// ProcessRegisteredEvent handles a view registration event from Shinzo Hub
func (vrh *ViewRegistrationHandler) ProcessRegisteredEvent(ctx context.Context, event shinzohub.ViewRegisteredEvent) error {
	if vrh.viewManager == nil {
		return fmt.Errorf("ViewManager not set - cannot process view registration")
	}

	fmt.Printf("🎯 Processing view registration: %s (creator: %s)\n", event.View.Name, event.Creator)

	// Save WASM blob data to file system if lenses exist
	if len(event.View.Transform.Lenses) > 0 {
		fmt.Printf("📦 Saving WASM blob data for view %s with %d lenses\n", event.View.Name, len(event.View.Transform.Lenses))
		if err := event.View.PostWasmToFile(context.Background(), "./.lens"); err != nil {
			fmt.Printf("Failed to save WASM blob for view %s: %v\n", event.View.Name, err)
		}
	}

	// Register view with ViewManager (which handles ViewMatcher and ViewRangeFinder)
	err := vrh.viewManager.RegisterView(ctx, event.View)
	if err != nil {
		return fmt.Errorf("failed to register view %s: %w", event.View.Name, err)
	}

	fmt.Printf("✅ Successfully registered view %s for block range monitoring\n", event.View.Name)
	return nil
}

// ViewManagerInterface to avoid circular imports
type ViewManagerInterface interface {
	RegisterView(ctx context.Context, viewDef view.View) error
}
