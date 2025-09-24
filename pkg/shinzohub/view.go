package shinzohub

import (
	"context"
	"fmt"
	"regexp"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

type ViewRegisteredEvent struct { // ViewRegisteredEvent implements ShinzoEvent interface
	Key     string
	Creator string
	View    View
}

type View struct {
	Query  string   `json:"query"`
	Sdl    string   `json:"sdl"`
	Lenses []string `json:"transform.lenses"`
	Name   string
}

func (event *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("(Key: %s, Creator: %s, View: %+v)", event.Key, event.Creator, event.View)
}

// ExtractNameFromSDL extracts the type name from the SDL string
func (view *View) ExtractNameFromSDL() {
	// Look for pattern: type <Name> @...
	re := regexp.MustCompile(`type\s+(\w+)\s+@`)
	matches := re.FindStringSubmatch(view.Sdl)
	if len(matches) > 1 {
		view.Name = matches[1]
	} else {
		// Fallback: look for type <Name> { pattern
		re = regexp.MustCompile(`type\s+(\w+)\s+{`)
		matches = re.FindStringSubmatch(view.Sdl)
		if len(matches) > 1 {
			view.Name = matches[1]
		}
	}
}

func (view *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(view.Sdl)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error applying view's schema: %v", err)
	}

	err = defraNode.DB.AddP2PCollections(ctx, view.Name)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", view.Name, err)
	}

	return nil
}
