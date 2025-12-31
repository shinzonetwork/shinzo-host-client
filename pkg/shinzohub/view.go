package shinzohub

import (
	"fmt"
	"regexp"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

type ViewRegisteredEvent struct { // ViewRegisteredEvent implements ShinzoEvent interface
	Key     string
	Creator string
	View    view.View
}

type EntityRegisteredEvent struct { // Subscribe to Registered and EntityRegistered events
	Key    string
	Owner  string
	DID    string
	Pid    string
	Entity string
}

func (vre *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("ViewRegistered: key=%s, creator=%s, view=%s", vre.Key, vre.Creator, vre.View.Name)
}

func (ere *EntityRegisteredEvent) ToString() string {
	return fmt.Sprintf("EntityRegistered: key=%s, owner=%s, did=%s, pid=%s", ere.Key, ere.Owner, ere.DID, ere.Pid)
}

// ExtractNameFromSDL extracts the type name from the SDL string
func ExtractNameFromSDL(view *view.View) {
	if view.Sdl == nil {
		view.Name = ""
		return
	}
	// Look for pattern: type <Name> @...
	re := regexp.MustCompile(`type\s+(\w+)\s+@`)
	matches := re.FindStringSubmatch(*view.Sdl)
	if len(matches) > 1 {
		view.Name = matches[1]
	} else {
		// Fallback: look for type <Name> { pattern
		re = regexp.MustCompile(`type\s+(\w+)\s+{`)
		matches = re.FindStringSubmatch(*view.Sdl)
		if len(matches) > 1 {
			view.Name = matches[1]
		}
	}
}
