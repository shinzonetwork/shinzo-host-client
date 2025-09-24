package shinzohub

import (
	"fmt"
	"regexp"

	"github.com/shinzonetwork/app-sdk/pkg/views"
)

type ViewRegisteredEvent struct { // ViewRegisteredEvent implements ShinzoEvent interface
	Key     string
	Creator string
	View    views.View
}

func (event *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("(Key: %s, Creator: %s, View: %+v)", event.Key, event.Creator, event.View)
}

// ExtractNameFromSDL extracts the type name from the SDL string
func ExtractNameFromSDL(view *views.View) {
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
