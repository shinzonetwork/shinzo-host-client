package shinzohub

import "fmt"

type ViewRegisteredEvent struct { // ViewRegisteredEvent implements ShinzoEvent interface
	Key     string
	Creator string
	View    string
}

func (event *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("(Key: %s, Creator: %s, View: %s)", event.Key, event.Creator, event.View)
}
