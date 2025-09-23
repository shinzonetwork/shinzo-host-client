package shinzohub

import "fmt"

type ViewRegisteredEvent struct { // ViewRegisteredEvent implements ShinzoEvent interface
	Key     string
	Creator string
	View    View
}

type View struct {
	Query  string   `json:"query"`
	Sdl    string   `json:"sdl"`
	Lenses []string `json:"transform.lenses"`
}

func (event *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("(Key: %s, Creator: %s, View: %+v)", event.Key, event.Creator, event.View)
}
