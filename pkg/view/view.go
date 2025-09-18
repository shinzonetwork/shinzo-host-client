package view

import "github.com/sourcenetwork/defradb/client"

const ViewResource string = "view"

type View struct {
	Name       string
	Lens       client.LensConfig
	Parents    []Collection
	Collection Collection
}
