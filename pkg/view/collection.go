package view

import "fmt"

type Collection struct {
	Type           CollectionType
	CollectionName string
}

type CollectionType int

const (
	Primitive CollectionType = iota
	DataView
)

var resourceType = map[CollectionType]string{
	Primitive: "primitive",
	DataView:  "view",
}

func (collectionType CollectionType) AsString() string {
	return resourceType[collectionType]
}

func (collection Collection) SourceObjectIdentifier() string {
	return fmt.Sprintf("%s:%s", collection.Type.AsString(), collection.CollectionName)
}
