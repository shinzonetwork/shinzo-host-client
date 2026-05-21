package constants

// Version represents a version with signature information.
type Version struct {
	CID                 string    `json:"cid"`
	Signature           Signature `json:"signature"`
	CollectionVersionID string    `json:"collectionVersionId"`
}
