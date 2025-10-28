package attestation

type AccessListEntry struct {
	Address     string      `json:"address"`
	StorageKeys []string    `json:"storageKeys"`
	Transaction Transaction `json:"transaction"`

	Version []Version `json:"_version"`
	DocId   string    `json:"_docID"`
}
