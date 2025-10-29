package attestation

import "github.com/shinzonetwork/app-sdk/pkg/attestation"

type AccessListEntry struct {
	Address     string      `json:"address"`
	StorageKeys []string    `json:"storageKeys"`
	Transaction Transaction `json:"transaction"`

	Version []attestation.Version `json:"_version"`
	DocId   string                `json:"_docID"`
}
