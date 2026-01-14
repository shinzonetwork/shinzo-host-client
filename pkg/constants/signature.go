package constants

// Signature represents a cryptographic signature
type Signature struct {
	Type     string `json:"type"`
	Identity string `json:"identity"`
	Value    string `json:"value"`
}
