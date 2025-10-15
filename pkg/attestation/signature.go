package attestation

type Signature struct {
	Type     string `json:"type"`
	Identity string `json:"identity"`
	Value    string `json:"value"`
	Typename string `json:"__typename"`
}
