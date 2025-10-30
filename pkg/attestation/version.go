package attestation

type Version struct {
	CID       string    `json:"cid"`
	Signature Signature `json:"signature"`
}
