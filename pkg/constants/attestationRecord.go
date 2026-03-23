package constants

// AttestationRecord represents an attestation record in DefraDB
type AttestationRecord struct {
	AttestedDocId string   `json:"attested_doc"`
	SourceDocIds  []string `json:"source_doc"`
	CIDs          []string `json:"CIDs"`
	DocType       string   `json:"doc_type"`
	VoteCount     int      `json:"vote_count"`
}
