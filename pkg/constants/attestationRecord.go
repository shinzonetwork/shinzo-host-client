package constants

// AttestationRecord represents an attestation record in DefraDB.
//
// BlockNumber is the block the record attests to, or nil when the attested document is not
// tied to a single block. Nil is left out of the stored document rather than written as zero,
// which is itself a valid block number.
type AttestationRecord struct {
	AttestedDocID string   `json:"attested_doc"`
	SourceDocIDs  []string `json:"source_doc"`
	CIDs          []string `json:"CIDs"`
	DocType       string   `json:"doc_type"`
	VoteCount     int      `json:"vote_count"`
	BlockNumber   *int64   `json:"blockNumber"`
}
