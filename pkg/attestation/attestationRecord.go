package attestation

import "fmt"

type AttestationRecord struct {
	DocId      string      `json:"_docID"`
	VoteCount  int         `json:"vote_count"`
	Signatures []Signature `json:"signatures"`
}

func CreateAttestationRecord(docId string, versions []Version) (*AttestationRecord, error) {
	versionCount := len(versions)
	if versionCount < 1 {
		return nil, fmt.Errorf("Must have at least one version")
	}

	attestationRecord := &AttestationRecord{
		DocId:      docId,
		VoteCount:  versionCount,
		Signatures: []Signature{},
	}
	for _, version := range versions {
		attestationRecord.Signatures = append(attestationRecord.Signatures, version.Signature)
	}

	return attestationRecord, nil
}
