package constants

// BlockAttestation is one host's record that a set of indexers attested to a block,
// identified by (hostId, blockNumber, merkleRoot). The host is the sole writer of its
// own records, so the list fields are safe under DefraDB's last-writer-wins merge. It
// is host-owned: written locally from BlockSignatures and excluded from host-to-host
// P2P sync (absent from AllCollections, the P2P subscription set). Separate from
// AttestationRecord, which serves the per-document and snapshot paths.
type BlockAttestation struct {
	// HostID is the producing host's cached signing identity, and the first component
	// of the (hostId, blockNumber, merkleRoot) record identity.
	HostID string `json:"hostId"`
	// BlockNumber is the height of the attested block.
	BlockNumber int64 `json:"blockNumber"`
	// BlockHash is the hash of the attested block.
	BlockHash string `json:"blockHash"`
	// MerkleRoot is the indexer-signed merkle root over the block's CIDs. A different
	// root for the same block is a divergent attestation and lands on its own record.
	MerkleRoot string `json:"merkleRoot"`
	// SignerIdentities are the signing identities of the indexers whose BlockSignatures
	// the host merged for this (block, merkleRoot), deduped on each write.
	SignerIdentities []string `json:"signerIdentities"`
	// CIDs are the block's document CIDs carried by the merged BlockSignatures.
	CIDs []string `json:"cids"`
	// VoteCount is the distinct-signer count, set to len(SignerIdentities) on every
	// write (not incremented), so it counts indexers rather than writes.
	VoteCount int `json:"voteCount"`
	// HostSignature is the host's secp256k1 signature over the canonical payload
	// returned by attestation.BlockAttestationSigningPayload. It authenticates the
	// host's claim about which indexers it saw and is recomputed whenever
	// SignerIdentities changes.
	HostSignature string `json:"hostSignature"`
	// ShinzoHeight is the ShinzoHub block height observed when the record was written,
	// used by the accounting service to bin attestations into epochs. Zero means the
	// height was unavailable at write time (no hub configured, or not yet polled).
	ShinzoHeight int64 `json:"shinzoHeight"`
	// CreatedAt is the unix time of the record's first write, preserved across later merges.
	CreatedAt int64 `json:"createdAt"`
}
