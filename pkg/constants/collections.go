package constants

// DefraDB Collection Names - matches schema.graphql types.
const (
	CollectionBlock             = "Ethereum__Mainnet__Block"
	CollectionTransaction       = "Ethereum__Mainnet__Transaction"
	CollectionLog               = "Ethereum__Mainnet__Log"
	CollectionAccessListEntry   = "Ethereum__Mainnet__AccessListEntry"
	CollectionAttestationRecord = "Ethereum__Mainnet__AttestationRecord"
	CollectionBlockAttestation  = "Ethereum__Mainnet__BlockAttestation"
	CollectionBlockSignature    = "Ethereum__Mainnet__BlockSignature"
	CollectionSnapshotSignature = "Ethereum__Mainnet__SnapshotSignature"
	CollectionChain             = "Ethereum__Mainnet"
)

// Collection name slice for bulk operations (used for P2P replication subscriptions).
// AttestationRecord and BlockAttestation are excluded from host-to-host sync: each
// host builds its own attestation state independently.

// AllCollections is a list of DefraDB collection names.
var AllCollections = []string{ //nolint:gochecknoglobals
	CollectionBlock,
	CollectionTransaction,
	CollectionAccessListEntry,
	CollectionLog,
	CollectionBlockSignature,
}
