package constants

// DefraDB Collection Names - matches schema.graphql types
const (
	CollectionBlock             = "Ethereum__Testnet__Block"
	CollectionTransaction       = "Ethereum__Testnet__Transaction"
	CollectionLog               = "Ethereum__Testnet__Log"
	CollectionAccessListEntry   = "Ethereum__Testnet__AccessListEntry"
	CollectionAttestationRecord = "Ethereum__Testnet__AttestationRecord"
	CollectionBlockSignature    = "Ethereum__Testnet__BlockSignature"
	CollectionSnapshotSignature = "Ethereum__Testnet__SnapshotSignature"
	CollectionChain             = "Ethereum__Testnet"
)

// Collection name slice for bulk operations (used for P2P replication subscriptions).
// AttestationRecord is excluded — each host builds its own attestation state independently.
var AllCollections = []string{
	CollectionBlock,
	CollectionTransaction,
	CollectionAccessListEntry,
	CollectionLog,
	CollectionBlockSignature,
}
