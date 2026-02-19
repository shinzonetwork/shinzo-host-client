package constants

// DefraDB Collection Names - matches schema.graphql types
const (
	CollectionBlock             = "Ethereum__Mainnet__Block"
	CollectionTransaction       = "Ethereum__Mainnet__Transaction"
	CollectionLog               = "Ethereum__Mainnet__Log"
	CollectionAccessListEntry   = "Ethereum__Mainnet__AccessListEntry"
	CollectionAttestationRecord = "Ethereum__Mainnet__AttestationRecord"
	CollectionBatchSignature    = "Ethereum__Mainnet__BatchSignature"
	CollectionChain             = "Ethereum__Mainnet"
)

// Collection name slice for bulk operations (used for P2P replication subscriptions).
// AttestationRecord is excluded — each host builds its own attestation state independently.
var AllCollections = []string{
	CollectionBlock,
	CollectionTransaction,
	CollectionAccessListEntry,
	CollectionLog,
	CollectionBatchSignature,
}
