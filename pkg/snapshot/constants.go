package snapshot

const (
	httpClientTimeoutMins  = 5
	sha256CombinedByteSize = 64
	merkleArity            = 2
)

// Wire-format identifiers used by snapshot import.
const (
	// snapshotMagic is the 4-byte file-header magic for kvSnapshot.
	snapshotMagic = "DFKV"

	// Signature-type names accepted by importer.verifySignature. The
	// lowercase variants are accepted alongside the canonical CamelCase
	// forms because older snapshot writers emit them.
	sigTypeES256K       = "ES256K"
	sigTypeES256KLower  = "ecdsa-256k"
	sigTypeEd25519      = "Ed25519"
	sigTypeEd25519Lower = "ed25519"
)
