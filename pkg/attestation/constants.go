package attestation

// Signature-type names accepted by the block signature verifier and the
// attestation record service. The lowercase variants are accepted alongside
// the canonical CamelCase forms because older writers emit them.
const (
	sigTypeES256K       = "ES256K"
	sigTypeES256KLower  = "ecdsa-256k"
	sigTypeEd25519      = "Ed25519"
	sigTypeEd25519Lower = "ed25519"
)
