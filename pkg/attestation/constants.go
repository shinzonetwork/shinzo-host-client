package attestation

// Signature-type names the block signature verifier accepts. Each key type has two accepted names.
const (
	sigTypeES256K       = "ES256K"
	sigTypeES256KLower  = "ecdsa-256k"
	sigTypeEd25519      = "Ed25519"
	sigTypeEd25519Lower = "ed25519"
)
