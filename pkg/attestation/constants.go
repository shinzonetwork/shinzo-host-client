package attestation

// Signature-type names the block signature verifier accepts. Generators write ES256K or EdDSA,
// the JOSE algorithm names.
const (
	sigTypeES256K       = "ES256K"
	sigTypeES256KLower  = "ecdsa-256k"
	sigTypeEdDSA        = "EdDSA"
	sigTypeEd25519      = "Ed25519"
	sigTypeEd25519Lower = "ed25519"
)
