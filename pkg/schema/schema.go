package schema

import (
	_ "embed"
)

// SchemaGraphQL holds the contents of the GraphQL schema defined in `schema.graphql`.
//
//go:embed schema.graphql
var SchemaGraphQL string

// GetSchema returns the GraphQL schema found in `schema.graphql` as a string.
func GetSchema() string {
	return SchemaGraphQL
}

// BlockAttestationSchema is the SDL for the host-owned BlockAttestation collection.
// It is registered separately from the shared schema (see host.applySchema), not in
// schema.graphql. One record per (hostId, blockNumber, merkleRoot); the host is the
// sole writer, so the list fields are safe under DefraDB's last-writer-wins.
const BlockAttestationSchema = `type Ethereum__Mainnet__BlockAttestation {
    hostId: String @index
    blockNumber: Int @index
    blockHash: String @index
    merkleRoot: String @index
    signerIdentities: [String]
    cids: [String]
    voteCount: Int
    hostSignature: String
    shinzoHeight: Int @index
    createdAt: Int @index
}`
