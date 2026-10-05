// Package chain describes the chains a host serves: their configuration and the DefraDB collections
// their data is stored in.
package chain

// EthereumMainnet is the collection prefix of Ethereum mainnet.
const EthereumMainnet = "Ethereum__Mainnet"

// Height fields of the EVM collections.
const (
	blockHeightField     = "number"
	dependentHeightField = "blockNumber"
	// A snapshot covers a range of blocks, so its height is the newest block in it.
	snapshotHeightField = "endBlock"
)

// Collection is a DefraDB collection and the field holding the block height each of its
// documents belongs to.
type Collection struct {
	Name        string
	HeightField string
}

// Collections are the DefraDB collections of one chain.
type Collections struct {
	// Prefix is the chain's prefix, which every collection name starts with.
	Prefix            string
	Block             Collection
	Transaction       Collection
	Log               Collection
	AccessListEntry   Collection
	BlockSignature    Collection
	SnapshotSignature Collection
	AttestationRecord Collection
}

// EVM returns the collections of an EVM chain whose generators write under prefix. Each is
// named "<prefix>__<table>".
func EVM(prefix string) Collections {
	name := func(table string) string { return prefix + "__" + table }
	return Collections{
		Prefix:            prefix,
		Block:             Collection{Name: name("Block"), HeightField: blockHeightField},
		Transaction:       Collection{Name: name("Transaction"), HeightField: dependentHeightField},
		Log:               Collection{Name: name("Log"), HeightField: dependentHeightField},
		AccessListEntry:   Collection{Name: name("AccessListEntry"), HeightField: dependentHeightField},
		BlockSignature:    Collection{Name: name("BlockSignature"), HeightField: dependentHeightField},
		SnapshotSignature: Collection{Name: name("SnapshotSignature"), HeightField: snapshotHeightField},
		AttestationRecord: Collection{Name: name("AttestationRecord"), HeightField: dependentHeightField},
	}
}

// Subscribed returns the names of the collections the host subscribes to over P2P.
// AttestationRecord is not among them: each host keeps its own attestations. Neither is
// SnapshotSignature: the host verifies snapshots against the signatures in the snapshot list.
func (c Collections) Subscribed() []string {
	return []string{c.Block.Name, c.Transaction.Name, c.AccessListEntry.Name, c.Log.Name, c.BlockSignature.Name}
}
