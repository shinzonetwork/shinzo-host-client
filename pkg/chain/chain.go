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
	// Header, Evidence, StateCapture and BlockAttestation are a tokens-mode chain's tables. A
	// full-mode chain has none of them, and a tokens-mode chain has none of the tables above.
	Header           Collection
	Evidence         Collection
	StateCapture     Collection
	BlockAttestation Collection
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

// Tokens returns the collections of a tokens-mode chain whose generators write under prefix:
// Header, Evidence, StateCapture and BlockAttestation, each named "<prefix>__<table>".
func Tokens(prefix string) Collections {
	name := func(table string) string { return prefix + "__" + table }
	return Collections{
		Prefix:           prefix,
		Header:           Collection{Name: name("Header"), HeightField: blockHeightField},
		Evidence:         Collection{Name: name("Evidence"), HeightField: dependentHeightField},
		StateCapture:     Collection{Name: name("StateCapture"), HeightField: dependentHeightField},
		BlockAttestation: Collection{Name: name("BlockAttestation"), HeightField: dependentHeightField},
	}
}

// Collections returns the collections of the chain c configures.
func (c Config) Collections() Collections {
	if c.Mode == ModeTokens {
		return Tokens(c.Prefix)
	}
	return EVM(c.Prefix)
}

// TokensMode reports whether these are a tokens-mode chain's collections.
func (c Collections) TokensMode() bool {
	return c.Header.Name != ""
}

// Generated returns the collections a chain's generators write: in full mode all but
// AttestationRecord, which the host writes, and in tokens mode all four.
func (c Collections) Generated() []Collection {
	if c.TokensMode() {
		return []Collection{c.Header, c.Evidence, c.StateCapture, c.BlockAttestation}
	}
	return []Collection{c.Block, c.Transaction, c.Log, c.AccessListEntry, c.BlockSignature, c.SnapshotSignature}
}

// Subscribed returns the names of the collections the host subscribes to over P2P. In full mode,
// AttestationRecord is not among them: each host keeps its own attestations. Neither is
// SnapshotSignature: the host verifies snapshots against the signatures in the snapshot list. In
// tokens mode they are all four tables.
func (c Collections) Subscribed() []string {
	if c.TokensMode() {
		return []string{c.Header.Name, c.Evidence.Name, c.StateCapture.Name, c.BlockAttestation.Name}
	}
	return []string{c.Block.Name, c.Transaction.Name, c.AccessListEntry.Name, c.Log.Name, c.BlockSignature.Name}
}
