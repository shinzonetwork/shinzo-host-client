package pool

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestPoolID_MatchesHubAndReconstruction checks PoolID assembles the CREATE2
// inputs the same way as an independent reconstruction, and that the result
// matches the address the hub's poolregistry precompile derives for the same
// inputs. If Pool.sol changes and PoolBytecode is not re-copied, this fails, so
// the mismatch is caught before any pool_id is stamped wrong.
func TestPoolID_MatchesHubAndReconstruction(t *testing.T) {
	view := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	// Reconstruct the CREATE2 inputs independently of poolSalt/buildInitCode.
	var saltInput [28]byte
	copy(saltInput[:20], view.Bytes())
	binary.BigEndian.PutUint64(saltInput[20:], DefaultWindowSize)
	padded := make([]byte, 32)
	copy(padded[12:], view.Bytes())
	initCode := append(append([]byte{}, PoolBytecode...), padded...)
	want := crypto.CreateAddress2(
		common.HexToAddress(deployer),
		crypto.Keccak256Hash(saltInput[:]),
		crypto.Keccak256(initCode),
	)

	got := PoolID(view, DefaultWindowSize)
	if got != want {
		t.Fatalf("PoolID assembled the CREATE2 inputs differently\n got:  %s\nwant: %s", got, want)
	}

	// The hub's poolregistry precompile derives this address for the same inputs.
	const fromHub = "0x4E7A157E7fA744EC550240a1b57EC41d72b0ccE5"
	if got.Hex() != fromHub {
		t.Fatalf("pool_id moved; bytecode or derivation drifted from the hub\n got:  %s\nwant: %s", got.Hex(), fromHub)
	}
}
