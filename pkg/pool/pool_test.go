package pool

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
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

	got, err := PoolID(view, DefaultWindowSize)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("PoolID assembled the CREATE2 inputs differently\n got:  %s\nwant: %s", got, want)
	}

	// The hub's poolregistry precompile derives this address for the same inputs.
	const fromHub = "0x773D75dFee746ab27FA61b3944956e6eD3ee28dB"
	if got.Hex() != fromHub {
		t.Fatalf("pool_id moved; bytecode or derivation drifted from the hub\n got:  %s\nwant: %s", got.Hex(), fromHub)
	}
}

// TestPoolID_PropagatesPackError checks a constructor-args mismatch surfaces as an
// error instead of silently producing a wrong pool_id. It swaps the package var, so
// it must not run in parallel with the golden test above.
func TestPoolID_PropagatesPackError(t *testing.T) {
	saved := PoolConstructorArgs
	defer func() { PoolConstructorArgs = saved }()
	// Two arguments cannot pack a single address, so Pack returns an error.
	PoolConstructorArgs = abi.Arguments{{Type: mustABIType("address")}, {Type: mustABIType("uint256")}}

	view := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	if _, err := PoolID(view, DefaultWindowSize); err == nil {
		t.Fatal("expected a pack error to propagate, got nil")
	}
}
