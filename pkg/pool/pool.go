// Package pool derives a pool's deterministic CREATE2 address (its pool_id) from
// the view it serves. The derivation matches the ShinzoHub poolregistry
// precompile byte for byte, so the host stamps the same id the chain deploys
// Pool.sol at.
package pool

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// DefaultWindowSize is the pinned config window for v1 pools. It feeds the
// CREATE2 salt only; the deployed contract does not store it.
const DefaultWindowSize = 10000

// deployer is the poolregistry precompile, the CREATE2 deployer of Pool.sol.
const deployer = "0x0000000000000000000000000000000000000213"

// PoolID returns the CREATE2 address ShinzoHub deploys the pool for
// (viewAddr, windowSize) at.
func PoolID(viewAddr common.Address, windowSize uint64) common.Address {
	salt := poolSalt(viewAddr, windowSize)
	initCode := buildInitCode(viewAddr)
	return crypto.CreateAddress2(common.HexToAddress(deployer), salt, crypto.Keccak256(initCode))
}

// poolSalt is keccak256(viewAddress || bigEndian(windowSize)).
func poolSalt(viewAddr common.Address, windowSize uint64) [32]byte {
	var buf [20 + 8]byte
	copy(buf[:20], viewAddr.Bytes())
	binary.BigEndian.PutUint64(buf[20:], windowSize)
	return crypto.Keccak256Hash(buf[:])
}

// buildInitCode is PoolBytecode followed by the abi-encoded view address. The
// window size differentiates pools through the salt, not the constructor.
// Packing a single address never fails.
func buildInitCode(viewAddr common.Address) []byte {
	args, _ := PoolConstructorArgs.Pack(viewAddr)
	return append(append([]byte{}, PoolBytecode...), args...)
}

// mustABIType parses an ABI type or panics; the argument is a compile-time
// constant, so a parse failure is a programming error.
func mustABIType(t string) abi.Type {
	parsed, err := abi.NewType(t, "", nil)
	if err != nil {
		panic(err)
	}
	return parsed
}
