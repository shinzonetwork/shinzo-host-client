package acp

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/ethereum/go-ethereum/common"
)

// QueryBalanceReader reads an address's spendable query balance from the hub.
type QueryBalanceReader interface {
	GetQueryBalance(ctx context.Context, bech32Address string) (*big.Int, error)
}

// BalanceAuthorizer gates a query on the payer's x/querybalance. The payer is the
// Ethereum address recovered from the request signature, encoded to the hub's
// bech32 form for the lookup, and is allowed when its balance meets the minimum.
//
// Balances only change when an epoch settles, so a funded balance is cached for
// the epoch and dropped when the epoch advances. A below-minimum balance is not
// served from the cache, so a mid-epoch top-up is seen on the next query.
type BalanceAuthorizer struct {
	hub        QueryBalanceReader
	epochs     EpochSource
	minBalance *big.Int
	hrp        string

	mu         sync.Mutex
	cache      map[common.Address]*big.Int
	cacheEpoch uint64
}

// NewBalanceAuthorizer returns an authorizer that allows a payer whose query
// balance is at least minBalance. epochs invalidates the per-payer balance cache
// once per settlement epoch. hrp is the bech32 prefix the hub keys on, e.g.
// "shinzo".
func NewBalanceAuthorizer(hub QueryBalanceReader, epochs EpochSource, minBalance *big.Int, hrp string) *BalanceAuthorizer {
	return &BalanceAuthorizer{
		hub:        hub,
		epochs:     epochs,
		minBalance: minBalance,
		hrp:        hrp,
		cache:      make(map[common.Address]*big.Int),
	}
}

// Authorize returns true when the payer's query balance meets the minimum. A
// read error is returned as-is, not reported as a deny.
func (a *BalanceAuthorizer) Authorize(ctx context.Context, payer common.Address) (bool, error) {
	epoch, err := a.epochs.Epoch(ctx)
	if err != nil {
		return false, fmt.Errorf("read epoch: %w", err)
	}

	if a.cachedFunded(epoch, payer) {
		return true, nil
	}

	addr, err := bech32.ConvertAndEncode(a.hrp, payer.Bytes())
	if err != nil {
		return false, fmt.Errorf("encode payer address: %w", err)
	}
	balance, err := a.hub.GetQueryBalance(ctx, addr)
	if err != nil {
		return false, fmt.Errorf("read query balance for %s: %w", addr, err)
	}

	a.mu.Lock()
	a.cache[payer] = balance
	a.mu.Unlock()

	return balance.Cmp(a.minBalance) >= 0, nil
}

// cachedFunded reports whether payer has a cached balance meeting the minimum for
// the current epoch, dropping the cache when the epoch advances. A below-minimum
// balance returns false so the caller re-reads it.
func (a *BalanceAuthorizer) cachedFunded(epoch uint64, payer common.Address) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if epoch != a.cacheEpoch {
		a.cache = make(map[common.Address]*big.Int)
		a.cacheEpoch = epoch
		return false
	}
	balance, found := a.cache[payer]
	return found && balance.Cmp(a.minBalance) >= 0
}
