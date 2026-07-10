package acp

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/ethereum/go-ethereum/common"
)

type stubBalanceReader struct {
	balance *big.Int
	err     error
	gotAddr string
	calls   int
}

func (s *stubBalanceReader) GetQueryBalance(_ context.Context, addr string) (*big.Int, error) {
	s.calls++
	s.gotAddr = addr
	return s.balance, s.err
}

type stubEpochSource struct {
	epoch uint64
	err   error
}

func (s *stubEpochSource) Epoch(context.Context) (uint64, error) {
	return s.epoch, s.err
}

// gateBalanceReader signals when a read is in flight and blocks until released, so
// a test can advance the epoch while a balance read is outstanding.
type gateBalanceReader struct {
	balance *big.Int
	entered chan struct{}
	release chan struct{}
}

func (g *gateBalanceReader) GetQueryBalance(context.Context, string) (*big.Int, error) {
	g.entered <- struct{}{}
	<-g.release
	return g.balance, nil
}

func authorizer(hub QueryBalanceReader, epochs EpochSource, minBalance int64) *BalanceAuthorizer {
	return NewBalanceAuthorizer(hub, epochs, big.NewInt(minBalance), "shinzo")
}

// TestBalanceAuthorizerAllowsFundedAndEncodesAddress checks a funded payer is
// allowed and that the address handed to the hub is shinzo-bech32 that decodes
// back to the payer's bytes, which is what x/querybalance keys on.
func TestBalanceAuthorizerAllowsFundedAndEncodesAddress(t *testing.T) {
	payer := common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	hub := &stubBalanceReader{balance: big.NewInt(1000)}
	a := authorizer(hub, &stubEpochSource{epoch: 1}, 500)

	ok, err := a.Authorize(context.Background(), payer)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("funded payer was denied")
	}

	hrp, data, err := bech32.DecodeAndConvert(hub.gotAddr)
	if err != nil {
		t.Fatalf("queried address %q is not valid bech32: %v", hub.gotAddr, err)
	}
	if hrp != "shinzo" {
		t.Errorf("hrp = %q, want shinzo", hrp)
	}
	if !bytes.Equal(data, payer.Bytes()) {
		t.Errorf("decoded address does not match the payer bytes")
	}
}

func TestBalanceAuthorizerDeniesUnderfunded(t *testing.T) {
	a := authorizer(&stubBalanceReader{balance: big.NewInt(100)}, &stubEpochSource{epoch: 1}, 500)
	ok, err := a.Authorize(context.Background(), common.Address{0x01})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("underfunded payer was allowed")
	}
}

func TestBalanceAuthorizerExactMinimumAllowed(t *testing.T) {
	a := authorizer(&stubBalanceReader{balance: big.NewInt(500)}, &stubEpochSource{epoch: 1}, 500)
	ok, err := a.Authorize(context.Background(), common.Address{0x01})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("a balance equal to the minimum was denied")
	}
}

func TestBalanceAuthorizerPropagatesReadError(t *testing.T) {
	a := authorizer(&stubBalanceReader{err: errors.New("hub down")}, &stubEpochSource{epoch: 1}, 500)
	if _, err := a.Authorize(context.Background(), common.Address{0x01}); err == nil {
		t.Fatal("expected the read error to propagate, got nil")
	}
}

func TestBalanceAuthorizerPropagatesEpochError(t *testing.T) {
	hub := &stubBalanceReader{balance: big.NewInt(1000)}
	a := authorizer(hub, &stubEpochSource{err: errors.New("no height")}, 500)
	if _, err := a.Authorize(context.Background(), common.Address{0x01}); err == nil {
		t.Fatal("expected the epoch error to propagate, got nil")
	}
	if hub.calls != 0 {
		t.Errorf("balance must not be read when the epoch is unknown, got %d reads", hub.calls)
	}
}

// TestBalanceAuthorizerCachesWithinEpoch checks a funded payer's balance is read
// once and reused for the rest of the epoch.
func TestBalanceAuthorizerCachesWithinEpoch(t *testing.T) {
	hub := &stubBalanceReader{balance: big.NewInt(1000)}
	a := authorizer(hub, &stubEpochSource{epoch: 5}, 500)
	payer := common.Address{0x01}

	for range 3 {
		if _, err := a.Authorize(context.Background(), payer); err != nil {
			t.Fatal(err)
		}
	}
	if hub.calls != 1 {
		t.Errorf("expected 1 balance read for a cached funded payer, got %d", hub.calls)
	}
}

// TestBalanceAuthorizerRefetchesOnNewEpoch checks the cache is dropped when the
// epoch advances, since balances may have settled.
func TestBalanceAuthorizerRefetchesOnNewEpoch(t *testing.T) {
	hub := &stubBalanceReader{balance: big.NewInt(1000)}
	epochs := &stubEpochSource{epoch: 5}
	a := authorizer(hub, epochs, 500)
	payer := common.Address{0x01}

	if _, err := a.Authorize(context.Background(), payer); err != nil {
		t.Fatal(err)
	}
	epochs.epoch = 6
	if _, err := a.Authorize(context.Background(), payer); err != nil {
		t.Fatal(err)
	}
	if hub.calls != 2 {
		t.Errorf("expected a re-read on the new epoch, got %d reads", hub.calls)
	}
}

// TestBalanceAuthorizerRereadsUnderfundedForTopup checks a cached below-minimum
// balance is not trusted: a mid-epoch top-up is seen on the next query.
func TestBalanceAuthorizerRereadsUnderfundedForTopup(t *testing.T) {
	hub := &stubBalanceReader{balance: big.NewInt(100)}
	a := authorizer(hub, &stubEpochSource{epoch: 5}, 500)
	payer := common.Address{0x01}

	ok, err := a.Authorize(context.Background(), payer)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("underfunded payer was allowed")
	}

	hub.balance = big.NewInt(1000) // the payer tops up within the same epoch
	ok, err = a.Authorize(context.Background(), payer)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("top-up within the epoch was not seen; the underfunded balance was cached")
	}
	if hub.calls != 2 {
		t.Errorf("expected a re-read after the underfunded result, got %d reads", hub.calls)
	}
}

// TestBalanceAuthorizerDoesNotCacheUnderfunded checks below-minimum balances are
// not stored: they never produce a cache hit, so caching them would only grow the
// map with dead entries under many distinct underfunded payers.
func TestBalanceAuthorizerDoesNotCacheUnderfunded(t *testing.T) {
	hub := &stubBalanceReader{balance: big.NewInt(100)}
	a := authorizer(hub, &stubEpochSource{epoch: 5}, 500)

	for i := range 5 {
		ok, err := a.Authorize(context.Background(), common.Address{byte(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("underfunded payer was allowed")
		}
	}

	a.mu.Lock()
	n := len(a.cache)
	a.mu.Unlock()
	if n != 0 {
		t.Errorf("below-minimum balances must not be cached, got %d entries", n)
	}
}

// TestBalanceAuthorizerDropsStaleEpochWrite checks a balance read that starts in
// one epoch and finishes after the epoch advanced is not cached under the new
// epoch, which would otherwise serve that stale balance for the rest of it.
func TestBalanceAuthorizerDropsStaleEpochWrite(t *testing.T) {
	hub := &gateBalanceReader{balance: big.NewInt(1000), entered: make(chan struct{}), release: make(chan struct{})}
	epochs := &stubEpochSource{epoch: 5}
	a := authorizer(hub, epochs, 500)
	payer := common.Address{0x01}

	done := make(chan struct{})
	var authErr error
	go func() {
		_, authErr = a.Authorize(context.Background(), payer)
		close(done)
	}()

	<-hub.entered // the balance read is in flight, having snapshotted epoch 5
	epochs.epoch = 6
	// A concurrent query in epoch 6 resets the cache before the epoch-5 read returns.
	a.cachedFunded(6, common.Address{0x02})
	close(hub.release) // let the epoch-5 read finish and attempt its write
	<-done

	if authErr != nil {
		t.Fatal(authErr)
	}
	if a.cachedFunded(6, payer) {
		t.Error("a balance read against the previous epoch was cached under the new one")
	}
}
