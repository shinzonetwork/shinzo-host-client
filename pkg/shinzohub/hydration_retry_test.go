package shinzohub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// useFastHydrateRetries shrinks the live-path retry budget for tests
// and restores it on cleanup.
func useFastHydrateRetries(t *testing.T, attempts int, base time.Duration) {
	t.Helper()
	prevAttempts, prevBase := hydrateMaxAttempts, hydrateBaseDelay
	hydrateMaxAttempts, hydrateBaseDelay = attempts, base
	t.Cleanup(func() {
		hydrateMaxAttempts, hydrateBaseDelay = prevAttempts, prevBase
	})
}

// servAfterN returns 404 on the first n requests, then 200.
func servAfterN(t *testing.T, n int, contract, bundle string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if count <= int64(n) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":5,"message":"view not found"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            testViewName,
				Creator:         testViewCreator,
				ContractAddress: contract,
				Data:            bundle,
				Height:          "12345",
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestFetchBundleWithRetry_SucceedsAfterTransient404(t *testing.T) {
	useFastHydrateRetries(t, 5, 1*time.Millisecond)
	wantBundle := makeTestBundle(t, testViewName)

	srv, hits := servAfterN(t, 2, testContractAddress1, wantBundle)

	c := NewRPCClient(srv.URL, nil)
	got, err := fetchBundleWithRetry(context.Background(), c, testContractAddress1)
	require.NoError(t, err)
	require.Equal(t, wantBundle, got)
	require.EqualValues(t, 3, hits.Load(), "expected one success after two transient failures")
}

func TestFetchBundleWithRetry_GivesUpAfterBudget(t *testing.T) {
	useFastHydrateRetries(t, 4, 1*time.Millisecond)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	_, err := fetchBundleWithRetry(context.Background(), c, testContractAddress1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gave up after 4 attempts")
	require.ErrorIs(t, err, ErrLCDHTTPStatus)
	require.EqualValues(t, 4, hits.Load(), "every attempt should hit the server")
}

// PermanentErrorSkipsRetry uses an empty LCD URL: GetViewBundle short-
// circuits before making any HTTP call, so the loop sees a non-transient
// error on its first iteration and returns it unwrapped.
func TestFetchBundleWithRetry_PermanentErrorSkipsRetry(t *testing.T) {
	useFastHydrateRetries(t, 5, 1*time.Millisecond)

	c := NewRPCClient("", nil)
	_, err := fetchBundleWithRetry(context.Background(), c, testContractAddress1)
	require.ErrorIs(t, err, ErrLCDNotConfigured)
	require.NotContains(t, err.Error(), "gave up", "permanent errors bypass retry")
}

// PermanentErrorAfterHTTPCallSkipsRetry covers the path where the server
// is reached but returns 200 with malformed JSON. The decode failure must
// not be retried, and the LCD must see exactly one request.
func TestFetchBundleWithRetry_PermanentErrorAfterHTTPCallSkipsRetry(t *testing.T) {
	useFastHydrateRetries(t, 5, 1*time.Millisecond)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	_, err := fetchBundleWithRetry(context.Background(), c, testContractAddress1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode LCD response")
	require.NotContains(t, err.Error(), "gave up", "decode failure bypasses retry")
	require.EqualValues(t, 1, hits.Load(), "permanent error must not retry")
}

// SucceedsAfterTransientEmptyData covers the other transient case the
// classifier accepts: the LCD answered 200 but the data field was empty
// (view registered but bundle bytes not yet populated). After one such
// response, a follow-up call returns the bundle.
func TestFetchBundleWithRetry_SucceedsAfterTransientEmptyData(t *testing.T) {
	useFastHydrateRetries(t, 5, 1*time.Millisecond)
	wantBundle := makeTestBundle(t, testViewName)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		data := wantBundle
		if count == 1 {
			data = "" // first response: bundle bytes not ready yet
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            testViewName,
				ContractAddress: testContractAddress1,
				Data:            data,
				Height:          "1",
			},
		})
	}))
	defer srv.Close()

	c := NewRPCClient(srv.URL, nil)
	got, err := fetchBundleWithRetry(context.Background(), c, testContractAddress1)
	require.NoError(t, err)
	require.Equal(t, wantBundle, got)
	require.EqualValues(t, 2, hits.Load(), "first empty-data, second populated")
}

func TestFetchBundleWithRetry_RespectsContextCancel(t *testing.T) {
	useFastHydrateRetries(t, 5, 50*time.Millisecond)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	c := NewRPCClient(srv.URL, nil)
	_, err := fetchBundleWithRetry(ctx, c, testContractAddress1)
	require.ErrorIs(t, err, context.Canceled)
	require.LessOrEqual(t, hits.Load(), int64(2), "cancellation should stop the loop early")
}

// fakeNetErr implements the Timeout() probe used by isTransientHydrationErr.
type fakeNetErr struct{ timeout bool }

func (e fakeNetErr) Timeout() bool { return e.timeout }
func (e fakeNetErr) Error() string { return "fake net err" }

func TestIsTransientHydrationErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not transient", nil, false},
		{"lcd http status wrap", fmt.Errorf("%w: HTTP 404", ErrLCDHTTPStatus), true},
		{"lcd empty data wrap", fmt.Errorf("%w: contract 0xabc", ErrLCDEmptyData), true},
		{"context deadline", context.DeadlineExceeded, true},
		{"timeout interface", fakeNetErr{timeout: true}, true},
		{"non-timeout interface", fakeNetErr{timeout: false}, false},
		{"decode failure is permanent", errors.New("decode bundle: invalid wire format"), false},
		{"missing contract is permanent", ErrEventNoContract, false},
		{"lcd not configured is permanent", ErrLCDNotConfigured, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isTransientHydrationErr(tc.err))
		})
	}
}
