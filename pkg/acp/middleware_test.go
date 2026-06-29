package acp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/shinzonetwork/shinzo-host-client/pkg/accounting"
	"github.com/shinzonetwork/shinzo-host-client/pkg/billing"
)

const (
	testChainID          = 91273002
	testViewFilteredLogs = "FilteredLogs"
	testViewFilteredTxs  = "FilteredTxs"
	testContractA        = "0xabc"
	testContractB        = "0xdef"
	testOpBNamed         = "Bar"
)

var errTestAuthzFail = errors.New("test: authorizer transport failure")

// A non-graphql path bypasses the gate entirely. The registry knows the view
// and the authorizer would deny, so passing through proves only the path check
// let it by.
func TestMiddleware_NonGraphQLPathPassesThrough(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v0/collections", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Empty(t, authz.calls, "non-graphql request must not invoke the authorizer")
}

// A query touching no view collections is not billed and passes through without
// a signature.
func TestMiddleware_NonViewQueryPassesWithoutSignature(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := newGraphQLPost([]byte(`{"query":"{ Block { number } }"}`))
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Empty(t, authz.calls)
}

// A view query with no signed extensions is denied; there is no payer to bill.
func TestMiddleware_ViewQueryWithoutSignatureIs403(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := newGraphQLPost([]byte(`{"query":"{ FilteredLogs { hash } }"}`))
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

// A funded payer's signed query is authorized and forwarded, and the authorizer
// is consulted for the recovered payer address.
func TestMiddleware_FundedSignedQueryAllowed(t *testing.T) {
	priv, signer := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil)
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Equal(t, []common.Address{signer}, authz.calls)
}

// The authorizer denies an underfunded payer; the middleware returns 402.
func TestMiddleware_UnderfundedDenied(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: false}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil)
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusPaymentRequired, w.Code)
	require.False(t, next.called)
	require.Len(t, authz.calls, 1)
}

// A transport error from the authorizer is indeterminate, not a deny, and maps
// to 503.
func TestMiddleware_AuthorizerErrorIs503(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{err: errTestAuthzFail}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil)
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.False(t, next.called)
}

// A query that differs from the one signed fails the query_hash binding before
// the balance is ever checked.
func TestMiddleware_TamperedQueryRejected(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	ext, err := billing.SignRequest(testChainID, priv, "{ FilteredLogs { hash } }", nil, 1, 1735689600)
	require.NoError(t, err)
	// Serve a different field selection than was signed; both resolve to the
	// same view so the request reaches verification.
	body, err := json.Marshal(requestBody{Query: "{ FilteredLogs { number } }", Extensions: ext})
	require.NoError(t, err)

	next := &recordingNext{}
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, newGraphQLPost(body))

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls, "verification must fail before the balance check")
}

// A request selecting more than one view cannot be attributed to a single pool
// and is rejected.
func TestMiddleware_MultiViewRejected(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{
		testViewFilteredLogs: testContractA,
		testViewFilteredTxs:  testContractB,
	}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := newGraphQLPost([]byte(`{"query":"{ FilteredLogs { hash } FilteredTxs { hash } }"}`))
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

// A view registered without a contract address fails closed; the authorizer is
// never consulted.
func TestMiddleware_ViewWithoutAddressFailsClosed(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: ""}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	r := newGraphQLPost([]byte(`{"query":"{ FilteredLogs { hash } }"}`))
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

func TestMiddleware_MalformedBodyIs400(t *testing.T) {
	authz := &fakeAuthorizer{}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	next := &recordingNext{}
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, newGraphQLPost([]byte(`{"query": "{ Foo`)))

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

// The middleware reads the body to parse and verify; it must rewind r.Body so
// the wrapped handler sees the original payload on the allow path.
func TestMiddleware_BodyForwardedToNextUnchanged(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil)
	sent, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	r.Body = io.NopCloser(bytes.NewReader(sent))

	next := &recordingNext{}
	mw.Wrap(next).ServeHTTP(httptest.NewRecorder(), r)
	require.Equal(t, sent, next.bodyReceived, "downstream must see the original body")
}

// A request signed with a stale timestamp is rejected before authorization when
// a freshness window is set, so a captured signature cannot be replayed long
// after it was issued.
func TestMiddleware_StaleRequestIs403(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, time.Minute, nil, nil)

	query := "{ FilteredLogs { hash } }"
	stale := uint64(time.Now().Add(-time.Hour).Unix())
	ext, err := billing.SignRequest(testChainID, priv, query, nil, 1, stale)
	require.NoError(t, err)
	body, err := json.Marshal(requestBody{Query: query, Extensions: ext})
	require.NoError(t, err)

	next := &recordingNext{}
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, newGraphQLPost(body))

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls, "a stale request must be rejected before the balance check")
}

// A request signed within the freshness window clears the freshness gate and
// reaches the wrapped handler.
func TestMiddleware_FreshRequestPasses(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, time.Minute, nil, nil)

	query := "{ FilteredLogs { hash } }"
	fresh := uint64(time.Now().Unix())
	ext, err := billing.SignRequest(testChainID, priv, query, nil, 1, fresh)
	require.NoError(t, err)
	body, err := json.Marshal(requestBody{Query: query, Extensions: ext})
	require.NoError(t, err)

	next := &recordingNext{}
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, newGraphQLPost(body))

	require.True(t, next.called, "a fresh request must reach the wrapped handler")
	require.Len(t, authz.calls, 1)
}

// On allow the gate wraps the response writer to count rows; the served body
// must still reach the client unchanged.
func TestMiddleware_AllowForwardsResponseBody(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, nil)

	const served = `{"data":{"FilteredLogs":[{"hash":"a"},{"hash":"b"}]}}`
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(served))
	})

	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil)
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Equal(t, served, w.Body.String(), "client must receive defradb's response unchanged")
}

// A query mixing the single view with a base collection is allowed and served,
// and the gate bills only the view's rows: the base collection is ignored.
func TestMiddleware_MixedViewAndBaseCollectionAllowed(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	core, logs := observer.New(zap.InfoLevel)
	mw := NewMiddleware(authz, reg, testChainID, 0, nil, zap.New(core).Sugar())

	const served = `{"data":{"FilteredLogs":[{"hash":"a"},{"hash":"b"}],"Block":[{"number":1},{"number":2},{"number":3}]}}`
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(served))
	})

	r := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } Block { number } }", nil)
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, r)

	require.Len(t, authz.calls, 1, "a view+base query must be authorized, not rejected")
	require.Equal(t, served, w.Body.String())
	allow := logs.FilterMessage("billing.allow").All()
	require.Len(t, allow, 1)
	require.Equal(t, uint64(2), allow[0].ContextMap()["rows"], "bills the view's 2 rows, not the base collection's 3")
	require.Equal(t, true, allow[0].ContextMap()["served"])
}

// On allow with a served view response the gate submits one service record
// carrying the signed extensions, the view address, the counted rows, and the
// observed attesting set.
func TestMiddleware_RecordsServedQuery(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	rec := &fakeRecorder{}
	mw := NewMiddleware(authz, reg, testChainID, 0, &Recording{
		Recorder:  rec,
		Attesters: func() []string { return []string{"idx1", "idx2"} },
	}, nil)

	query := "{ FilteredLogs { hash } }"
	ext, err := billing.SignRequest(testChainID, priv, query, nil, 1, 1735689600)
	require.NoError(t, err)
	body, err := json.Marshal(requestBody{Query: query, Extensions: ext})
	require.NoError(t, err)

	const served = `{"data":{"FilteredLogs":[{"hash":"a"},{"hash":"b"}]}}`
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(served))
	})
	mw.Wrap(next).ServeHTTP(httptest.NewRecorder(), newGraphQLPost(body))

	require.Len(t, rec.inputs, 1)
	in := rec.inputs[0]
	require.Equal(t, ext, in.Extensions)
	require.Equal(t, common.HexToAddress(testContractA), in.ViewAddress)
	require.Equal(t, uint64(2), in.RowsQueried)
	require.Equal(t, []string{"idx1", "idx2"}, in.AttestedIndexers)
}

// A query the gate allows but whose response carries no view rows (a failed
// query returning data null) is served but not recorded: only served queries are
// billed.
func TestMiddleware_FailedQueryNotRecorded(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	rec := &fakeRecorder{}
	mw := NewMiddleware(authz, reg, testChainID, 0, &Recording{Recorder: rec}, nil)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"boom"}]}`))
	})
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil))

	require.Equal(t, http.StatusOK, w.Code, "the query is served; only recording is skipped")
	require.Empty(t, rec.inputs, "a failed query must not be recorded")
}

// A recorder error is logged and swallowed: the served response is unchanged,
// because recording is best-effort.
func TestMiddleware_RecordFailureStillServes(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	rec := &fakeRecorder{err: errors.New("accounting service unreachable")}
	core, logs := observer.New(zap.InfoLevel)
	mw := NewMiddleware(authz, reg, testChainID, 0, &Recording{Recorder: rec}, zap.New(core).Sugar())

	const served = `{"data":{"FilteredLogs":[{"hash":"a"}]}}`
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(served))
	})
	w := httptest.NewRecorder()
	mw.Wrap(next).ServeHTTP(w, signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, served, w.Body.String(), "a recording failure must not alter the served response")
	require.Len(t, rec.inputs, 1, "the record was attempted")
	require.Len(t, logs.FilterMessage("billing.record_failed").All(), 1, "the failure is logged")
}

// The record submission runs on a context detached from the request, so a client
// that disconnects does not cancel the in-flight POST. Driving the request with an
// already-cancelled context proves the recorder still receives a live,
// deadline-bounded one.
func TestMiddleware_RecordUsesDetachedContext(t *testing.T) {
	priv, _ := newKey(t)
	authz := &fakeAuthorizer{allow: true}
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	rec := &fakeRecorder{}
	mw := NewMiddleware(authz, reg, testChainID, 0, &Recording{Recorder: rec}, nil)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"FilteredLogs":[{"hash":"a"}]}}`))
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := signedGraphQLPost(t, priv, "{ FilteredLogs { hash } }", nil).WithContext(ctx)
	mw.Wrap(next).ServeHTTP(httptest.NewRecorder(), req)

	require.Len(t, rec.inputs, 1)
	require.NoError(t, rec.ctxErr, "recording must not inherit request-context cancellation")
	require.True(t, rec.ctxHasDeadline, "recording context must be time-bounded")
}

// --- helpers ---

type requestBody struct {
	Query      string             `json:"query"`
	Variables  json.RawMessage    `json:"variables,omitempty"`
	Extensions billing.Extensions `json:"extensions"`
}

func newKey(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()
	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	return priv, crypto.PubkeyToAddress(priv.PublicKey)
}

func newGraphQLPost(body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, GraphQLPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func signedGraphQLPost(t *testing.T, priv *ecdsa.PrivateKey, query string, vars json.RawMessage) *http.Request {
	t.Helper()
	ext, err := billing.SignRequest(testChainID, priv, query, vars, 1, 1735689600)
	require.NoError(t, err)
	body, err := json.Marshal(requestBody{Query: query, Variables: vars, Extensions: ext})
	require.NoError(t, err)
	return newGraphQLPost(body)
}

// fakeAuthorizer records every payer it is asked about and returns a fixed
// decision. An unconfigured authorizer denies.
type fakeAuthorizer struct {
	allow bool
	err   error
	calls []common.Address
}

func (f *fakeAuthorizer) Authorize(_ context.Context, payer common.Address) (bool, error) {
	f.calls = append(f.calls, payer)
	return f.allow, f.err
}

// fakeRegistry treats every key in views as a registered view; an empty-string
// value models "registered but no contract address" for fail-closed tests.
type fakeRegistry struct {
	views map[string]string
}

func (f fakeRegistry) IsView(name string) bool {
	_, ok := f.views[name]
	return ok
}

func (f fakeRegistry) ContractAddress(name string) (string, bool) {
	addr, ok := f.views[name]
	if !ok || addr == "" {
		return "", false
	}
	return addr, true
}

// recordingNext is the wrapped handler; tests inspect whether it was reached and
// what body it observed.
type recordingNext struct {
	called       bool
	bodyReceived []byte
}

func (n *recordingNext) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.called = true
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		n.bodyReceived = body
	}
	w.WriteHeader(http.StatusOK)
}

// fakeRecorder captures what the gate submits and snapshots the context's state
// at call time, so tests can assert the submission and that recording runs on a
// live, deadline-bounded context. err exercises the best-effort failure path.
type fakeRecorder struct {
	inputs         []accounting.RecordInput
	ctxErr         error
	ctxHasDeadline bool
	err            error
}

func (f *fakeRecorder) Record(ctx context.Context, in accounting.RecordInput) error {
	f.ctxErr = ctx.Err()
	_, f.ctxHasDeadline = ctx.Deadline()
	f.inputs = append(f.inputs, in)
	return f.err
}
