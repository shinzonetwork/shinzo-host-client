package acp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// Shared test constants used across the acp package's test suite.
const (
	testViewFilteredLogs = "FilteredLogs"
	testViewFilteredTxs  = "FilteredTxs"
	testContractA        = "0xabc"
	testContractB        = "0xdef"
	testDIDAlice         = "did:key:alice"
	testDIDEve           = "did:key:eve"
	testOpBNamed         = "Bar"
)

// errTestAuthzFail simulates a transport-level failure from the
// authorizer (e.g., SourceHub unreachable). Declared at package scope
// so test assertions can branch on it via errors.Is.
var errTestAuthzFail = errors.New("test: authorizer transport failure")

// Non-graphql paths must bypass the gating logic entirely. The
// registry and request are set up so that, if the path check were
// removed, the request would be denied: the body references a view
// the registry knows about and the authenticator refuses every caller.
// Passing through under that configuration proves the path check is
// the only thing skipping the gate.
func TestMiddleware_NonGraphQLPathPassesThrough(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(noAuth{}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v0/collections", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Empty(t, authz.calls, "non-graphql request must not invoke the authorizer")
}

// A graphql request that touches no view collections requires no
// authentication and passes through to next.
func TestMiddleware_NonViewQueryPassesWithoutAuth(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{}}
	mw := NewMiddleware(noAuth{}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ Block { number } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Empty(t, authz.calls)
}

// A view collection without authentication is denied. The middleware
// does not consult the authorizer because there is no caller identity
// to evaluate.
func TestMiddleware_ViewQueryWithoutAuthIs403(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(noAuth{}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

// Happy path: the caller is authenticated and the authorizer allows.
// Middleware forwards to next.
func TestMiddleware_ViewQueryWithValidAuth_Allowed(t *testing.T) {
	authz := newFakeAuthorizer()
	authz.allow(testDIDAlice, testContractA)
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Equal(t, []authzCall{{testDIDAlice, testContractA}}, authz.calls)
}

// The authorizer denies the request; middleware returns 403 without
// calling next.
func TestMiddleware_ViewQueryDeniedByAuthorizer_403(t *testing.T) {
	authz := newFakeAuthorizer() // default: deny everything
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDEve}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	require.Len(t, authz.calls, 1, "the authorizer must be consulted before denying")
}

// A transport-level error from the authorizer (SourceHub unreachable,
// gRPC failure, etc.) is an indeterminate result, not a deny. The
// middleware maps it to 503 so the caller can retry once the backend
// recovers.
func TestMiddleware_ViewQueryAuthorizerError_503(t *testing.T) {
	authz := newFakeAuthorizer()
	authz.fail(testDIDAlice, testContractA, errTestAuthzFail)
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.False(t, next.called)
}

// A view that is registered but missing its contract address fails
// closed (503). The authorizer is never consulted because we don't have
// an object_id to look up against.
func TestMiddleware_ViewWithoutAddressFailsClosed(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: ""}} // registered, no address
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls, "fail-closed must not consult the authorizer")
}

// When a single request selects multiple views, all must be authorized.
// One denial fails the whole request even if other views would have
// been allowed.
func TestMiddleware_MultipleViewsAllMustPass(t *testing.T) {
	authz := newFakeAuthorizer()
	authz.allow(testDIDAlice, testContractA)
	// 0xdef is not in the allow map, so it defaults to deny.
	reg := fakeRegistry{
		views: map[string]string{
			testViewFilteredLogs: testContractA,
			testViewFilteredTxs:  testContractB,
		},
	}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } FilteredTxs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
	// The middleware short-circuits on first deny; at least one call
	// must have happened, but the second view may not be consulted.
	require.GreaterOrEqual(t, len(authz.calls), 1)
}

// A request mixing a view and a primitive collection is gated only on
// the view; the primitive part passes through.
func TestMiddleware_MixedViewAndPrimitive_AuthorizedOnViewOnly(t *testing.T) {
	authz := newFakeAuthorizer()
	authz.allow(testDIDAlice, testContractA)
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ Block { number } FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
	require.Equal(t, []authzCall{{testDIDAlice, testContractA}}, authz.calls,
		"the authorizer must be consulted only for view collections")
}

// Body must remain readable by the wrapped handler. The middleware
// reads the body to parse the GraphQL operation; if it does not rewind
// r.Body, the wrapped handler sees an empty payload.
func TestMiddleware_BodyForwardedToNextUnchanged(t *testing.T) {
	authz := newFakeAuthorizer()
	authz.allow(testDIDAlice, testContractA)
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newGraphQLPost(body)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, body, next.bodyReceived, "downstream must see the original body")
}

// A malformed JSON body returns 400 immediately. The authorizer is not
// consulted; we cannot determine what the request was asking for.
func TestMiddleware_MalformedBodyIs400(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(stubAuth{did: testDIDAlice}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	r := newGraphQLPost([]byte(`{"query": "{ Foo`)) // truncated
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.False(t, next.called)
	require.Empty(t, authz.calls)
}

// GET requests are gated the same as POST: a view query without auth
// returns 403.
func TestMiddleware_GETViewQueryWithoutAuthIs403(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(noAuth{}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	r := httptest.NewRequest(http.MethodGet, GraphQLPath+"?query=%7B+FilteredLogs+%7B+hash+%7D+%7D", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, next.called)
}

// An empty POST body on the graphql path has no collections to gate;
// the request passes through and defradb decides how to respond.
func TestMiddleware_EmptyBodyPassesThrough(t *testing.T) {
	authz := newFakeAuthorizer()
	reg := fakeRegistry{views: map[string]string{testViewFilteredLogs: testContractA}}
	mw := NewMiddleware(noAuth{}, authz, reg, nil)

	next := &recordingNext{}
	srv := mw.Wrap(next)

	r := newGraphQLPost(nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, next.called)
}

// --- helpers ---

func newGraphQLPost(body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, GraphQLPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

// noAuth fails every CallerDID request with ErrAuthRequired, modelling
// "the caller did not send a valid bearer token".
type noAuth struct{}

func (noAuth) CallerDID(*http.Request) (string, error) {
	return "", ErrAuthRequired
}

// stubAuth always reports the same DID; used in tests where the caller
// identity is given.
type stubAuth struct{ did string }

func (s stubAuth) CallerDID(*http.Request) (string, error) {
	return s.did, nil
}

type authzCall struct {
	did  string
	addr string
}

type authzDecision struct {
	allow bool
	err   error
}

// fakeAuthorizer records every AuthorizeView call and returns a
// pre-configured decision keyed by (callerDID, contractAddr). Unknown
// keys default to deny so a test that forgets to set an expectation
// fails fast.
type fakeAuthorizer struct {
	decisions map[string]authzDecision
	calls     []authzCall
}

func newFakeAuthorizer() *fakeAuthorizer {
	return &fakeAuthorizer{decisions: make(map[string]authzDecision)}
}

func (f *fakeAuthorizer) allow(did, addr string) {
	f.decisions[did+"|"+addr] = authzDecision{allow: true}
}

func (f *fakeAuthorizer) fail(did, addr string, err error) {
	f.decisions[did+"|"+addr] = authzDecision{err: err}
}

func (f *fakeAuthorizer) AuthorizeView(_ context.Context, did, addr string) (bool, error) {
	f.calls = append(f.calls, authzCall{did, addr})
	d := f.decisions[did+"|"+addr]
	return d.allow, d.err
}

// fakeRegistry treats every key in `views` as a registered view; an
// empty-string value models "registered but no contract address" so
// fail-closed behavior can be tested without touching view.Manager.
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

// recordingNext is the wrapped handler; tests inspect it to verify
// whether the middleware forwarded the request and what body the
// downstream handler observed.
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
