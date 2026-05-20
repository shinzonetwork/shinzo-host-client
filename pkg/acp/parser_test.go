package acp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExtractCollections_PostJSONSingleField(t *testing.T) {
	body := []byte(`{"query":"{ FilteredLogs { hash } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testViewFilteredLogs}, got)
}

func TestExtractCollections_PostJSONMultipleFields(t *testing.T) {
	body := []byte(`{"query":"{ FilteredLogs { hash } Block { number } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{testViewFilteredLogs, "Block"}, got)
}

// Aliases must not mask the underlying field name: gating decisions are made
// on the collection the operation actually selects, not on the local label.
func TestExtractCollections_AliasResolvesToFieldName(t *testing.T) {
	body := []byte(`{"query":"{ first: FilteredLogs { hash } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testViewFilteredLogs}, got)
}

func TestExtractCollections_DeduplicatesRepeatedSelections(t *testing.T) {
	body := []byte(`{"query":"{ Logs { a } Logs { b } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{"Logs"}, got)
}

// When operationName is set, only that operation's selections are returned.
// This matters for clients that send multi-operation documents and pick at
// request time; gating the unselected operations would cause false denials.
func TestExtractCollections_OperationNameFiltersToNamedOp(t *testing.T) {
	body := []byte(`{
		"query": "query A { Foo { x } } query B { Bar { y } }",
		"operationName": "B"
	}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testOpBNamed}, got)
}

func TestExtractCollections_NoOperationNameWalksAllOps(t *testing.T) {
	body := []byte(`{"query":"query A { Foo { x } } query B { Bar { y } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	sort.Strings(got)
	require.Equal(t, []string{testOpBNamed, "Foo"}, got)
}

func TestExtractCollections_MutationTopLevelFields(t *testing.T) {
	body := []byte(`{"query":"mutation { update_FilteredLogs(input: {}) { _docID } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{"update_FilteredLogs"}, got)
}

func TestExtractCollections_PostGraphQLContentType(t *testing.T) {
	body := []byte(`{ FilteredLogs { hash } }`)
	r := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/graphql")

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testViewFilteredLogs}, got)
}

func TestExtractCollections_GetQueryParam(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/v0/graphql?query=%7B+FilteredLogs+%7B+hash+%7D+%7D", nil)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testViewFilteredLogs}, got)
}

func TestExtractCollections_GetWithOperationName(t *testing.T) {
	r := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/graphql?query=query+A+%7BFoo%7Bx%7D%7D+query+B+%7BBar%7By%7D%7D&operationName=B",
		nil,
	)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testOpBNamed}, got)
}

func TestExtractCollections_EmptyBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", bytes.NewReader(nil))
	r.Header.Set("Content-Type", "application/json")

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestExtractCollections_EmptyQueryField(t *testing.T) {
	body := []byte(`{"query":""}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestExtractCollections_MalformedJSONReturnsErr(t *testing.T) {
	body := []byte(`{"query": "{ Foo { x }`) // truncated json
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMalformedQuery)
}

func TestExtractCollections_MalformedGraphQLReturnsErr(t *testing.T) {
	body := []byte(`{"query":"{ Foo { x "}`) // unterminated selection set
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMalformedQuery)
}

// The middleware must hand the request body off to the downstream handler
// unchanged; reading the body for parsing without resetting it would cause
// defradb to receive an empty payload.
func TestExtractCollections_BodyReadableAfterExtraction(t *testing.T) {
	body := []byte(`{"query":"{ Foo { x } }"}`)
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.NoError(t, err)

	remaining, readErr := io.ReadAll(r.Body)
	require.NoError(t, readErr)
	require.Equal(t, body, remaining, "body must be re-readable for downstream handlers")
}

// Unsupported methods (DELETE, PATCH, etc.) are not gated; the middleware
// passes them through unchanged. defradb will respond with whatever it
// considers correct for that method.
func TestExtractCollections_UnsupportedMethodReturnsNil(t *testing.T) {
	r := httptest.NewRequest(http.MethodDelete, "/api/v0/graphql", nil)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Nil(t, got)
}

// Content-Type with parameters (e.g. charset) still resolves to the base
// media type. application/json; charset=utf-8 is treated as JSON.
func TestExtractCollections_ContentTypeWithParametersStillJSON(t *testing.T) {
	body := []byte(`{"query":"{ Foo { x } }"}`)
	r := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{"Foo"}, got)
}

// Introspection (`__schema`, `__type`) is a real top-level field. The
// parser returns it; whether to gate is the registry's call (not a view).
func TestExtractCollections_IntrospectionFieldReturned(t *testing.T) {
	body := []byte(`{"query":"{ __schema { types { name } } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{"__schema"}, got)
}

// ErrMalformedQuery must be reachable via errors.Is so middleware can
// branch on it without string-matching.
func TestErrMalformedQuery_IsRecognizable(t *testing.T) {
	body := []byte(`not json at all`)
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.True(t, errors.Is(err, ErrMalformedQuery))
}

// A named fragment spread used at the top level of the operation
// resolves to its declared selection set. Fields inside that selection
// set are top-level fields of the operation for gating purposes.
func TestExtractCollections_NamedFragmentSpreadAtTopLevel(t *testing.T) {
	body := []byte(`{"query":"fragment AsQuery on Query { FilteredLogs { hash } } query { ...AsQuery }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{testViewFilteredLogs}, got)
}

// An inline fragment at the top level introduces no Field of its own in
// the AST. Its selection set carries the actual top-level fields and must
// be traversed.
func TestExtractCollections_InlineFragmentAtTopLevel(t *testing.T) {
	body := []byte(`{"query":"query { ... on Query { FilteredLogs { hash } } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{testViewFilteredLogs}, got)
}

// Nested fragment chain. Spreads expand through arbitrary depth and the
// parser must follow each link.
func TestExtractCollections_NestedFragmentSpreads(t *testing.T) {
	body := []byte(`{"query":"fragment Inner on Query { FilteredLogs { hash } } fragment Outer on Query { ...Inner } query { ...Outer }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{testViewFilteredLogs}, got)
}

// A direct Field and an InlineFragment at the same selection level both
// contribute their fields. The result includes the Field's name and
// every name reachable through the fragment.
func TestExtractCollections_MixedFieldAndInlineFragmentAtTopLevel(t *testing.T) {
	body := []byte(`{"query":"query { Block { number } ... on Query { FilteredLogs { hash } } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Block", testViewFilteredLogs}, got)
}

// Fragments INSIDE a Field's own selection set describe that field's
// sub-shape and do not introduce new top-level fields. The outer Field is
// the only entry; the inline fragment is not traversed further by this
// gate.
func TestExtractCollections_InlineFragmentInsideField(t *testing.T) {
	body := []byte(`{"query":"query { FilteredLogs { ... on FilteredLogs { hash } } }"}`)
	r := newPostJSON(t, body)

	got, err := ExtractCollections(r)
	require.NoError(t, err)
	require.Equal(t, []string{testViewFilteredLogs}, got)
}

// A fragment spread that names a fragment not declared in the document
// has no resolvable selection set. ParseQuery accepts the spread because
// validation is a separate stage; the gate rejects it as malformed
// since the resulting field set is undefined.
func TestExtractCollections_UndeclaredFragmentReturnsErr(t *testing.T) {
	body := []byte(`{"query":"query { ...Missing }"}`)
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrMalformedQuery))
}

// A fragment cycle (A spreads B, B spreads A) is illegal per the GraphQL
// spec but expressible in raw input. The gate must detect the cycle and
// return ErrMalformedQuery in bounded time rather than recursing
// indefinitely or forwarding the request.
func TestExtractCollections_FragmentCycleReturnsErr(t *testing.T) {
	body := []byte(`{"query":"fragment A on Query { ...B } fragment B on Query { ...A } query { ...A }"}`)
	r := newPostJSON(t, body)

	done := make(chan error, 1)
	go func() {
		_, err := ExtractCollections(r)
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrMalformedQuery))
	case <-time.After(2 * time.Second):
		t.Fatal("parser did not terminate on fragment cycle within 2s")
	}
}

// Deep but acyclic fragment nesting is rejected once it exceeds the
// configured depth bound. The user-facing impact is that hand-written
// queries with more than maxFragmentDepth levels of fragment indirection
// fail with a clear error. The bound is generous enough that legitimate
// documents do not hit it.
func TestExtractCollections_ExcessiveFragmentDepthReturnsErr(t *testing.T) {
	var b strings.Builder
	const depth = 32 // exceeds maxFragmentDepth
	for i := 0; i < depth; i++ {
		fmt.Fprintf(&b, "fragment F%d on Query { ...F%d } ", i, i+1)
	}
	fmt.Fprintf(&b, "fragment F%d on Query { FilteredLogs { hash } } query { ...F0 }", depth)

	body := []byte(`{"query":"` + b.String() + `"}`)
	r := newPostJSON(t, body)

	_, err := ExtractCollections(r)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrMalformedQuery))
}

func newPostJSON(t *testing.T, body []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}
