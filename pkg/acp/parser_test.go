package acp

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

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

func newPostJSON(t *testing.T, body []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/v0/graphql", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}
