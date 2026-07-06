package acp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// ErrMalformedQuery wraps any failure to parse the GraphQL operation in a
// request. The middleware treats it as a client error (400).
var ErrMalformedQuery = errors.New("malformed graphql query")

const contentTypeGraphQL = "application/graphql"

// maxFragmentDepth bounds recursion through fragment spreads and inline
// fragments. Fragment-spread cycles are detected separately by name (see
// visitedFrags in collectTopLevelFields); the depth bound additionally
// caps deeply nested non-cyclic fragments and inline-fragment chains,
// which have no name to track. Sixteen levels exceeds any reasonable
// hand-written nesting.
const maxFragmentDepth = 16

// ExtractCollections returns the top-level field names selected by the
// GraphQL operation in r, deduplicated, with aliases resolved back to the
// underlying field name. Fragment spreads and inline fragments used at
// the operation's top level are followed, so the fields they introduce
// are included in the result.
//
// Supported transports:
//   - GET with `query` (and optional `operationName`) URL parameters
//   - POST with `query` (and optional `operationName`) URL parameters
//   - POST with `Content-Type: application/json` body `{"query": ..., "operationName": ...}`
//   - POST with `Content-Type: application/graphql` body (raw query string)
//
// On POST a non-empty URL `query` parameter takes precedence over the
// body. This mirrors defradb's GraphQL handler so the middleware gates
// the same operation defradb will execute.
//
// For POST requests that fall through to the body, the body is read
// fully and reset on r so downstream handlers can re-read it. An empty
// body or empty `query` returns nil with no error so the caller can pass
// the request through without gating.
// Parse failures, fragment cycles, undeclared fragments, and excessive
// fragment nesting return ErrMalformedQuery so the caller can respond
// 400 rather than forwarding a request whose top-level field set is
// unsafe to determine.
func ExtractCollections(r *http.Request) ([]string, error) {
	req, err := parseGraphQLRequest(r)
	if err != nil {
		return nil, err
	}
	return collectionsFromQuery(req.Query, req.OperationName)
}

// topLevelField is one top-level selection: ResponseKey is the key it appears
// under in the response (its alias, or the field name when unaliased), and Name
// is the underlying field.
type topLevelField struct {
	ResponseKey string
	Name        string
}

// topLevelFields returns the top-level fields selected by query, following
// fragment spreads and inline fragments, deduplicated by response key (fields
// that share a response key merge into one in the response). operationName, when
// set, restricts the walk to that named operation. An empty query returns nil so
// the caller can pass the request through without gating.
func topLevelFields(query, operationName string) ([]topLevelField, error) {
	if query == "" {
		return nil, nil
	}

	doc, parseErr := parser.ParseQuery(&ast.Source{Input: query})
	if parseErr != nil {
		return nil, fmt.Errorf("%w: %s", ErrMalformedQuery, parseErr.Error())
	}

	seen := make(map[string]struct{})
	var fields []topLevelField
	for _, op := range doc.Operations {
		if operationName != "" && op.Name != operationName {
			continue
		}
		opFields, err := collectTopLevelFields(op.SelectionSet, doc, seen, make(map[string]struct{}), 0)
		if err != nil {
			return nil, err
		}
		fields = append(fields, opFields...)
	}
	return fields, nil
}

// collectionsFromQuery returns the distinct underlying field names selected at
// the top level of query, with aliases resolved to the field name. Used to detect
// which views a request touches. An empty query returns nil.
func collectionsFromQuery(query, operationName string) ([]string, error) {
	fields, err := topLevelFields(query, operationName)
	if err != nil {
		return nil, err
	}
	return collectionNames(fields), nil
}

// collectionNames returns the distinct underlying field names from fields, in
// first-seen order.
func collectionNames(fields []topLevelField) []string {
	seen := make(map[string]struct{})
	var names []string
	for _, f := range fields {
		if _, dup := seen[f.Name]; dup {
			continue
		}
		seen[f.Name] = struct{}{}
		names = append(names, f.Name)
	}
	return names
}

// responseKeys returns the response keys under which the field named name
// appears. A view selected more than once (aliased) yields one key per selection.
func responseKeys(fields []topLevelField, name string) []string {
	var keys []string
	for _, f := range fields {
		if f.Name == name {
			keys = append(keys, f.ResponseKey)
		}
	}
	return keys
}

// collectTopLevelFields walks a selection set and returns every top-level field
// reachable from it, recursing into FragmentSpread and InlineFragment. seen
// deduplicates by response key (fields sharing a response key merge in the
// response); visitedFrags blocks fragment-spread cycles by name; depth is bounded
// by maxFragmentDepth.
//
// "Top-level" means a field whose parent is the operation itself or a top-level
// fragment used at the operation level. Selections nested inside a field's own
// SelectionSet describe that field's sub-shape and are not traversed here.
func collectTopLevelFields(
	sels ast.SelectionSet,
	doc *ast.QueryDocument,
	seen map[string]struct{},
	visitedFrags map[string]struct{},
	depth int,
) ([]topLevelField, error) {
	if depth > maxFragmentDepth {
		return nil, fmt.Errorf("%w: fragment depth exceeded %d", ErrMalformedQuery, maxFragmentDepth)
	}

	var fields []topLevelField
	for _, sel := range sels {
		switch s := sel.(type) {
		case *ast.Field:
			if _, dup := seen[s.Alias]; dup {
				continue
			}
			seen[s.Alias] = struct{}{}
			fields = append(fields, topLevelField{ResponseKey: s.Alias, Name: s.Name})

		case *ast.InlineFragment:
			nested, err := collectTopLevelFields(s.SelectionSet, doc, seen, visitedFrags, depth+1)
			if err != nil {
				return nil, err
			}
			fields = append(fields, nested...)

		case *ast.FragmentSpread:
			if _, cycle := visitedFrags[s.Name]; cycle {
				return nil, fmt.Errorf("%w: fragment cycle through %q", ErrMalformedQuery, s.Name)
			}
			frag := doc.Fragments.ForName(s.Name)
			if frag == nil {
				// ParseQuery does not validate, so a spread may name a fragment with
				// no definition. Reject as malformed since the field set is undefined.
				return nil, fmt.Errorf("%w: undeclared fragment %q", ErrMalformedQuery, s.Name)
			}
			visitedFrags[s.Name] = struct{}{}
			nested, err := collectTopLevelFields(frag.SelectionSet, doc, seen, visitedFrags, depth+1)
			delete(visitedFrags, s.Name)
			if err != nil {
				return nil, err
			}
			fields = append(fields, nested...)
		}
	}
	return fields, nil
}

// graphQLRequest is the parsed content of a GraphQL request: the operation, plus
// the variables and the signed billing envelope carried under "extensions".
// Variables and Extensions are nil when the transport does not carry them.
type graphQLRequest struct {
	Query         string
	OperationName string
	Variables     json.RawMessage
	Extensions    json.RawMessage
}

// parseGraphQLRequest reads the GraphQL request from r across the supported
// transports. On POST a non-empty URL `query` parameter takes precedence over
// the body, matching defradb's handler. When the body path is taken, the body
// is read into memory and r.Body is rewound so downstream handlers can re-read
// it. Unsupported methods return a zero request so the caller passes the request
// through without gating.
func parseGraphQLRequest(r *http.Request) (graphQLRequest, error) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		return graphQLRequest{
			Query:         q.Get("query"),
			OperationName: q.Get("operationName"),
			Variables:     rawOrNil(q.Get("variables")),
		}, nil

	case http.MethodPost:
		// Defradb prefers URL `query` over the body on POST. Mirror that
		// so the middleware sees the same query defradb will execute.
		if q := r.URL.Query().Get("query"); q != "" {
			return graphQLRequest{
				Query:         q,
				OperationName: r.URL.Query().Get("operationName"),
				Variables:     rawOrNil(r.URL.Query().Get("variables")),
			}, nil
		}
		if r.Body == nil {
			return graphQLRequest{}, nil
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			return graphQLRequest{}, fmt.Errorf("read body: %w", readErr)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) == 0 {
			return graphQLRequest{}, nil
		}

		if contentTypeOf(r) == contentTypeGraphQL {
			return graphQLRequest{Query: string(body)}, nil
		}

		var payload struct {
			Query         string          `json:"query"`
			OperationName string          `json:"operationName"`
			Variables     json.RawMessage `json:"variables"`
			Extensions    json.RawMessage `json:"extensions"`
		}
		if jsonErr := json.Unmarshal(body, &payload); jsonErr != nil {
			return graphQLRequest{}, fmt.Errorf("%w: %s", ErrMalformedQuery, jsonErr.Error())
		}
		return graphQLRequest{
			Query:         payload.Query,
			OperationName: payload.OperationName,
			Variables:     payload.Variables,
			Extensions:    payload.Extensions,
		}, nil

	default:
		return graphQLRequest{}, nil
	}
}

// rawOrNil returns s as json.RawMessage, or nil when s is empty.
func rawOrNil(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	return json.RawMessage(s)
}

// contentTypeOf returns the media type portion of the request's
// Content-Type header, lowercased, with any parameters stripped.
func contentTypeOf(r *http.Request) string {
	ct := r.Header.Get("Content-Type")
	if semi := strings.IndexByte(ct, ';'); semi >= 0 {
		ct = ct[:semi]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}
