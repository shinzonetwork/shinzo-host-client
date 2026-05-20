// Package acp implements the host-side access-control middleware. The
// middleware authenticates each incoming GraphQL request, identifies which
// view collections it touches, and forwards or rejects based on the
// SourceHub subscriber tuples for the caller.
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
//   - POST with `Content-Type: application/json` body `{"query": ..., "operationName": ...}`
//   - POST with `Content-Type: application/graphql` body (raw query string)
//
// For POST requests the body is read fully and reset on r so downstream
// handlers can re-read it. An empty body or empty `query` returns nil with
// no error so the caller can pass the request through without gating.
// Parse failures, fragment cycles, undeclared fragments, and excessive
// fragment nesting return ErrMalformedQuery so the caller can respond
// 400 rather than forwarding a request whose top-level field set is
// unsafe to determine.
func ExtractCollections(r *http.Request) ([]string, error) {
	query, operationName, err := extractGraphQLOperation(r)
	if err != nil {
		return nil, err
	}
	if query == "" {
		return nil, nil
	}

	doc, parseErr := parser.ParseQuery(&ast.Source{Input: query})
	if parseErr != nil {
		return nil, fmt.Errorf("%w: %s", ErrMalformedQuery, parseErr.Error())
	}

	seen := make(map[string]struct{})
	var names []string
	for _, op := range doc.Operations {
		if operationName != "" && op.Name != operationName {
			continue
		}
		opNames, err := collectTopLevelFields(op.SelectionSet, doc, seen, make(map[string]struct{}), 0)
		if err != nil {
			return nil, err
		}
		names = append(names, opNames...)
	}
	return names, nil
}

// collectTopLevelFields walks a selection set and returns the names of
// every top-level Field reachable from it, recursing into FragmentSpread
// and InlineFragment along the way. seen deduplicates Field names across
// the entire walk; visitedFrags blocks fragment-spread cycles by name.
// depth is bounded by maxFragmentDepth.
//
// "Top-level" means a Field whose parent in the selection tree is either
// the operation itself or another top-level fragment used at the
// operation level. Selections nested inside a Field's own SelectionSet
// describe that field's sub-shape and are not new top-level entries, so
// they are not traversed here.
func collectTopLevelFields(
	sels ast.SelectionSet,
	doc *ast.QueryDocument,
	seen map[string]struct{},
	visitedFrags map[string]struct{},
	depth int,
) ([]string, error) {
	if depth > maxFragmentDepth {
		return nil, fmt.Errorf("%w: fragment depth exceeded %d", ErrMalformedQuery, maxFragmentDepth)
	}

	var names []string
	for _, sel := range sels {
		switch s := sel.(type) {
		case *ast.Field:
			if _, dup := seen[s.Name]; dup {
				continue
			}
			seen[s.Name] = struct{}{}
			names = append(names, s.Name)

		case *ast.InlineFragment:
			nested, err := collectTopLevelFields(s.SelectionSet, doc, seen, visitedFrags, depth+1)
			if err != nil {
				return nil, err
			}
			names = append(names, nested...)

		case *ast.FragmentSpread:
			if _, cycle := visitedFrags[s.Name]; cycle {
				return nil, fmt.Errorf("%w: fragment cycle through %q", ErrMalformedQuery, s.Name)
			}
			frag := doc.Fragments.ForName(s.Name)
			if frag == nil {
				// Undeclared fragment. ParseQuery does not run validation,
				// so the spread can reference a name that has no
				// definition in the document. Reject as malformed rather
				// than forward, since the resolved field set is undefined.
				return nil, fmt.Errorf("%w: undeclared fragment %q", ErrMalformedQuery, s.Name)
			}
			visitedFrags[s.Name] = struct{}{}
			nested, err := collectTopLevelFields(frag.SelectionSet, doc, seen, visitedFrags, depth+1)
			delete(visitedFrags, s.Name)
			if err != nil {
				return nil, err
			}
			names = append(names, nested...)
		}
	}
	return names, nil
}

// extractGraphQLOperation reads the GraphQL query and operationName from r.
// For POST requests it reads the body into memory and rewinds r.Body so
// downstream handlers can re-read it; for GET requests it reads URL params.
// Unsupported methods return ("", "", nil) so the caller passes the request
// through without parsing.
func extractGraphQLOperation(r *http.Request) (query, operationName string, err error) {
	switch r.Method {
	case http.MethodGet:
		return r.URL.Query().Get("query"), r.URL.Query().Get("operationName"), nil

	case http.MethodPost:
		if r.Body == nil {
			return "", "", nil
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			return "", "", fmt.Errorf("read body: %w", readErr)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) == 0 {
			return "", "", nil
		}

		mediaType := contentTypeOf(r)
		if mediaType == contentTypeGraphQL {
			return string(body), "", nil
		}

		var payload struct {
			Query         string `json:"query"`
			OperationName string `json:"operationName"`
		}
		if jsonErr := json.Unmarshal(body, &payload); jsonErr != nil {
			return "", "", fmt.Errorf("%w: %s", ErrMalformedQuery, jsonErr.Error())
		}
		return payload.Query, payload.OperationName, nil

	default:
		return "", "", nil
	}
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
