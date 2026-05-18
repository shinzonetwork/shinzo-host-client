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

// ExtractCollections returns the top-level field names selected by the
// GraphQL operation in r, deduplicated, with aliases resolved back to the
// underlying field name.
//
// Supported transports:
//   - GET with `query` (and optional `operationName`) URL parameters
//   - POST with `Content-Type: application/json` body `{"query": ..., "operationName": ...}`
//   - POST with `Content-Type: application/graphql` body (raw query string)
//
// For POST requests the body is read fully and reset on r so downstream
// handlers can re-read it. An empty body or empty `query` returns nil with
// no error so the caller can pass the request through without gating.
// Parse failures return ErrMalformedQuery so the caller can respond 400
// rather than silently allowing an un-gated request through.
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
		for _, sel := range op.SelectionSet {
			field, ok := sel.(*ast.Field)
			if !ok {
				continue
			}
			if _, dup := seen[field.Name]; dup {
				continue
			}
			seen[field.Name] = struct{}{}
			names = append(names, field.Name)
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
