// Package canonical derives the query_hash that binds a billing request to the
// exact GraphQL query and variables the client signed over.
//
//	query_hash = keccak256(JCS({"query": query, "variables": variables}))
//
// JCS is RFC 8785 (JSON Canonicalization Scheme). The query stays an opaque
// string and is never re-parsed as GraphQL, so the hash does not depend on how
// the client laid out the query text. The browser client computes the same
// value with canonicalize + js-sha3; the golden vectors under testdata/golden
// gate that the two paths agree byte for byte.
package canonical

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/gowebpki/jcs"
	"golang.org/x/crypto/sha3"
)

// maxSafeInteger is 2^53. At or above it, IEEE-754 doubles can no longer hold
// every integer, and JSON numbers decode to doubles. Such a value would round,
// letting two distinct inputs collide on one query_hash, so we reject it and
// require callers to send large values as strings.
var maxSafeInteger = new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 53))

// QueryHash returns keccak256 over the JCS canonicalization of
// {"query": query, "variables": variables}, plus the canonical bytes it hashed
// (the preimage, which the host can log when a recompute mismatches).
//
// variables must be a JSON object; nil, empty, or JSON null is treated as {}.
// It errors if variables is not an object, or holds a number anywhere in the
// tree whose magnitude is >= 2^53.
func QueryHash(query string, variables json.RawMessage) (hash [32]byte, canonicalBytes []byte, err error) {
	vars := bytes.TrimSpace(variables)
	if len(vars) == 0 || bytes.Equal(vars, []byte("null")) {
		vars = []byte("{}")
	}
	if err := checkVariables(vars); err != nil {
		return [32]byte{}, nil, err
	}

	envelope, err := json.Marshal(struct {
		Query     string          `json:"query"`
		Variables json.RawMessage `json:"variables"`
	}{Query: query, Variables: vars})
	if err != nil {
		return [32]byte{}, nil, fmt.Errorf("marshal envelope: %w", err)
	}

	// Hash the JCS output, not the json.Marshal bytes: encoding/json escapes <,
	// >, and & as \u00xx where JCS and the JS client do not. Canonicalizing
	// re-serializes from the parsed value and drops that divergence.
	canonicalBytes, err = jcs.Transform(envelope)
	if err != nil {
		return [32]byte{}, nil, fmt.Errorf("jcs transform: %w", err)
	}

	h := sha3.NewLegacyKeccak256()
	h.Write(canonicalBytes)
	copy(hash[:], h.Sum(nil))
	return hash, canonicalBytes, nil
}

// checkVariables confirms vars is a JSON object and that no number in it has
// magnitude >= 2^53. It decodes with UseNumber so the bound is tested against
// the original literal, before any rounding to float64.
func checkVariables(vars []byte) error {
	dec := json.NewDecoder(bytes.NewReader(vars))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("decode variables: %w", err)
	}
	if dec.More() {
		return errors.New("variables must be a single JSON object")
	}
	if _, ok := v.(map[string]any); !ok {
		return errors.New("variables must be a JSON object")
	}
	return checkBounds(v)
}

// checkBounds walks decoded JSON and returns an error for the first number it
// finds with magnitude >= 2^53.
func checkBounds(v any) error {
	switch t := v.(type) {
	case map[string]any:
		for _, child := range t {
			if err := checkBounds(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range t {
			if err := checkBounds(child); err != nil {
				return err
			}
		}
	case json.Number:
		r, ok := new(big.Rat).SetString(t.String())
		if !ok {
			return fmt.Errorf("invalid number %q", t.String())
		}
		if new(big.Rat).Abs(r).Cmp(maxSafeInteger) >= 0 {
			return fmt.Errorf("number %s has magnitude >= 2^53; send large values as strings", t.String())
		}
	}
	return nil
}
