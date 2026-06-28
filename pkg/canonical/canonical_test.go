package canonical

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// update rewrites the canonical/hash fields of the golden file from QueryHash
// output, keeping the inputs. testdata/golden/verify.mjs (canonicalize + js-sha3)
// recomputes the same file independently, so a regenerated golden must be
// re-checked with verify.mjs to confirm the JS client still agrees.
var update = flag.Bool("update", false, "regenerate testdata/golden/canonical.json from QueryHash")

var goldenPath = filepath.Join("testdata", "golden", "canonical.json")

type vector struct {
	Name      string          `json:"name"`
	Query     string          `json:"query"`
	Variables json.RawMessage `json:"variables"`
	Canonical string          `json:"canonical"`
	Hash      string          `json:"hash"`
}

type reject struct {
	Name      string          `json:"name"`
	Query     string          `json:"query"`
	Variables json.RawMessage `json:"variables"`
}

type golden struct {
	Vectors []vector `json:"vectors"`
	Rejects []reject `json:"rejects"`
}

func loadGolden(t *testing.T) *golden {
	t.Helper()
	b, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g golden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	return &g
}

// TestQueryHashGolden checks QueryHash reproduces the frozen canonical bytes and
// hash for every vector. Passing here plus a passing verify.mjs is the
// cross-language guarantee that the browser client hashes identically.
func TestQueryHashGolden(t *testing.T) {
	g := loadGolden(t)
	if len(g.Vectors) == 0 {
		t.Fatal("golden file has no vectors")
	}

	if *update {
		for i := range g.Vectors {
			hash, canonical, err := QueryHash(g.Vectors[i].Query, g.Vectors[i].Variables)
			if err != nil {
				t.Fatalf("update %s: %v", g.Vectors[i].Name, err)
			}
			g.Vectors[i].Canonical = string(canonical)
			g.Vectors[i].Hash = "0x" + hex.EncodeToString(hash[:])
		}
		writeGolden(t, g)
	}

	for _, v := range g.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			hash, canonical, err := QueryHash(v.Query, v.Variables)
			if err != nil {
				t.Fatalf("QueryHash: %v", err)
			}
			if string(canonical) != v.Canonical {
				t.Errorf("canonical mismatch\n got: %s\nwant: %s", canonical, v.Canonical)
			}
			if got := "0x" + hex.EncodeToString(hash[:]); got != v.Hash {
				t.Errorf("hash mismatch\n got: %s\nwant: %s", got, v.Hash)
			}
		})
	}
}

// TestQueryHashRejects checks the object-shape and >= 2^53 guards fire on inputs
// that must never produce a hash.
func TestQueryHashRejects(t *testing.T) {
	g := loadGolden(t)
	if len(g.Rejects) == 0 {
		t.Fatal("golden file has no reject cases")
	}
	for _, r := range g.Rejects {
		t.Run(r.Name, func(t *testing.T) {
			if _, _, err := QueryHash(r.Query, r.Variables); err == nil {
				t.Fatalf("expected an error, got nil")
			}
		})
	}
}

// TestAbsentVariablesEqualsEmpty pins that an omitted variables block and an
// explicit empty one are the same request: nil, empty, whitespace, and JSON null
// all hash like {}.
func TestAbsentVariablesEqualsEmpty(t *testing.T) {
	const query = "query Q { field }"
	want, _, err := QueryHash(query, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("baseline {}: %v", err)
	}
	for _, vars := range []json.RawMessage{nil, []byte(``), []byte(`  `), []byte(`null`)} {
		got, _, err := QueryHash(query, vars)
		if err != nil {
			t.Fatalf("variables %q: %v", string(vars), err)
		}
		if got != want {
			t.Errorf("variables %q hashed differently from {}", string(vars))
		}
	}
}

// TestObjectKeyOrderIgnored_ArrayOrderKept pins the JCS ordering rules the hash
// relies on: object keys are sorted (reordering them is the same request) while
// array element order is significant (reordering it is a different request).
func TestObjectKeyOrderIgnored_ArrayOrderKept(t *testing.T) {
	keysA, _, err := QueryHash("q", json.RawMessage(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	keysB, _, err := QueryHash("q", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if keysA != keysB {
		t.Error("object key order changed the hash; JCS must sort keys")
	}

	arrA, _, err := QueryHash("q", json.RawMessage(`{"ids":[1,2]}`))
	if err != nil {
		t.Fatal(err)
	}
	arrB, _, err := QueryHash("q", json.RawMessage(`{"ids":[2,1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if arrA == arrB {
		t.Error("array element order did not change the hash; arrays must stay ordered")
	}
}

// TestUnicodeEscapeEqualsUtf8 pins that a unicode escape and the raw UTF-8 bytes
// for the same character hash identically, because JCS decodes the escape before
// canonicalizing.
func TestUnicodeEscapeEqualsUtf8(t *testing.T) {
	utf8Hash, _, err := QueryHash("q", []byte(`{"s":"café"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 0x5c is the backslash byte; this forms the JSON escape for é, which JCS
	// must decode to the same bytes as the raw form above.
	escaped := `{"s":"caf` + string(rune(0x5c)) + `u00e9"}`
	escapedHash, _, err := QueryHash("q", []byte(escaped))
	if err != nil {
		t.Fatal(err)
	}
	if utf8Hash != escapedHash {
		t.Error("unicode escape and raw bytes for é must hash the same; JCS decodes escapes")
	}
}

func writeGolden(t *testing.T, g *golden) {
	t.Helper()
	b, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		t.Fatalf("marshal golden: %v", err)
	}
	if err := os.WriteFile(goldenPath, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
}
