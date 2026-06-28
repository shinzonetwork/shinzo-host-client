// Cross-language gate for the query_hash golden vectors.
//
// The browser client computes query_hash with canonicalize (RFC 8785 JCS) +
// js-sha3 keccak256. This script reproduces every vector in canonical.json that
// way and fails if any canonical string or hash diverges from the Go output. A
// green Go `go test ./pkg/canonical` plus a green run here is the guarantee that
// the Go host/app and the JS client hash identically.
//
//	npm i canonicalize@3.0.0 js-sha3
//	node verify.mjs            # or: node verify.mjs /path/to/canonical.json

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import canonicalizePkg from 'canonicalize';
import sha3 from 'js-sha3';

const canonicalize = canonicalizePkg.default ?? canonicalizePkg;
const { keccak256 } = sha3;

const here = dirname(fileURLToPath(import.meta.url));
const goldenPath = process.argv[2] ?? join(here, 'canonical.json');
const golden = JSON.parse(readFileSync(goldenPath, 'utf8'));

let failures = 0;
for (const v of golden.vectors) {
  const canonical = canonicalize({ query: v.query, variables: v.variables });
  const hash = '0x' + keccak256(canonical);
  if (canonical !== v.canonical) {
    console.error(`FAIL ${v.name}: canonical\n  got:  ${canonical}\n  want: ${v.canonical}`);
    failures++;
  } else if (hash !== v.hash) {
    console.error(`FAIL ${v.name}: hash\n  got:  ${hash}\n  want: ${v.hash}`);
    failures++;
  }
}

if (failures > 0) {
  console.error(`\n${failures} vector(s) diverged between Go and JS`);
  process.exit(1);
}
console.log(`OK: all ${golden.vectors.length} vectors match the Go golden (canonicalize + js-sha3)`);
