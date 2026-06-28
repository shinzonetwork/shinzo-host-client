// Cross-language gate for the EIP-712 QueryRequest signature.
//
// The browser client signs QueryRequest with viem. This script signs the golden
// inputs with viem and, when eip712.json already carries a signature, asserts it
// matches and recovers to the expected address. A green Go `go test ./pkg/billing`
// plus a green run here is the guarantee that a viem signature verifies in Go.
//
//	npm i viem
//	node sign.mjs            # or: node sign.mjs /path/to/eip712.json

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { privateKeyToAccount } from 'viem/accounts';
import { recoverTypedDataAddress } from 'viem';

const here = dirname(fileURLToPath(import.meta.url));
const goldenPath = process.argv[2] ?? join(here, 'eip712.json');
const g = JSON.parse(readFileSync(goldenPath, 'utf8'));

const typedData = {
  domain: { name: g.domain.name, version: g.domain.version, chainId: g.domain.chainId },
  types: {
    QueryRequest: [
      { name: 'queryHash', type: 'bytes32' },
      { name: 'nonce', type: 'bytes32' },
      { name: 'timestamp', type: 'uint256' },
    ],
  },
  primaryType: 'QueryRequest',
  message: {
    queryHash: g.request.queryHash,
    nonce: g.request.nonce,
    timestamp: BigInt(g.request.timestamp),
  },
};

const account = privateKeyToAccount(g.privateKey);
const signature = await account.signTypedData(typedData);
const recovered = await recoverTypedDataAddress({ ...typedData, signature });

console.log('address:   ' + account.address);
console.log('signature: ' + signature);
console.log('recovered: ' + recovered);

let failures = 0;
if (recovered.toLowerCase() !== account.address.toLowerCase()) {
  console.error('FAIL: viem did not self-recover');
  failures++;
}
if (g.address && g.address.toLowerCase() !== account.address.toLowerCase()) {
  console.error(`FAIL: address ${account.address} != golden ${g.address}`);
  failures++;
}
if (g.signature) {
  if (g.signature.toLowerCase() !== signature.toLowerCase()) {
    console.error(`FAIL: signature\n  viem:   ${signature}\n  golden: ${g.signature}`);
    failures++;
  } else {
    console.log('OK: viem signature matches the golden');
  }
}
process.exit(failures ? 1 : 0);
