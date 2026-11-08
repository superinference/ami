/**
 * cov100 companion for web-utils.ts.
 *
 * Targets the one residual uncovered branch in httpGetInternal:
 *
 *   const pinIP = resolvedIP && isValidIP(resolvedIP) ? resolvedIP : undefined;
 *
 * Every pre-existing httpGet test passes either NO resolvedIP (falsy, so the
 * `&&` short-circuits on the left) or a VALID IP literal ('127.0.0.1', …, so
 * isValidIP returns true). The arm where `resolvedIP` is truthy but
 * `isValidIP(resolvedIP)` returns FALSE — a caller handing in a bogus pin —
 * was never exercised.
 *
 * Behaviour under test: a truthy-but-invalid resolvedIP must be IGNORED for DNS
 * pinning (pinIP stays undefined, no `lookup` override installed), and the
 * request must otherwise proceed normally and return the real body. In other
 * words a malformed pin degrades safely to a normal lookup rather than breaking
 * the fetch (which is what would happen if the bad string were forwarded into
 * pinnedDnsLookup / http's lookup option).
 *
 * ESM-safe: behaviour-only assertions, a real loopback http server on port 0,
 * no source-text reads, no namespace monkeypatching, no __dirname coupling.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import { httpGet } from '../src/tools/web-utils';

describe('httpGet — truthy-but-invalid resolvedIP is ignored (cov100)', () => {
  let server: http.Server;
  let base: string;

  before(async () => {
    server = http.createServer((req, res) => {
      if (req.url === '/ok') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('body-via-normal-lookup');
        return;
      }
      res.writeHead(404);
      res.end('nope');
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  });

  after(async () => {
    (server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
    await new Promise<void>((r) => server.close(() => r()));
  });

  it('fetches normally when resolvedIP is a non-IP string (pin skipped)', async () => {
    // 'not-an-ip' is truthy, so the left side of `resolvedIP && isValidIP(...)`
    // passes; isValidIP('not-an-ip') is false, so pinIP must resolve to
    // undefined and NO lookup override is installed. The URL host (127.0.0.1)
    // is then resolved by Node's own lookup and the fetch succeeds.
    const res = await httpGet(`${base}/ok`, new AbortController().signal, {
      resolvedIP: 'not-an-ip',
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body, 'body-via-normal-lookup');
    assert.equal(res.truncated, undefined);
  });

  it('ignores an empty-string resolvedIP (falsy left arm) and still fetches', async () => {
    // Companion case: an empty string is falsy, short-circuiting the `&&` on the
    // LEFT. Same observable outcome — a clean normal fetch — but distinct from
    // the invalid-but-truthy arm above.
    const res = await httpGet(`${base}/ok`, new AbortController().signal, {
      resolvedIP: '',
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body, 'body-via-normal-lookup');
  });
});
