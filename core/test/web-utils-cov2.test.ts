/**
 * Accurate-coverage companion for web-utils.ts.
 *
 * The ESM coverage harness (scripts/coverage-esm.mjs) excludes web-tools.test.ts
 * and web-fetch-security.test.ts because they are coupled to the on-disk layout /
 * go through the web-fetch tool. The branches they exercise in web-utils.ts are
 * therefore unmeasured. This file re-covers those branches DIRECTLY against the
 * web-utils exports in an ESM-safe way:
 *   - no source-text assertions (behaviour only)
 *   - no namespace monkeypatching (real http server + IP-literal DNS short-circuit)
 *   - no __dirname/import.meta.url fixtures
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import {
  httpGet,
  validateUrlSafety,
  isValidIP,
  pinnedDnsLookup,
  isMetadataEndpoint,
  getUserAgent,
  DEFAULT_WEB_USER_AGENT,
  BLOCKED_CONTENT_TYPES,
  MAX_RESPONSE_BYTES,
  MAX_HTTP_BODY_BYTES,
} from '../src/tools/web-utils';

// ---------------------------------------------------------------------------
// isValidIP — every branch
// ---------------------------------------------------------------------------
describe('isValidIP (cov2)', () => {
  it('rejects empty / sentinel strings', () => {
    assert.equal(isValidIP(''), false);
    assert.equal(isValidIP('undefined'), false);
    assert.equal(isValidIP('null'), false);
  });
  it('accepts a dotted-quad IPv4', () => {
    assert.equal(isValidIP('93.184.216.34'), true);
  });
  it('accepts an IPv6 address (colon form)', () => {
    assert.equal(isValidIP('2001:db8::1'), true);
    assert.equal(isValidIP('::1'), true);
  });
  it('rejects a bare hostname', () => {
    assert.equal(isValidIP('example.com'), false);
    assert.equal(isValidIP('not-an-ip'), false);
  });
});

// ---------------------------------------------------------------------------
// isMetadataEndpoint — every branch
// ---------------------------------------------------------------------------
describe('isMetadataEndpoint (cov2)', () => {
  it('flags the AWS IMDS v4 address (incl. with a port)', () => {
    assert.equal(isMetadataEndpoint('http://169.254.169.254/latest/meta-data/'), true);
    assert.equal(isMetadataEndpoint('http://169.254.169.254:80/latest/'), true);
  });
  it('flags the GCP metadata hostname', () => {
    assert.equal(isMetadataEndpoint('http://metadata.google.internal/computeMetadata/v1/'), true);
  });
  it('flags the ECS task-metadata address', () => {
    assert.equal(isMetadataEndpoint('http://169.254.170.2/v2/metadata'), true);
  });
  it('flags the IMDSv2 IPv6 address with brackets stripped', () => {
    assert.equal(isMetadataEndpoint('http://[fd00:ec2::254]/latest/'), true);
  });
  it('does not flag an ordinary public host', () => {
    assert.equal(isMetadataEndpoint('https://example.com/'), false);
  });
  it('does not flag a near-miss link-local address', () => {
    assert.equal(isMetadataEndpoint('http://169.254.169.253/test'), false);
  });
  it('returns false (not throw) for an unparseable URL', () => {
    assert.equal(isMetadataEndpoint('not a url'), false);
    assert.equal(isMetadataEndpoint(''), false);
  });
});

// ---------------------------------------------------------------------------
// getUserAgent — default + override branches
// ---------------------------------------------------------------------------
describe('getUserAgent (cov2)', () => {
  it('returns the default UA when the env override is unset', () => {
    const prev = process.env.SUPERINFERENCE_WEB_USER_AGENT;
    delete process.env.SUPERINFERENCE_WEB_USER_AGENT;
    try {
      assert.equal(getUserAgent(), DEFAULT_WEB_USER_AGENT);
    } finally {
      if (prev !== undefined) process.env.SUPERINFERENCE_WEB_USER_AGENT = prev;
    }
  });
  it('returns the env override when set', () => {
    const prev = process.env.SUPERINFERENCE_WEB_USER_AGENT;
    process.env.SUPERINFERENCE_WEB_USER_AGENT = 'Cov2-UA/1.0';
    try {
      assert.equal(getUserAgent(), 'Cov2-UA/1.0');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_WEB_USER_AGENT;
      else process.env.SUPERINFERENCE_WEB_USER_AGENT = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// content-type / size constants — guardrail values (regression anchors)
// ---------------------------------------------------------------------------
describe('web-utils constants (cov2)', () => {
  it('blocks executable/opaque binary content types but not json/html', () => {
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/octet-stream'));
    assert.ok(BLOCKED_CONTENT_TYPES.has('application/vnd.microsoft.portable-executable'));
    assert.equal(BLOCKED_CONTENT_TYPES.has('application/json'), false);
    assert.equal(BLOCKED_CONTENT_TYPES.has('text/html'), false);
  });
  it('keeps the pre-flight content-length ceiling at 50MB', () => {
    assert.equal(MAX_RESPONSE_BYTES, 50 * 1024 * 1024);
  });
});

// ---------------------------------------------------------------------------
// pinnedDnsLookup — all calling conventions and families
// ---------------------------------------------------------------------------
describe('pinnedDnsLookup (cov2)', () => {
  it('handles the {all:true} convention Node uses (array of addresses)', () => {
    const lookup = pinnedDnsLookup('93.184.216.34');
    let got: unknown;
    (lookup as (h: string, o: unknown, c: (e: unknown, a: unknown) => void) => void)(
      'anything.example', { all: true }, (_e, addrs) => { got = addrs; },
    );
    assert.deepEqual(got, [{ address: '93.184.216.34', family: 4 }]);
  });

  it('handles the (hostname, options-object, callback) non-all convention', () => {
    const lookup = pinnedDnsLookup('93.184.216.34');
    let addr: unknown; let fam: unknown;
    (lookup as (h: string, o: unknown, c: (e: unknown, a: unknown, f?: number) => void) => void)(
      'anything.example', {}, (_e, a, f) => { addr = a; fam = f; },
    );
    assert.equal(addr, '93.184.216.34');
    assert.equal(fam, 4);
  });

  it('handles the (hostname, callback) convention where options is the callback', () => {
    const lookup = pinnedDnsLookup('93.184.216.34');
    let addr: unknown;
    (lookup as (h: string, c: (e: unknown, a: unknown, f?: number) => void) => void)(
      'anything.example', (_e, a) => { addr = a; },
    );
    assert.equal(addr, '93.184.216.34');
  });

  it('reports family 6 for an IPv6 pin', () => {
    const lookup = pinnedDnsLookup('2001:db8::1');
    let fam: unknown;
    (lookup as (h: string, o: unknown, c: (e: unknown, a: unknown) => void) => void)(
      'anything.example', { all: true }, (_e, addrs) => {
        fam = (addrs as Array<{ family: number }>)[0].family;
      },
    );
    assert.equal(fam, 6);
  });
});

// ---------------------------------------------------------------------------
// validateUrlSafety — private-IP-resolution and resolvedIP branches,
// covered offline by passing IP literals (dns.lookup short-circuits numeric
// hosts without a network round-trip).
// ---------------------------------------------------------------------------
describe('validateUrlSafety (cov2)', () => {
  it('blocks when the host resolves to a private IP', async () => {
    const result = await validateUrlSafety('http://10.0.0.1/path');
    assert.ok('error' in result);
    assert.match(result.error, /private IP 10\.0\.0\.1/);
  });

  it('returns the resolved IP for a public literal host', async () => {
    const result = await validateUrlSafety('http://93.184.216.34/path');
    assert.ok('resolvedIP' in result);
    assert.equal(result.resolvedIP, '93.184.216.34');
  });

  it('blocks a known metadata hostname before any DNS lookup', async () => {
    const result = await validateUrlSafety('http://metadata.google.internal/');
    assert.ok('error' in result);
    assert.match(result.error, /private\/metadata hostname/);
  });

  it('blocks when DNS resolution fails for a bogus hostname', async () => {
    const result = await validateUrlSafety('http://nonexistent-zzz-si-cov2.invalid/');
    assert.ok('error' in result);
    assert.match(result.error, /DNS resolution failed/);
  });
});

// ---------------------------------------------------------------------------
// httpGet — redirect handling, truncation, timeout, abort, transport errors.
// All served from a loopback server; the initial request pins resolvedIP to
// 127.0.0.1 so no DNS/network egress is needed.
// ---------------------------------------------------------------------------
describe('httpGet transport (cov2)', () => {
  let server: http.Server;
  let base: string;
  let deadPort: number;

  before(async () => {
    server = http.createServer((req, res) => {
      switch (req.url) {
        case '/ok':
          res.writeHead(200, { 'Content-Type': 'text/plain' });
          res.end('hello cov2');
          return;
        case '/big': {
          // Comfortably over MAX_HTTP_BODY_BYTES so the truncation path fires.
          res.writeHead(200, { 'Content-Type': 'text/plain' });
          const chunk = Buffer.alloc(256 * 1024, 0x61);
          for (let i = 0; i < 6; i++) res.write(chunk); // 1.5 MB
          res.end();
          return;
        }
        case '/redirect-private':
          res.writeHead(302, { Location: '/ok' }); // resolves back to loopback
          res.end();
          return;
        case '/redirect-bogus':
          res.writeHead(302, { Location: 'http://nonexistent-zzz-si-cov2.invalid/x' });
          res.end();
          return;
        case '/redirect-loop':
          res.writeHead(301, { Location: 'http://93.184.216.34/next' });
          res.end();
          return;
        case '/redirect-noloc':
          // 3xx status but no Location header: must be treated as a body, not a redirect.
          res.writeHead(302);
          res.end('no location here');
          return;
        case '/nohdr':
          // No Content-Type header: exercises the `content-type ?? ''` fallback.
          res.writeHead(200);
          res.end('bare body');
          return;
        case '/slow':
          // Never respond: forces the client request timeout.
          return;
        case '/reset':
          // Headers promise more than we send, then we kill the socket so the
          // response stream emits an error mid-body.
          res.writeHead(200, { 'Content-Type': 'text/plain', 'Content-Length': '1000' });
          res.write('partial');
          res.socket?.destroy();
          return;
        default:
          res.writeHead(404);
          res.end('nope');
      }
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;

    // Reserve, then release, a port so we have one guaranteed-refused address.
    const tmp = http.createServer();
    await new Promise<void>((r) => tmp.listen(0, '127.0.0.1', () => r()));
    deadPort = (tmp.address() as { port: number }).port;
    await new Promise<void>((r) => tmp.close(() => r()));
  });

  after(async () => {
    (server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
    await new Promise<void>((r) => server.close(() => r()));
  });

  const pin = { resolvedIP: '127.0.0.1' };

  it('fetches a normal body with the resolvedIP pin', async () => {
    const res = await httpGet(`${base}/ok`, new AbortController().signal, pin);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body, 'hello cov2');
    assert.equal(res.truncated, undefined);
  });

  it('truncates a body larger than MAX_HTTP_BODY_BYTES', async () => {
    const res = await httpGet(`${base}/big`, new AbortController().signal, pin);
    assert.equal(res.statusCode, 200);
    assert.equal(res.truncated, true);
    assert.equal(Buffer.byteLength(res.body), MAX_HTTP_BODY_BYTES);
  });

  it('truncation also caps the raw bodyBuffer', async () => {
    const res = await httpGet(`${base}/big`, new AbortController().signal, { ...pin, raw: true });
    assert.equal(res.truncated, true);
    assert.ok(res.bodyBuffer);
    assert.equal(res.bodyBuffer!.length, MAX_HTTP_BODY_BYTES);
  });

  it('blocks a redirect whose target is a private/internal URL', async () => {
    await assert.rejects(
      httpGet(`${base}/redirect-private`, new AbortController().signal, pin),
      /Blocked: redirect to private\/internal URL/,
    );
  });

  it('rejects a redirect whose target fails SSRF DNS validation', async () => {
    await assert.rejects(
      httpGet(`${base}/redirect-bogus`, new AbortController().signal, pin),
      /DNS resolution failed/,
    );
  });

  it('rejects when the redirect budget is exhausted (maxRedirects: 0)', async () => {
    await assert.rejects(
      httpGet(`${base}/redirect-loop`, new AbortController().signal, { ...pin, maxRedirects: 0 }),
      /Too many redirects/,
    );
  });

  it('rejects on request timeout', async () => {
    await assert.rejects(
      httpGet(`${base}/slow`, new AbortController().signal, { ...pin, timeoutMs: 120 }),
      /Request timed out after 120ms/,
    );
  });

  it('rejects when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(
      httpGet(`${base}/ok`, ac.signal, pin),
      /Aborted/,
    );
  });

  it('rejects when aborted mid-flight', async () => {
    const ac = new AbortController();
    const p = httpGet(`${base}/slow`, ac.signal, pin);
    setTimeout(() => ac.abort(), 30);
    await assert.rejects(p, /Aborted/);
  });

  it('rejects on a transport error (connection refused)', async () => {
    await assert.rejects(
      httpGet(`http://127.0.0.1:${deadPort}/ok`, new AbortController().signal, { resolvedIP: '127.0.0.1' }),
      (err: Error) => err instanceof Error && /ECONNREFUSED/.test(err.message),
    );
  });

  it('rejects when the response stream errors mid-body', async () => {
    await assert.rejects(
      httpGet(`${base}/reset`, new AbortController().signal, pin),
      (err: Error) => err instanceof Error,
    );
  });

  it('treats a 3xx with no Location header as a normal body', async () => {
    const res = await httpGet(`${base}/redirect-noloc`, new AbortController().signal, pin);
    assert.equal(res.statusCode, 302);
    assert.equal(res.body, 'no location here');
  });

  it('defaults contentType to empty string when the header is absent', async () => {
    const res = await httpGet(`${base}/nohdr`, new AbortController().signal, pin);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body, 'bare body');
    assert.equal(res.contentType, '');
  });

  it('rejects an unparseable URL before making a request', async () => {
    await assert.rejects(
      httpGet('http://[::bad url', new AbortController().signal),
      /Invalid URL/,
    );
  });
});
