/**
 * U087 — web-utils SSRF + userinfo hardening (the reference-parity).
 *
 * Covers:
 *  - IPv4-mapped IPv6 decoding in hex / compressed / expanded form so a mapped
 *    metadata address (e.g. ::ffff:a9fe:a9fe = 169.254.169.254) is recognized as
 *    private (ssrfGuard.ts parity).
 *  - Rejecting URLs that carry embedded credentials (userinfo) in the SSRF
 *    validator and the redirect guard (WebFetchTool/utils.ts parity).
 *  - An end-to-end redirect-follow path (through httpGet) that must refuse to
 *    follow a credential-bearing redirect target.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type * as http from 'node:http';
import type * as https from 'node:https';
import type * as dns from 'node:dns';
import {
  isPrivateIP,
  isBlockedRedirectTarget,
  validateUrlSafety,
  urlHasUserinfo,
  httpGet,
  _webHooks,
  type HostLookupAll,
} from '../src/tools/web-utils';

// ---------------------------------------------------------------------------
// isPrivateIP — IPv4-mapped IPv6 in every representation
// ---------------------------------------------------------------------------

describe('isPrivateIP — IPv4-mapped IPv6 hex/compressed/expanded', () => {
  // Hex-compressed form of 169.254.169.254 (AWS/GCP metadata). Pre-fix this was
  // treated as PUBLIC — the classic SSRF escape the guard must close.
  it('::ffff:a9fe:a9fe (hex metadata) is private', () =>
    assert.ok(isPrivateIP('::ffff:a9fe:a9fe')));
  it('expanded 0:0:0:0:0:ffff:a9fe:a9fe is private', () =>
    assert.ok(isPrivateIP('0:0:0:0:0:ffff:a9fe:a9fe')));
  it('::ffff:7f00:1 (hex 127.0.0.1) is private', () =>
    assert.ok(isPrivateIP('::ffff:7f00:1')));
  it('hex mapped 10.x (::ffff:0a00:1) is private', () =>
    assert.ok(isPrivateIP('::ffff:0a00:1')));
  it('uppercase hex mapped (::FFFF:A9FE:A9FE) is private', () =>
    assert.ok(isPrivateIP('::FFFF:A9FE:A9FE')));
  // Dotted mapped form must keep working (regression lock).
  it('::ffff:169.254.169.254 (dotted) is private', () =>
    assert.ok(isPrivateIP('::ffff:169.254.169.254')));

  // Public addresses must stay public.
  it('::ffff:8.8.8.8 (public mapped) is public', () =>
    assert.ok(!isPrivateIP('::ffff:8.8.8.8')));
  it('::ffff:0808:0808 (public mapped hex) is public', () =>
    assert.ok(!isPrivateIP('::ffff:0808:0808')));
  it('2606:2800:220:1:248:1893:25c8:1946 (public v6) is public', () =>
    assert.ok(!isPrivateIP('2606:2800:220:1:248:1893:25c8:1946')));
  it('non-mapped v6 falls through to prefix checks (fe80::1 private)', () =>
    assert.ok(isPrivateIP('fe80::1')));
  it('malformed v6 is not mis-decoded (:::: is not private via mapping)', () =>
    assert.ok(!isPrivateIP('gg::ffff:1')));
});

// ---------------------------------------------------------------------------
// urlHasUserinfo + guards rejecting embedded credentials
// ---------------------------------------------------------------------------

describe('urlHasUserinfo', () => {
  it('true for user:pass@', () =>
    assert.ok(urlHasUserinfo(new URL('http://user:pass@example.com/'))));
  it('true for bare user@', () =>
    assert.ok(urlHasUserinfo(new URL('http://user@example.com/'))));
  it('false for a clean URL', () =>
    assert.ok(!urlHasUserinfo(new URL('http://example.com/'))));
});

describe('isBlockedRedirectTarget — embedded credentials', () => {
  it('blocks a credential-bearing redirect to a PUBLIC host', () =>
    assert.ok(isBlockedRedirectTarget('http://user:pass@public.example/path')));
  it('blocks a bare-username redirect to a PUBLIC host', () =>
    assert.ok(isBlockedRedirectTarget('https://admin@public.example/')));
  it('still allows a clean public redirect target', () =>
    assert.ok(!isBlockedRedirectTarget('https://public.example/path')));
});

describe('validateUrlSafety — embedded credentials', () => {
  // A lookup that would resolve the host to a PUBLIC address: proves the
  // userinfo rejection happens on the URL shape, BEFORE/independent of DNS.
  const publicLookup: HostLookupAll = (_h, _o, cb) =>
    cb(null, [{ address: '93.184.216.34', family: 4 } as dns.LookupAddress]);

  it('rejects user:pass@ even when the host resolves public', async () => {
    const r = await validateUrlSafety('http://user:pass@public.example/', publicLookup);
    assert.ok('error' in r);
    assert.match(r.error, /embedded credentials/);
  });
  it('rejects bare user@', async () => {
    const r = await validateUrlSafety('https://alice@public.example/', publicLookup);
    assert.ok('error' in r);
  });
  it('allows a clean public URL (regression lock)', async () => {
    const r = await validateUrlSafety('https://public.example/', publicLookup);
    assert.ok('resolvedIP' in r);
    assert.equal(r.resolvedIP, '93.184.216.34');
  });
});

// ---------------------------------------------------------------------------
// Integration: a hostname that RESOLVES to a hex-mapped metadata address is
// blocked by validateUrlSafety (isPrivateIP is wired into the DNS check).
// ---------------------------------------------------------------------------

describe('validateUrlSafety — DNS resolves to a hex-mapped metadata IP', () => {
  it('blocks a host resolving to ::ffff:a9fe:a9fe', async () => {
    const lookup: HostLookupAll = (_h, _o, cb) =>
      cb(null, [{ address: '::ffff:a9fe:a9fe', family: 6 } as dns.LookupAddress]);
    const r = await validateUrlSafety('http://rebind.example/', lookup);
    assert.ok('error' in r);
    assert.match(r.error, /private IP/);
  });
});

// ---------------------------------------------------------------------------
// E2E red-green: httpGet must NOT follow a credential-bearing redirect.
// Driven entirely through the real httpGet redirect loop via the rawGet seam
// (no network). Pre-fix the redirect was followed and the body 'LEAKED' was
// returned; post-fix the follow is refused at the redirect guard.
// ---------------------------------------------------------------------------

describe('httpGet — refuses to follow a credential-bearing redirect (e2e)', () => {
  const origRawGet = _webHooks.rawGet;
  const origValidate = _webHooks.validateRedirectSafety;

  before(() => {
    // Make the redirect target "SSRF-safe" so, PRE-FIX, nothing else would stop
    // the follow — isolating the credential check as the sole discriminator.
    _webHooks.validateRedirectSafety = async () => ({ resolvedIP: '93.184.216.34' });
    _webHooks.rawGet = ((
      _requester: typeof http | typeof https,
      url: string,
      _options: http.RequestOptions,
      callback: (res: http.IncomingMessage) => void,
    ) => {
      const parsed = new URL(url);
      const res = new EventEmitter() as unknown as http.IncomingMessage & {
        resume: () => void;
      };
      res.resume = () => {};
      if (!parsed.username && !parsed.password) {
        // First hop: 302 to a credential-bearing PUBLIC target.
        res.statusCode = 302;
        res.headers = { location: 'http://evil:s3cret@public.example/secret' };
        process.nextTick(() => callback(res));
      } else {
        // Redirect target (only reached if the follow is wrongly permitted).
        res.statusCode = 200;
        res.headers = { 'content-type': 'text/plain' };
        process.nextTick(() => {
          callback(res);
          process.nextTick(() => {
            res.emit('data', Buffer.from('LEAKED'));
            res.emit('end');
          });
        });
      }
      const req = new EventEmitter() as unknown as http.ClientRequest;
      (req as unknown as { destroy: () => void }).destroy = () => {};
      return req;
    }) as typeof _webHooks.rawGet;
  });

  after(() => {
    _webHooks.rawGet = origRawGet;
    _webHooks.validateRedirectSafety = origValidate;
  });

  it('rejects instead of returning the redirect-target body', async () => {
    const ac = new AbortController();
    await assert.rejects(
      httpGet('http://start.example/', ac.signal, { maxRedirects: 3 }),
      /private\/internal/,
    );
  });
});
