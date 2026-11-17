/**
 * U087 — web-utils.ts affordance + link-preservation parity gaps.
 *
 * Covers the confirmed gaps for core/core/src/tools/web-utils.ts:
 *  - SSRF block messages carry an allowed-alternative affordance (the loopback
 *    "allowed for local dev" clause) so the error is self-correcting — parity
 *    with Claude Code's ssrfError (ssrfGuard.ts:L287). Applied to the private-
 *    hostname, private-IP, and redirect block messages.
 *  - Redirect-limit error names the ceiling: "Too many redirects (exceeded N
 *    hops)" instead of a bare "Too many redirects" — parity with Claude's
 *    `Too many redirects (exceeded ${MAX_REDIRECTS})` (WebFetchTool/utils.ts:269).
 *  - stripHtml preserves anchor link targets as Markdown [text](href) before the
 *    catch-all tag strip, instead of discarding every <a href> URL — parity with
 *    Claude's turndown HTML→Markdown conversion (WebFetchTool/utils.ts:456-458).
 *
 * RED on the pre-fix implementation: the messages had no affordance, the redirect
 * error was the bare string, and stripHtml dropped every href.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'node:http';
import {
  stripHtml,
  validateUrlSafety,
  httpGet,
  isTransientHttpError,
  SSRF_LOOPBACK_AFFORDANCE,
  _webHooks,
  type HostLookupAll,
} from '../src/tools/web-utils';

// A dns.lookup stand-in that resolves any hostname to the given address(es).
function lookupTo(...addrs: string[]): HostLookupAll {
  return ((_h, _o, cb) => cb(null, addrs.map((address) => ({ address, family: address.includes(':') ? 6 : 4 })))) as HostLookupAll;
}

describe('U087 — SSRF block messages carry the loopback affordance', () => {
  it('appends the affordance to the private-metadata-hostname block', async () => {
    const r = await validateUrlSafety('http://metadata.google.internal/');
    assert.ok('error' in r);
    assert.match(r.error, /is a private\/metadata hostname/);
    assert.ok(r.error.includes(SSRF_LOOPBACK_AFFORDANCE), 'affordance must travel with the error');
  });

  it('appends the affordance to the resolves-to-private-IP block', async () => {
    const r = await validateUrlSafety('http://sneaky.example/', lookupTo('10.1.2.3'));
    assert.ok('error' in r);
    assert.match(r.error, /resolves to private IP 10\.1\.2\.3/);
    assert.ok(r.error.includes(SSRF_LOOPBACK_AFFORDANCE), 'affordance must travel with the error');
  });

  it('exposes a single canonical affordance string', () => {
    assert.equal(SSRF_LOOPBACK_AFFORDANCE, 'Loopback (127.0.0.1, ::1) is allowed for local dev.');
  });
});

describe('U087 — stripHtml preserves anchor link targets as Markdown', () => {
  it('converts <a href> to [text](href) instead of dropping the URL', () => {
    const out = stripHtml('<p>See <a href="https://example.com/doc">the doc</a> now.</p>');
    assert.ok(out.includes('[the doc](https://example.com/doc)'), out);
    assert.ok(!out.includes('<'));
  });

  it('handles single-quoted and unquoted href values', () => {
    assert.ok(stripHtml(`<a href='https://a.test/x'>A</a>`).includes('[A](https://a.test/x)'));
    assert.ok(stripHtml(`<a href=https://b.test/y>B</a>`).includes('[B](https://b.test/y)'));
  });

  it('strips inner tags inside the anchor text but keeps the link', () => {
    const out = stripHtml('<a href="https://c.test/z"><b>bold</b> text</a>');
    assert.ok(out.includes('[bold text](https://c.test/z)'), out);
    assert.ok(!out.includes('<b>'));
  });

  it('still renders anchor text for an empty href (bare un-linked text)', () => {
    const out = stripHtml('<a href="">click</a>');
    assert.ok(out.includes('[click]()'), out);
  });

  it('nested anchor still yields link text with no tags (regression lock)', () => {
    const out = stripHtml('<div><span><a href="#">link</a></span></div>');
    assert.ok(out.includes('link'));
    assert.ok(!out.includes('<'));
  });
});

describe('U087 — redirect-limit error names the hop ceiling', () => {
  let server: http.Server;
  let port: number;
  // A public-looking host whose connection is pinned to the loopback server, so a
  // self-redirect loop is actually FOLLOWED (a loopback-IP redirect target would
  // be refused by isBlockedRedirectTarget before the hop-limit is ever reached).
  let loopUrl: string;
  const realValidate = _webHooks.validateRedirectSafety;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.writeHead(302, { location: loopUrl });
      res.end();
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
    loopUrl = `http://public.example:${port}/loop`;
    // Every redirect hop re-validates as safe, pinned to the loopback server.
    _webHooks.validateRedirectSafety = async () => ({ resolvedIP: '127.0.0.1' });
  });
  after(async () => {
    _webHooks.validateRedirectSafety = realValidate;
    (server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
    await new Promise<void>((r) => server.close(() => r()));
  });

  it('reports the configured ceiling in the error message', async () => {
    await assert.rejects(
      httpGet(loopUrl, new AbortController().signal, { resolvedIP: '127.0.0.1', maxRedirects: 2 }),
      /Too many redirects \(exceeded 2 hops\)/,
    );
  });

  it('reports the default ceiling when maxRedirects is not overridden', async () => {
    await assert.rejects(
      httpGet(loopUrl, new AbortController().signal, { resolvedIP: '127.0.0.1' }),
      /Too many redirects \(exceeded 5 hops\)/,
    );
  });

  it('classifies the ceiling error as non-transient (never retried)', () => {
    assert.equal(isTransientHttpError(new Error('Too many redirects (exceeded 5 hops)')), false);
    // The legacy bare message is still classified non-transient (prefix match).
    assert.equal(isTransientHttpError(new Error('Too many redirects')), false);
  });
});
