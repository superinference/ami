/**
 * Parity hardening for web-utils: URL scheme allowlist, charset-aware body
 * decoding, numeric HTML-entity decoding, and bracketed-IPv6 / case-insensitive
 * SSRF coverage. Each block below is a red-green reproducer: the assertions
 * discriminate the hardened behavior from the pre-fix behavior (documented
 * inline), so reverting the corresponding change in web-utils.ts turns the test
 * red. Additive only — no previously-allowed http/https behavior changes.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'http';
import type * as dns from 'dns';
import {
  ALLOWED_URL_SCHEMES,
  isAllowedScheme,
  validateUrlSafety,
  isBlockedRedirectTarget,
  isPrivateIP,
  parseCharset,
  decodeBody,
  stripHtml,
  httpGet,
  type HostLookupAll,
} from '../src/tools/web-utils.ts';

function lookupReturning(addrs: { address: string; family?: number }[]): HostLookupAll {
  return ((_h: string, _o: unknown, cb: (e: NodeJS.ErrnoException | null, a: dns.LookupAddress[]) => void) => {
    cb(null, addrs.map(a => ({ address: a.address, family: a.family ?? 4 })) as dns.LookupAddress[]);
  }) as HostLookupAll;
}

// ---------------------------------------------------------------------------
// Gap A: URL scheme allowlist
// ---------------------------------------------------------------------------
describe('scheme allowlist', () => {
  it('ALLOWED_URL_SCHEMES is exactly http/https', () => {
    assert.deepEqual([...ALLOWED_URL_SCHEMES].sort(), ['http:', 'https:']);
  });

  it('isAllowedScheme accepts http/https, rejects others and junk', () => {
    assert.equal(isAllowedScheme('http://example.com/'), true);
    assert.equal(isAllowedScheme('https://example.com/'), true);
    assert.equal(isAllowedScheme('file:///etc/passwd'), false);
    assert.equal(isAllowedScheme('ftp://example.com/'), false);
    assert.equal(isAllowedScheme('gopher://example.com/'), false);
    assert.equal(isAllowedScheme('not a url'), false);
  });

  it('validateUrlSafety blocks a non-http scheme even when it resolves public', async () => {
    // RED on revert: without the scheme guard, gopher://example.com resolves
    // to a PUBLIC ip and returns { resolvedIP }, i.e. allowed. The guard makes
    // it an error without any DNS work.
    const res = await validateUrlSafety('gopher://example.com/', lookupReturning([{ address: '93.184.216.34' }]));
    assert.ok('error' in res, 'non-http scheme must be blocked');
    assert.match(res.error, /scheme/i);
  });

  it('validateUrlSafety still permits ordinary https', async () => {
    const res = await validateUrlSafety('https://example.com/', lookupReturning([{ address: '93.184.216.34' }]));
    assert.ok('resolvedIP' in res);
  });

  it('isBlockedRedirectTarget blocks redirects to non-http schemes', () => {
    // RED on revert: file:// / gopher:// previously returned false (allowed) —
    // the classic "redirect to file://" SSRF/LFI escape.
    assert.equal(isBlockedRedirectTarget('file:///etc/passwd'), true);
    assert.equal(isBlockedRedirectTarget('gopher://example.com/'), true);
    // A plain public https redirect must remain allowed.
    assert.equal(isBlockedRedirectTarget('https://example.com/next'), false);
  });
});

// ---------------------------------------------------------------------------
// Gap D: bracketed IPv6 + case-insensitive private-range detection
// ---------------------------------------------------------------------------
describe('IPv6 SSRF coverage', () => {
  it('isBlockedRedirectTarget blocks bracketed link-local / ULA IPv6', () => {
    // RED on revert: the old code only matched the "[::"-prefix forms, so
    // [fe80::1] / [fc00::1] / [fd12::1] returned false (allowed).
    assert.equal(isBlockedRedirectTarget('http://[fe80::1]/'), true);
    assert.equal(isBlockedRedirectTarget('http://[fc00::1]/'), true);
    assert.equal(isBlockedRedirectTarget('http://[fd12:3456::1]/'), true);
    assert.equal(isBlockedRedirectTarget('http://[FE80::1]/'), true); // uppercase
  });

  it('isBlockedRedirectTarget still allows public IPv6', () => {
    assert.equal(isBlockedRedirectTarget('http://[2606:2800:220:1::1]/'), false);
  });

  it('isPrivateIP is case-insensitive for IPv6 literals', () => {
    // RED on revert: without the toLowerCase normalization, the uppercase
    // forms fall through every (lowercase) hex-prefix check and return false.
    assert.equal(isPrivateIP('FE80::1'), true);
    assert.equal(isPrivateIP('FC00::1'), true);
    assert.equal(isPrivateIP('FD12::1'), true);
    assert.equal(isPrivateIP('::FFFF:127.0.0.1'), true);
    // sanity: public IPv6 still false
    assert.equal(isPrivateIP('2606:2800:220:1::1'), false);
  });
});

// ---------------------------------------------------------------------------
// Gap B: charset-aware body decoding
// ---------------------------------------------------------------------------
describe('charset decoding', () => {
  it('parseCharset extracts and normalizes the charset', () => {
    assert.equal(parseCharset('text/html; charset=ISO-8859-1'), 'iso-8859-1');
    assert.equal(parseCharset('text/html; charset="utf-8"'), 'utf-8');
    assert.equal(parseCharset('text/plain'), null);
    assert.equal(parseCharset(undefined), null);
  });

  it('decodeBody honors latin1 / utf-16le and falls back to utf-8', () => {
    // RED on revert: without decodeBody, the body was force-decoded utf-8 and
    // 0xE9 became the U+FFFD replacement char, not 'é'.
    assert.equal(decodeBody(Buffer.from([0xe9]), 'text/html; charset=iso-8859-1'), 'é');
    assert.equal(decodeBody(Buffer.from('e900', 'hex'), 'text/plain; charset=utf-16le'), 'é');
    // unknown label -> utf-8 fallback
    assert.equal(decodeBody(Buffer.from('hi', 'utf-8'), 'text/plain; charset=weird-enc'), 'hi');
    // no content-type -> utf-8 (unchanged legacy behavior)
    assert.equal(decodeBody(Buffer.from('plain', 'utf-8')), 'plain');
  });

  it('httpGet decodes a declared non-utf-8 charset (wiring)', async () => {
    // RED on revert of the finish() wiring: body would be utf-8-decoded and the
    // 0xE9 byte would become U+FFFD rather than 'é'.
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=iso-8859-1' });
      res.end(Buffer.from([0x63, 0x61, 0x66, 0xe9])); // "caf" + 0xE9
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    try {
      const port = (server.address() as { port: number }).port;
      const result = await httpGet(`http://127.0.0.1:${port}/`, new AbortController().signal, { resolvedIP: '127.0.0.1' });
      assert.equal(result.body, 'café');
    } finally {
      server.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Gap C: numeric HTML entity decoding
// ---------------------------------------------------------------------------
describe('stripHtml numeric entities', () => {
  it('decodes decimal and hex numeric character references', () => {
    // RED on revert: these survived as literal "&#…;" text.
    assert.equal(stripHtml('<p>&#8364;</p>'), '€');       // euro, decimal
    assert.equal(stripHtml('<p>a&#x2014;b</p>'), 'a—b');  // em dash, hex
    assert.equal(stripHtml('<p>&#x1F600;</p>'), '\u{1F600}'); // astral emoji
  });

  it('drops an out-of-range numeric reference instead of throwing', () => {
    assert.equal(stripHtml('<p>x&#1114112;y</p>'), 'xy'); // > 0x10FFFF
  });

  it('still decodes the pre-existing named entity set', () => {
    assert.equal(stripHtml('&amp; &lt; &gt; &quot; &#39; &nbsp;').replace(/\s+/g, ' ').trim(), "& < > \" '");
  });
});
