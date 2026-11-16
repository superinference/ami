/**
 * U087 — redirect protocol/port boundary guard (Claude-parity).
 *
 * Mirrors the protocol/port arms of Claude Code's isPermittedRedirect
 * (WebFetchTool/utils.ts): a redirect that changes protocol (esp. the
 * https→http TLS downgrade) or port must never be auto-followed, even when the
 * target host is PUBLIC. AMI deliberately still follows a same-protocol /
 * same-port cross-host redirect (differentiator) after SSRF re-validation.
 *
 * Covers:
 *  - redirectCrossesSecurityBoundary unit matrix (downgrade, upgrade, scheme
 *    switch, port change, default-vs-explicit port, unparseable, allowed cases).
 *  - E2E red-green through the real httpGet redirect loop (via the rawGet seam,
 *    no network): an https→http downgrade and a port change to a PUBLIC host are
 *    both refused; a same-protocol/same-port cross-host redirect still follows.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type * as http from 'node:http';
import type * as https from 'node:https';
import {
  redirectCrossesSecurityBoundary,
  httpGet,
  _webHooks,
} from '../src/tools/web-utils';

// ---------------------------------------------------------------------------
// Unit: redirectCrossesSecurityBoundary
// ---------------------------------------------------------------------------

describe('redirectCrossesSecurityBoundary', () => {
  it('blocks an https→http protocol downgrade (same host)', () => {
    assert.ok(redirectCrossesSecurityBoundary('https://ex.com/a', 'http://ex.com/a'));
  });
  it('blocks an http→https protocol upgrade (any scheme change)', () => {
    assert.ok(redirectCrossesSecurityBoundary('http://ex.com/a', 'https://ex.com/a'));
  });
  it('blocks a same-host port change', () => {
    assert.ok(redirectCrossesSecurityBoundary('http://ex.com:80/a', 'http://ex.com:8080/a'));
  });
  it('allows a cross-host port difference (AMI re-validates cross-host via SSRF)', () => {
    assert.ok(!redirectCrossesSecurityBoundary('http://ex.com:8080/a', 'http://other.com/a'));
  });
  it('allows an explicit default port (WHATWG URL normalizes :443→"" on https, as the reference does)', () => {
    assert.ok(!redirectCrossesSecurityBoundary('https://ex.com/a', 'https://ex.com:443/a'));
  });
  it('blocks an explicit non-default port against an implied default', () => {
    assert.ok(redirectCrossesSecurityBoundary('https://ex.com/a', 'https://ex.com:8443/a'));
  });
  it('blocks when the original URL is unparseable', () => {
    assert.ok(redirectCrossesSecurityBoundary('not a url', 'https://ex.com/a'));
  });
  it('blocks when the redirect URL is unparseable', () => {
    assert.ok(redirectCrossesSecurityBoundary('https://ex.com/a', 'not a url'));
  });
  it('allows same-protocol/same-port path change (regression lock)', () => {
    assert.ok(!redirectCrossesSecurityBoundary('https://ex.com/a', 'https://ex.com/b?x=1'));
  });
  it('allows a same-protocol/same-port CROSS-HOST redirect (AMI differentiator)', () => {
    assert.ok(!redirectCrossesSecurityBoundary('https://a.com/x', 'https://b.com/y'));
  });
  it('allows matching explicit ports', () => {
    assert.ok(!redirectCrossesSecurityBoundary('http://ex.com:8080/a', 'http://other.com:8080/b'));
  });
});

// ---------------------------------------------------------------------------
// E2E red-green through the real httpGet redirect loop (rawGet seam, no net).
// PRE-FIX the downgrade/port-change was followed (body returned); POST-FIX the
// follow is refused at the boundary guard.
// ---------------------------------------------------------------------------

type HopMap = Record<string, { status: number; location?: string; body?: string }>;

function seamFor(hops: HopMap): typeof _webHooks.rawGet {
  return ((
    _requester: typeof http | typeof https,
    url: string,
    _options: http.RequestOptions,
    callback: (res: http.IncomingMessage) => void,
  ) => {
    const hop = hops[url];
    const res = new EventEmitter() as unknown as http.IncomingMessage & { resume: () => void };
    res.resume = () => {};
    if (!hop) {
      res.statusCode = 404;
      res.headers = {};
      process.nextTick(() => {
        callback(res);
        process.nextTick(() => res.emit('end'));
      });
    } else if (hop.location) {
      res.statusCode = hop.status;
      res.headers = { location: hop.location };
      process.nextTick(() => callback(res));
    } else {
      res.statusCode = hop.status;
      res.headers = { 'content-type': 'text/plain' };
      process.nextTick(() => {
        callback(res);
        process.nextTick(() => {
          res.emit('data', Buffer.from(hop.body ?? ''));
          res.emit('end');
        });
      });
    }
    const req = new EventEmitter() as unknown as http.ClientRequest;
    (req as unknown as { destroy: () => void }).destroy = () => {};
    return req;
  }) as typeof _webHooks.rawGet;
}

describe('httpGet — refuses a boundary-crossing redirect (e2e)', () => {
  const origRawGet = _webHooks.rawGet;
  const origValidate = _webHooks.validateRedirectSafety;

  before(() => {
    // Make every redirect target "SSRF-safe" so the boundary guard is the sole
    // discriminator (PUBLIC host, no private-IP block in the way).
    _webHooks.validateRedirectSafety = async () => ({ resolvedIP: '93.184.216.34' });
  });
  after(() => {
    _webHooks.rawGet = origRawGet;
    _webHooks.validateRedirectSafety = origValidate;
  });

  it('refuses an https→http downgrade to a PUBLIC host', async () => {
    _webHooks.rawGet = seamFor({
      'https://start.example/': { status: 302, location: 'http://start.example/x' },
      'http://start.example/x': { status: 200, body: 'DOWNGRADED' },
    });
    await assert.rejects(
      httpGet('https://start.example/', new AbortController().signal, { maxRedirects: 3 }),
      /protocol\/port boundary/,
    );
  });

  it('refuses a port change to a PUBLIC host', async () => {
    _webHooks.rawGet = seamFor({
      'http://start.example/': { status: 302, location: 'http://start.example:8080/x' },
      'http://start.example:8080/x': { status: 200, body: 'PIVOTED' },
    });
    await assert.rejects(
      httpGet('http://start.example/', new AbortController().signal, { maxRedirects: 3 }),
      /protocol\/port boundary/,
    );
  });

  it('still follows a same-protocol/same-port cross-host redirect (differentiator lock)', async () => {
    _webHooks.rawGet = seamFor({
      'https://a.example/': { status: 302, location: 'https://b.example/final' },
      'https://b.example/final': { status: 200, body: 'FOLLOWED' },
    });
    const r = await httpGet('https://a.example/', new AbortController().signal, {
      maxRedirects: 3,
      resolvedIP: '93.184.216.34',
    });
    assert.equal(r.statusCode, 200);
    assert.equal(r.body, 'FOLLOWED');
  });
});
