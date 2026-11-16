/**
 * U087 — web-utils.ts SSRF-range + publicly-resolvable-host parity gaps.
 *
 * Covers the confirmed gaps for core/core/src/tools/web-utils.ts:
 *  - Full fe80::/10 link-local range (fe80..febf first hextet, not just the
 *    `fe80:` string prefix).
 *  - Full fc00::/7 unique-local range (fc00:: .. fdff:: → first hex pair fc/fd,
 *    not just the `fc00:` prefix).
 *  - Reject single-label (dot-less) hostnames pre-DNS in validateUrlSafety.
 *
 * Each case below FAILS on the pre-fix implementation (RED): `fe81::1`/`fc01::1`
 * were treated as public, and `http://intranet/` reached the DNS lookup.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  isPrivateIP,
  isBlockedRedirectTarget,
  validateUrlSafety,
  type HostLookupAll,
} from '../src/tools/web-utils';

describe('U087 — isPrivateIP full fe80::/10 link-local range', () => {
  it('blocks the whole fe80..febf first-hextet range, not just fe80:', () => {
    assert.equal(isPrivateIP('fe80::1'), true);
    assert.equal(isPrivateIP('fe81::1'), true);
    assert.equal(isPrivateIP('fe90::abcd'), true);
    assert.equal(isPrivateIP('fea0::1'), true);
    assert.equal(isPrivateIP('febf::1'), true);
    // Uppercase literal (redirect host / caller-supplied) is normalized too.
    assert.equal(isPrivateIP('FE81::1'), true);
  });

  it('does NOT block fec0:: and above (outside fe80::/10)', () => {
    assert.equal(isPrivateIP('fec0::1'), false);
    assert.equal(isPrivateIP('ff02::1'), false);
    // fe7f:: is below the range start.
    assert.equal(isPrivateIP('fe7f::1'), false);
  });
});

describe('U087 — isPrivateIP full fc00::/7 unique-local range', () => {
  it('blocks the whole fc00:: .. fdff:: range (fc and fd prefixes)', () => {
    assert.equal(isPrivateIP('fc00::1'), true);
    assert.equal(isPrivateIP('fc01::1'), true);
    assert.equal(isPrivateIP('fcff::1'), true);
    assert.equal(isPrivateIP('fd00::1'), true);
    assert.equal(isPrivateIP('fdff::1'), true);
  });

  it('keeps public global-unicast IPv6 allowed', () => {
    assert.equal(isPrivateIP('2606:2800:220:1::1'), false);
    assert.equal(isPrivateIP('2001:db8::1'), false);
  });
});

describe('U087 — isBlockedRedirectTarget uses the widened ranges', () => {
  it('blocks bracketed fe81::/fc01:: redirect targets', () => {
    assert.equal(isBlockedRedirectTarget('http://[fe81::1]/'), true);
    assert.equal(isBlockedRedirectTarget('http://[fc01::1]/'), true);
    assert.equal(isBlockedRedirectTarget('http://[fcff::1]/'), true);
    // Public IPv6 redirect target stays followable.
    assert.equal(isBlockedRedirectTarget('http://[2606:2800:220:1::1]/'), false);
  });
});

describe('U087 — validateUrlSafety rejects single-label hostnames pre-DNS', () => {
  const neverLookup: HostLookupAll = (() => {
    throw new Error('DNS lookup must not run for a single-label host');
  }) as HostLookupAll;

  it('blocks a dot-less hostname before any DNS lookup', async () => {
    const res = await validateUrlSafety('http://intranet/', neverLookup);
    assert.ok('error' in res);
    assert.match((res as { error: string }).error, /single-label/);
  });

  it('blocks another bare internal name without resolving it', async () => {
    const res = await validateUrlSafety('https://wiki/', neverLookup);
    assert.ok('error' in res);
  });

  it('still allows a normal multi-label public host (reaches DNS)', async () => {
    const lookup: HostLookupAll = (_h, _o, cb) =>
      cb(null, [{ address: '93.184.216.34', family: 4 }]);
    const res = await validateUrlSafety('https://ok.example/', lookup);
    assert.deepEqual(res, { resolvedIP: '93.184.216.34' });
  });

  it('exempts IPv4-literal hosts from the single-label check', async () => {
    const lookup: HostLookupAll = (_h, _o, cb) =>
      cb(null, [{ address: '93.184.216.34', family: 4 }]);
    const res = await validateUrlSafety('http://93.184.216.34/', lookup);
    assert.ok('resolvedIP' in res);
  });
});
