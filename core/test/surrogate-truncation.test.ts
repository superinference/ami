/**
 * Red-green reproducer tests for the surrogate-pair splitting bug in
 * truncation functions.
 *
 * Three truncation sites use raw String.slice() which operates on UTF-16
 * code units and can split surrogate pairs, producing lone surrogates
 * that corrupt data on UTF-8 round-trip (U+FFFD replacement).
 *
 * Affected functions:
 *   1. spillToolText()     -- spill-output.ts
 *   2. truncateBashText()  -- bash.ts (internal truncate(), now exported)
 *
 * ContextManager.truncateToolOutput() is tested separately in common/test/
 * because core tests must not import from common.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { spillToolText } from '../src/tools/spill-output';
import { truncateBashText, diagnoseError } from '../src/tools/bash';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Assert that `s` contains no lone (unpaired) UTF-16 surrogates.
 * A lone high surrogate (0xD800..0xDBFF) not followed by a low surrogate,
 * or a lone low surrogate (0xDC00..0xDFFF) not preceded by a high surrogate,
 * will cause U+FFFD replacement on UTF-8 round-trip.
 */
function assertNoLoneSurrogates(s: string, label: string): void {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i);
    if (code >= 0xD800 && code <= 0xDBFF) {
      // High surrogate -- must be followed by a low surrogate
      const next = i + 1 < s.length ? s.charCodeAt(i + 1) : -1;
      assert.ok(
        next >= 0xDC00 && next <= 0xDFFF,
        `${label}: lone high surrogate at position ${i}`,
      );
      i++; // skip the low surrogate
    } else if (code >= 0xDC00 && code <= 0xDFFF) {
      assert.fail(`${label}: lone low surrogate at position ${i}`);
    }
  }
}

/**
 * Assert UTF-8 round-trip integrity: encoding to UTF-8 and decoding back
 * must produce the same string.  Lone surrogates are replaced with U+FFFD.
 */
function assertUtf8RoundTrip(s: string, label: string): void {
  const roundTrip = Buffer.from(s, 'utf-8').toString('utf-8');
  assert.equal(s, roundTrip, `${label}: UTF-8 round-trip must be lossless`);
}

// ---------------------------------------------------------------------------
// spillToolText -- spill-output.ts:27
// ---------------------------------------------------------------------------

describe('spillToolText surrogate-pair safety', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'surr-spill-'));
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true });
  });

  it('does not split a surrogate pair at the head boundary', () => {
    // With limit=1000, headLen = floor(1000 * 0.67) = 670
    // Place emoji at position 669 so slice(0, 670) cuts inside it:
    //   positions 669..670 = surrogate pair for U+1F600
    //   slice(0, 670) grabs 669 A's + \uD83D = lone high surrogate
    const limit = 1000;
    const headLen = Math.floor(limit * 0.67); // 670
    const text = 'A'.repeat(headLen - 1) + '\u{1F600}' + 'B'.repeat(limit);
    // text.length = 669 + 2 + 1000 = 1671 > limit

    const result = spillToolText(tmpDir, 'emoji-head', text, limit);

    assertNoLoneSurrogates(result, 'spillToolText head');
    assertUtf8RoundTrip(result, 'spillToolText head');
  });

  it('does not split a surrogate pair at the tail boundary', () => {
    // tailLen = max(0, limit - headLen - 200)
    // With limit=1000: tailLen = max(0, 1000 - 670 - 200) = 130
    // text.slice(-130) starts 130 chars from the end
    // Place emoji so the low surrogate is at text.length - tailLen
    const limit = 1000;
    const headLen = Math.floor(limit * 0.67); // 670
    const tailLen = Math.max(0, limit - headLen - 200); // 130

    // Build text so that the emoji splits at the tail boundary.
    // The tail starts at text.length - tailLen.
    // Place high surrogate at text.length - tailLen - 1
    // and low surrogate at text.length - tailLen.
    const totalLen = 2000;
    const tailStart = totalLen - tailLen; // 1870
    // emoji high surrogate at position tailStart-1 (1869), low at tailStart (1870)
    const text = 'A'.repeat(tailStart - 1) + '\u{1F600}' + 'C'.repeat(totalLen - tailStart - 1);
    assert.equal(text.length, totalLen);

    const result = spillToolText(tmpDir, 'emoji-tail', text, limit);

    assertNoLoneSurrogates(result, 'spillToolText tail');
    assertUtf8RoundTrip(result, 'spillToolText tail');
  });

  it('handles CJK supplementary characters (U+20000 range)', () => {
    // CJK Unified Ideographs Extension B -- also surrogate pairs in UTF-16
    const cjk = '\u{20000}'; // two code units in UTF-16
    const limit = 1000;
    const headLen = Math.floor(limit * 0.67); // 670
    const text = 'A'.repeat(headLen - 1) + cjk + 'B'.repeat(limit);

    const result = spillToolText(tmpDir, 'cjk', text, limit);

    assertNoLoneSurrogates(result, 'spillToolText CJK');
    assertUtf8RoundTrip(result, 'spillToolText CJK');
  });
});

// ---------------------------------------------------------------------------
// truncateBashText -- bash.ts (the internal truncate() function)
// ---------------------------------------------------------------------------

describe('truncateBashText surrogate-pair safety', () => {
  it('does not split a surrogate pair at the head boundary', () => {
    // half = floor((maxLength - 100) / 2)
    // With maxLength = 300: half = floor(200 / 2) = 100
    // Place emoji at position 99 so slice(0, 100) cuts inside it
    const maxLength = 300;
    const half = Math.floor((maxLength - 100) / 2); // 100
    const text = 'A'.repeat(half - 1) + '\u{1F600}' + 'B'.repeat(maxLength);
    // text.length = 99 + 2 + 300 = 401 > maxLength

    const result = truncateBashText(text, maxLength);

    assertNoLoneSurrogates(result, 'truncateBashText head');
    assertUtf8RoundTrip(result, 'truncateBashText head');
  });

  it('does not split a surrogate pair at the tail boundary', () => {
    // tail slice: text.slice(text.length - half)
    // The tail starts at text.length - half
    // Place emoji so the low surrogate is at text.length - half
    const maxLength = 300;
    const half = Math.floor((maxLength - 100) / 2); // 100
    const totalLen = 500;
    const tailStart = totalLen - half; // 400
    // High surrogate at tailStart-1 (399), low surrogate at tailStart (400)
    const text = 'A'.repeat(tailStart - 1) + '\u{1F600}' + 'C'.repeat(totalLen - tailStart - 1);
    assert.equal(text.length, totalLen);

    const result = truncateBashText(text, maxLength);

    assertNoLoneSurrogates(result, 'truncateBashText tail');
    assertUtf8RoundTrip(result, 'truncateBashText tail');
  });

  it('handles CJK supplementary characters (U+20000 range)', () => {
    const cjk = '\u{20000}';
    const maxLength = 300;
    const half = Math.floor((maxLength - 100) / 2); // 100
    const text = 'A'.repeat(half - 1) + cjk + 'B'.repeat(maxLength);

    const result = truncateBashText(text, maxLength);

    assertNoLoneSurrogates(result, 'truncateBashText CJK');
    assertUtf8RoundTrip(result, 'truncateBashText CJK');
  });

  it('does not truncate when text fits within maxLength', () => {
    const text = 'Hello \u{1F600} World';
    const result = truncateBashText(text, 1000);
    assert.equal(result, text);
  });
});

// ---------------------------------------------------------------------------
// CC17: ENOSPC detection in diagnoseError -- bash.ts ERROR_HINTS
// ---------------------------------------------------------------------------

describe('diagnoseError ENOSPC detection', () => {
  it('detects ENOSPC error code', () => {
    const hint = diagnoseError('write failed: ENOSPC');
    assert.ok(hint !== null, 'should return a hint for ENOSPC');
    assert.ok(hint!.includes('Disk is full'), 'hint should mention disk is full');
    assert.ok(hint!.includes('df -h'), 'hint should suggest df -h');
  });

  it('detects "No space left on device" message', () => {
    const hint = diagnoseError('Error: No space left on device');
    assert.ok(hint !== null, 'should return a hint for No space left on device');
    assert.ok(hint!.includes('Disk is full'), 'hint should mention disk is full');
    assert.ok(hint!.includes('df -h'), 'hint should suggest df -h');
  });

  it('does not false-positive on unrelated errors', () => {
    const hint = diagnoseError('something else went wrong');
    // Should not match the ENOSPC pattern (may match other patterns or be null)
    if (hint !== null) {
      assert.ok(!hint.includes('Disk is full'), 'should not mention disk full for unrelated errors');
    }
  });
});

// ---------------------------------------------------------------------------
// CC17: spillToolText ENOSPC-specific banner -- spill-output.ts
// Tests the exported buildSpillBanner helper (added as part of CC17 fix)
// ---------------------------------------------------------------------------

describe('buildSpillBanner ENOSPC discrimination', () => {
  it('exports buildSpillBanner as a function', () => {
    // buildSpillBanner is added as part of the CC17 fix.
    // In RED phase this will be undefined => test fails.
    const mod = require('../src/tools/spill-output') as Record<string, unknown>;
    assert.ok(typeof mod.buildSpillBanner === 'function',
      'spill-output.ts must export buildSpillBanner');
  });

  it('returns ENOSPC-specific banner when errCode is ENOSPC', () => {
    const mod = require('../src/tools/spill-output') as Record<string, unknown>;
    assert.ok(typeof mod.buildSpillBanner === 'function',
      'buildSpillBanner must exist');
    const banner = (mod.buildSpillBanner as (omitted: number, spillFile: string | null, errCode?: string) => string)(
      5000, null, 'ENOSPC',
    );
    assert.ok(/disk full/i.test(banner),
      `banner should mention "disk full", got: ${banner}`);
    assert.ok(/ENOSPC/.test(banner),
      `banner should mention ENOSPC, got: ${banner}`);
    // Must NOT contain the generic message
    assert.ok(!/could not persist to disk/.test(banner),
      'ENOSPC banner must not use generic "could not persist to disk" text');
  });

  it('returns generic banner for non-ENOSPC errors', () => {
    const mod = require('../src/tools/spill-output') as Record<string, unknown>;
    assert.ok(typeof mod.buildSpillBanner === 'function',
      'buildSpillBanner must exist');
    const banner = (mod.buildSpillBanner as (omitted: number, spillFile: string | null, errCode?: string) => string)(
      5000, null, 'EPERM',
    );
    assert.ok(/could not persist to disk/.test(banner),
      `non-ENOSPC errors should use generic message, got: ${banner}`);
    assert.ok(!/disk full/i.test(banner),
      'generic banner must not mention "disk full"');
  });

  it('returns persisted-to-file banner when spillFile is set', () => {
    const mod = require('../src/tools/spill-output') as Record<string, unknown>;
    assert.ok(typeof mod.buildSpillBanner === 'function',
      'buildSpillBanner must exist');
    const banner = (mod.buildSpillBanner as (omitted: number, spillFile: string | null, errCode?: string) => string)(
      5000, '/tmp/spill-12345.txt',
    );
    assert.ok(/persisted to/.test(banner),
      `banner should mention persisted, got: ${banner}`);
    assert.ok(banner.includes('/tmp/spill-12345.txt'),
      'banner should include the file path');
  });

  it('returns generic banner when no errCode provided (undefined)', () => {
    const mod = require('../src/tools/spill-output') as Record<string, unknown>;
    assert.ok(typeof mod.buildSpillBanner === 'function',
      'buildSpillBanner must exist');
    const banner = (mod.buildSpillBanner as (omitted: number, spillFile: string | null, errCode?: string) => string)(
      5000, null,
    );
    assert.ok(/could not persist to disk/.test(banner),
      `no-errCode should use generic message, got: ${banner}`);
  });
});
