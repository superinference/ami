import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions, matchSnippet, MAX_SNIPPET_CHARS } from '../src/session-search';
import { isValidSessionId } from '../src/session';
import { projectStateDir } from '../src/reference-journal';

/** Scan a string for an UNPAIRED UTF-16 surrogate (a lone high or low surrogate) —
 *  the broken-glyph artifact produced when a slice cuts through an astral codepoint
 *  (e.g. an emoji). A well-formed string returns false. */
function hasLoneSurrogate(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      // high surrogate must be followed by a low surrogate
      const next = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (!(next >= 0xdc00 && next <= 0xdfff)) return true;
      i++; // consume the valid pair
    } else if (c >= 0xdc00 && c <= 0xdfff) {
      // a low surrogate not preceded by a high surrogate (handled above) is lone
      return true;
    }
  }
  return false;
}

/**
 * U077 — session-id validation of on-disk `*.json` filenames before enumeration.
 *
 * Parity with the reference's `listSessionsImpl` UUID-filename validation
 * (listSessionsImpl.ts): a stray `.json` file dropped in the sessions dir whose
 * stem is NOT a format-valid session id (empty stem `.json`, or a crafted
 * traversal stem `...json` → `..`) must NOT be scanned as a session. AMI's id
 * contract is {@link isValidSessionId} (ids are `session-…`, not UUIDs), so the
 * filter reuses that single canonical validator — no second id grammar.
 */
describe('searchSessions — session-id filename validation (U077)', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-u077-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    // A real, valid session that legitimately matches the query.
    fs.writeFileSync(path.join(sessDir, 'session-real.json'), JSON.stringify({
      id: 'session-real',
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips an empty-stem ".json" file (stem "" is not a valid session id)', () => {
    // A stray file literally named ".json" — stem is "", which isValidSessionId rejects.
    fs.writeFileSync(path.join(sessDir, '.json'), JSON.stringify({
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
    const results = searchSessions(tmpDir, 'uniqueneedletoken');
    // Only the real session must surface; the empty-stem file is never scanned.
    assert.equal(results.length, 1);
    assert.equal(results[0].sessionId, 'session-real');
  });

  it('skips a crafted traversal-stem file ("...json" → stem "..")', () => {
    // "...json" ends with ".json"; stripping the suffix yields "..", which
    // isValidSessionId rejects as a path-traversal stem.
    fs.writeFileSync(path.join(sessDir, '...json'), JSON.stringify({
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
    const results = searchSessions(tmpDir, 'uniqueneedletoken');
    assert.equal(results.length, 1);
    assert.equal(results[0].sessionId, 'session-real');
  });

  it('still scans files whose stem is a valid session id', () => {
    fs.writeFileSync(path.join(sessDir, 'session-second.json'), JSON.stringify({
      id: 'session-second',
      messages: [{ role: 'user', content: 'uniqueneedletoken again' }],
    }));
    const ids = searchSessions(tmpDir, 'uniqueneedletoken').map(r => r.sessionId).sort();
    assert.deepEqual(ids, ['session-real', 'session-second']);
  });

  it('isValidSessionId is the canonical validator the filter reuses', () => {
    // Guards against a divergent second id grammar being introduced.
    assert.equal(isValidSessionId(''), false);
    assert.equal(isValidSessionId('..'), false);
    assert.equal(isValidSessionId('session-real'), true);
  });
});

/**
 * U077 — surrogate-safe snippet boundaries (from the reference's surrogate-pair
 * concern in render-to-screen.ts scanPositions, L163-196). The reference maps
 * code-unit `indexOf` offsets to display cells, explicitly accounting for
 * surrogate pairs so an astral character (emoji) is never split across a boundary.
 *
 * AMI's search returns string snippets, not render-cell positions, so the full
 * code-unit-to-cell map is out of scope (it would be dead code — there is no
 * positional-highlight consumer in session-search). The SENSIBLE, in-scope kernel
 * is that {@link matchSnippet} must not slice THROUGH an emoji at a snippet
 * boundary — a UTF-16 code-unit slice can leave a lone surrogate that renders as a
 * broken replacement glyph (U+FFFD) at the snippet edge.
 */
describe('matchSnippet — surrogate-safe boundaries (U077)', () => {
  const EMOJI = '\u{1F600}'; // 😀 = 😀 (two UTF-16 code units)

  it('head path (no match): slice(0,max) never ends on a lone high surrogate', () => {
    // phrase/terms empty → idx stays -1 → the head-slice path (used by the
    // reranker recent-fill snippet). max=5 lands mid-pair at code unit 4.
    const text = EMOJI.repeat(20);
    const snip = matchSnippet(text, [], '', 5);
    assert.equal(hasLoneSurrogate(snip), false, `lone surrogate in: ${JSON.stringify(snip)}`);
    // Two whole emoji fit in 5 code units (4 used); the half-emoji is dropped.
    assert.equal(snip, EMOJI.repeat(2));
  });

  it('start boundary never begins on a lone low surrogate', () => {
    // Centered snippet whose computed start lands on the low half of an emoji.
    // Short needle so the whole match stays inside the small centered window.
    const text = EMOJI.repeat(10) + 'ND' + EMOJI.repeat(10);
    const snip = matchSnippet(text, ['nd'], 'nd', 10);
    assert.equal(hasLoneSurrogate(snip), false, `lone surrogate in: ${JSON.stringify(snip)}`);
    assert.ok(snip.toLowerCase().includes('nd'));
  });

  it('end boundary never ends on a lone high surrogate', () => {
    // Match at index 0; the snippet end lands on the high half of an emoji.
    const text = 'NEEDLE' + EMOJI.repeat(20);
    const snip = matchSnippet(text, ['needle'], 'needle', 9);
    assert.equal(hasLoneSurrogate(snip), false, `lone surrogate in: ${JSON.stringify(snip)}`);
    assert.ok(snip.toLowerCase().includes('needle'));
  });

  it('inner room re-slice stays surrogate-safe', () => {
    // When both ellipses are added, the room re-slice can also split a pair.
    const text = EMOJI.repeat(5) + 'NEEDLE' + EMOJI.repeat(50);
    const snip = matchSnippet(text, ['needle'], 'needle', 15);
    assert.equal(hasLoneSurrogate(snip), false, `lone surrogate in: ${JSON.stringify(snip)}`);
  });

  it('pure-ASCII snippets are unchanged (regression)', () => {
    const text = 'a'.repeat(100) + 'needle' + 'b'.repeat(100);
    const snip = matchSnippet(text, ['needle'], 'needle', 20);
    assert.ok(snip.includes('needle'));
    assert.ok(snip.length <= 20);
    assert.ok(snip.startsWith('…') && snip.endsWith('…'));
  });

  it('text that already fits is returned verbatim even with emoji', () => {
    const text = `hi ${EMOJI} there`;
    assert.equal(matchSnippet(text, ['there'], 'there', MAX_SNIPPET_CHARS), text);
  });
});
