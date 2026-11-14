/**
 * Claude-parity: session-search hardening — match-centered snippets, scan/read
 * budgets, and system-reminder stripping.
 *   - matchSnippet: …before <match> after… with a bounded total length
 *   - size-guarded read (skip a session file over MAX_SESSION_SEARCH_BYTES)
 *   - system-reminder spans do not produce phantom matches
 *
 * Run: cd core/core && node --import tsx --test test/session-search-hardening.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  searchSessions,
  matchSnippet,
  flattenMessageText,
  MAX_SESSION_SEARCH_BYTES,
  MAX_SNIPPET_CHARS,
} from '../src/session-search';
import { projectStateDir } from '../src/reference-journal';

let tmpDir: string;
beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-srch-')); });
afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ } });

function writeSession(id: string, obj: Record<string, unknown>): string {
  const dir = projectStateDir(tmpDir, 'sessions');
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, `${id}.json`);
  fs.writeFileSync(p, JSON.stringify({ id, ...obj }));
  return p;
}

describe('matchSnippet', () => {
  it('returns short text verbatim', () => {
    assert.equal(matchSnippet('short text', ['text'], ''), 'short text');
  });
  it('centers the window on the match with leading + trailing ellipsis', () => {
    const text = 'A'.repeat(400) + ' NEEDLE ' + 'B'.repeat(400);
    const snip = matchSnippet(text, ['needle'], 'needle');
    assert.ok(snip.toLowerCase().includes('needle'), 'snippet must contain the match');
    assert.ok(snip.startsWith('…') && snip.endsWith('…'), 'both sides clipped → both ellipses');
  });
  it('never exceeds the snippet budget (ellipses included)', () => {
    const text = 'x'.repeat(100) + 'zzmatch' + 'y'.repeat(2000);
    const snip = matchSnippet(text, ['zzmatch'], 'zzmatch');
    assert.ok(snip.length <= MAX_SNIPPET_CHARS, `len ${snip.length}`);
  });
  it('a head match has no leading ellipsis', () => {
    const text = 'keyword ' + 'q'.repeat(500);
    const snip = matchSnippet(text, ['keyword'], 'keyword');
    assert.ok(!snip.startsWith('…'));
    assert.ok(snip.endsWith('…'));
  });
});

describe('searchSessions — snippet centering', () => {
  it('returns a match-centered snippet, not a fixed head slice', () => {
    writeSession('mid', { messages: [{ role: 'user', content: 'Z'.repeat(500) + ' UNIQBODY ' + 'Y'.repeat(500) }] });
    const r = searchSessions(tmpDir, 'uniqbody').find(x => x.sessionId === 'mid');
    assert.ok(r, 'match found');
    assert.ok(r!.content.toLowerCase().includes('uniqbody'), 'snippet is centered on the match');
    assert.ok(r!.content.length <= MAX_SNIPPET_CHARS);
  });
});

describe('searchSessions — size guard', () => {
  it('skips a session file larger than MAX_SESSION_SEARCH_BYTES', () => {
    const p = writeSession('big', { messages: [{ role: 'user', content: 'giantkeyword here' }] });
    fs.truncateSync(p, MAX_SESSION_SEARCH_BYTES + 1); // sparse grow past the cap
    const r = searchSessions(tmpDir, 'giantkeyword');
    assert.equal(r.find(x => x.sessionId === 'big'), undefined, 'oversized session must be skipped');
  });
});

describe('system-reminder stripping (no phantom matches)', () => {
  it('flattenMessageText drops <system-reminder> spans', () => {
    const out = flattenMessageText({ role: 'user', content: 'real text <system-reminder>phantomword</system-reminder> more' });
    assert.ok(out.includes('real text'));
    assert.doesNotMatch(out, /phantomword/);
  });
  it('searchSessions does not match a term only inside a system-reminder', () => {
    writeSession('sr', { messages: [{ role: 'user', content: 'ordinary <system-reminder>phantomneedle</system-reminder> body' }] });
    const r = searchSessions(tmpDir, 'phantomneedle');
    assert.equal(r.find(x => x.sessionId === 'sr'), undefined, 'reminder-only term must not match');
  });
});
