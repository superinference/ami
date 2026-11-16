/**
 * Unit tests for the U021 session.ts parity additions:
 *  - isValidSessionId: strict format-validation boundary for untrusted session ids,
 *    wired into load()/loadAsync()/delete();
 *  - normalizeSessionTag + SessionManager.toggleTag: the /tag toggle/replace/remove
 *    state machine;
 *  - extractLastPrompt (distinct from the first-prompt preview) and
 *    resolveSessionSummary (customTitle > aiTitle > lastPrompt > firstPrompt);
 *  - list() surfacing persisted metadata (tag/mode/pr/worktree/entrypoint/
 *    permissionMode/lastPrompt/summary), filtering subagent sidechain sessions,
 *    dropping metadata-only sessions, and deduping by internal session id;
 *  - round-trip persistence of the new Session metadata fields.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  isValidSessionId,
  normalizeSessionTag,
  extractMeaningfulPreview,
  extractLastPrompt,
  resolveSessionSummary,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function session(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'u' },
    ...over,
  } as Session;
}
const user = (content: string): Message => ({ role: 'user', content } as Message);

describe('isValidSessionId', () => {
  it('accepts well-formed and AMI/custom ids (incl. spaces and UUIDs)', () => {
    for (const ok of [
      'a', 'session-20261116-101112-500-3', 'trunc-tool', 'my session',
      '3f2504e0-4f89-41d3-9a0c-0305e82c3301', 'A_b.c-1',
    ]) {
      assert.equal(isValidSessionId(ok), true, ok);
    }
  });

  it('rejects non-strings, empty/over-long, traversal and separator/control bytes', () => {
    assert.equal(isValidSessionId(undefined), false);
    assert.equal(isValidSessionId(123 as unknown), false);
    assert.equal(isValidSessionId(''), false);
    assert.equal(isValidSessionId('z'.repeat(256)), false);
    assert.equal(isValidSessionId('.'), false);
    assert.equal(isValidSessionId('..'), false);
    assert.equal(isValidSessionId('a..b'), false);
    assert.equal(isValidSessionId('a/b'), false);
    assert.equal(isValidSessionId('a\\b'), false);
    assert.equal(isValidSessionId('../../etc/passwd'), false);
    assert.equal(isValidSessionId('x\u0000y'), false);
    assert.equal(isValidSessionId('x\u0001y'), false);
    assert.equal(isValidSessionId('x\u007fy'), false);
  });
});

describe('load/loadAsync/delete reject invalid ids up front', () => {
  it('returns null / no-ops for a malformed id without throwing', async () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('keep-me', { messages: [user('hi')] }));

    assert.equal(sm.load('../../etc/passwd'), null);
    assert.equal(sm.load('a/b/c'), null);
    assert.equal(await sm.loadAsync('a/b/c'), null);
    // loadAsync and load agree for the same untrusted input.
    assert.equal(await sm.loadAsync('../../etc/passwd'), sm.load('../../etc/passwd'));

    // delete() of a malformed id is a no-op and must not remove the valid session.
    sm.delete('../../etc/passwd');
    assert.ok(sm.load('keep-me'), 'valid session must survive an invalid delete');
  });
});

describe('normalizeSessionTag', () => {
  it('collapses whitespace and trims', () => {
    assert.equal(normalizeSessionTag('  bug  fix '), 'bug fix');
    assert.equal(normalizeSessionTag('a\tb\nc'), 'a b c');
  });
  it('strips control and zero-width/bidi characters', () => {
    assert.equal(normalizeSessionTag('\u200bwip\u0001'), 'wip');
    assert.equal(normalizeSessionTag('fe\ufeffature'), 'feature');
  });
  it('returns empty string when nothing meaningful remains', () => {
    assert.equal(normalizeSessionTag('  \u0001\u200b '), '');
    assert.equal(normalizeSessionTag(''), '');
  });
});

describe('SessionManager.toggleTag state machine', () => {
  it('adds, replaces, then removes the single session tag (round-tripped)', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('s', { messages: [user('hi')] }));

    const a = sm.toggleTag('s', '  bugfix ');
    assert.deepEqual(a, { ok: true, action: 'added', tag: 'bugfix', previous: undefined });
    assert.equal(sm.load('s')?.tag, 'bugfix');

    const r = sm.toggleTag('s', 'feature');
    assert.deepEqual(r, { ok: true, action: 'replaced', tag: 'feature', previous: 'bugfix' });
    assert.equal(sm.load('s')?.tag, 'feature');

    const rm = sm.toggleTag('s', 'feature');
    assert.deepEqual(rm, { ok: true, action: 'removed', previous: 'feature' });
    assert.equal(sm.load('s')?.tag, undefined);
  });

  it('fails cleanly for no active session and for an empty-after-normalize tag', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    assert.deepEqual(sm.toggleTag('missing', 'x'), { ok: false, error: 'No active session to tag.' });

    sm.save(session('s2', { messages: [user('hi')] }));
    assert.deepEqual(sm.toggleTag('s2', '\u200b\u0001'), { ok: false, error: 'Tag name cannot be empty.' });
    assert.equal(sm.load('s2')?.tag, undefined);
  });
});

describe('extractLastPrompt vs extractMeaningfulPreview', () => {
  it('preview is the FIRST prompt, lastPrompt is the LAST', () => {
    const msgs = [user('first thing'), { role: 'assistant', content: 'ok' } as Message, user('last thing')];
    assert.equal(extractMeaningfulPreview(msgs), 'first thing');
    assert.equal(extractLastPrompt(msgs), 'last thing');
  });
  it('returns (no messages) when there is no meaningful user turn', () => {
    assert.equal(extractLastPrompt([]), '(no messages)');
  });
  it('falls back to a slash-command name when no real prompt exists', () => {
    const msgs = [user('<command-name>clear</command-name>')];
    assert.equal(extractLastPrompt(msgs), 'clear');
  });
});

describe('resolveSessionSummary precedence', () => {
  it('honours customTitle > aiTitle > lastPrompt > firstPrompt', () => {
    assert.equal(resolveSessionSummary({ customTitle: 'C', aiTitle: 'A' }, { lastPrompt: 'L', firstPrompt: 'F' }), 'C');
    assert.equal(resolveSessionSummary({ aiTitle: 'A' }, { lastPrompt: 'L', firstPrompt: 'F' }), 'A');
    assert.equal(resolveSessionSummary({}, { lastPrompt: 'L', firstPrompt: 'F' }), 'L');
    assert.equal(resolveSessionSummary({}, { firstPrompt: 'F' }), 'F');
  });
  it('returns undefined for a metadata-only session (and ignores the (no messages) sentinel)', () => {
    assert.equal(resolveSessionSummary({}, {}), undefined);
    assert.equal(resolveSessionSummary({}, { lastPrompt: '(no messages)', firstPrompt: '(no messages)' }), undefined);
  });
});

describe('list() surfaces metadata and filters/dedupes', () => {
  it('populates lastPrompt, summary and the persisted metadata fields', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('meta', {
      messages: [user('do the first'), { role: 'assistant', content: 'ok' } as Message, user('do the last')],
      customTitle: 'My Title',
      tag: 'wip',
      mode: 'coordinator',
      pr: { number: 7, url: 'https://github.com/o/r/pull/7', repository: 'o/r' },
      worktreeState: { originalCwd: '/tmp/proj', worktreePath: '/tmp/proj-wt', branch: 'feat' },
      entrypoint: 'cli',
      settings: { permissionMode: 'auto-allow' },
    }));
    const entry = sm.list().find(e => e.id === 'meta')!;
    assert.equal(entry.preview, 'do the first');
    assert.equal(entry.lastPrompt, 'do the last');
    assert.equal(entry.summary, 'My Title');
    assert.equal(entry.tag, 'wip');
    assert.equal(entry.mode, 'coordinator');
    assert.deepEqual(entry.pr, { number: 7, url: 'https://github.com/o/r/pull/7', repository: 'o/r' });
    assert.equal(entry.worktree, '/tmp/proj-wt');
    assert.equal(entry.entrypoint, 'cli');
    assert.equal(entry.permissionMode, 'auto-allow');
  });

  it('excludes subagent sidechain sessions by default and includes them on request', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('main-1', { messages: [user('real work')] }));
    sm.save(session('side-1', { messages: [user('subagent work')], isSidechain: true }));

    const ids = sm.list().map(e => e.id);
    assert.ok(ids.includes(asSessionId('main-1')));
    assert.ok(!ids.includes(asSessionId('side-1')), 'sidechain must be hidden by default');

    const all = sm.list({ includeSidechain: true });
    assert.ok(all.some(e => e.id === asSessionId('side-1')));
    assert.equal(all.find(e => e.id === asSessionId('side-1'))?.isSidechain, true);
  });

  it('dropEmpty removes a metadata-only session with no title and no prompts', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('empty-1', { messages: [] }));
    sm.save(session('full-1', { messages: [user('hello')] }));

    assert.ok(sm.list().some(e => e.id === asSessionId('empty-1')), 'kept by default');
    const dropped = sm.list({ dropEmpty: true }).map(e => e.id);
    assert.ok(!dropped.includes(asSessionId('empty-1')));
    assert.ok(dropped.includes(asSessionId('full-1')));
  });

  it('dedupes two files carrying the same internal session id, keeping the newest', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    // Two on-disk files with DIFFERENT filenames but the SAME internal id.
    const older = { id: 'dup', messages: [], customTitle: 'Old', createdAt: 'x', updatedAt: 'x', provider: { model: 'm', baseUrl: 'u' } };
    const newer = { ...older, customTitle: 'New' };
    fs.writeFileSync(path.join(dir, 'copyA.json'), JSON.stringify(older));
    fs.writeFileSync(path.join(dir, 'copyB.json'), JSON.stringify(newer));
    const past = new Date(Date.now() - 60_000);
    fs.utimesSync(path.join(dir, 'copyA.json'), past, past); // make copyA older

    const dupEntries = sm.list().filter(e => e.id === asSessionId('dup'));
    assert.equal(dupEntries.length, 1, 'duplicate internal id must be collapsed to one entry');
    assert.equal(dupEntries[0].summary, 'New', 'the newest readable copy wins');
  });
});

describe('Session metadata round-trips through save/load', () => {
  it('persists mode/pr/worktreeState/entrypoint/isSidechain/permissionMode', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('rt', {
      messages: [user('hi')],
      mode: 'normal',
      pr: { number: 42, url: 'https://example/pr/42' },
      worktreeState: { originalCwd: '/a', worktreePath: '/b', branch: 'wip' },
      entrypoint: 'detached',
      isSidechain: false,
      settings: { permissionMode: 'deny-all' },
    }));
    const loaded = sm.load('rt')!;
    assert.equal(loaded.mode, 'normal');
    assert.deepEqual(loaded.pr, { number: 42, url: 'https://example/pr/42' });
    assert.deepEqual(loaded.worktreeState, { originalCwd: '/a', worktreePath: '/b', branch: 'wip' });
    assert.equal(loaded.entrypoint, 'detached');
    assert.equal(loaded.settings?.permissionMode, 'deny-all');
  });

  it('round-trips an explicit null worktreeState (exited-worktree marker)', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('rt-null', { messages: [user('hi')], worktreeState: null }));
    assert.equal(sm.load('rt-null')!.worktreeState, null);
  });
});
