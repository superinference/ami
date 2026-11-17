/**
 * U021 session.ts parity additions (this wave):
 *  - parsePrIdentifier: integer | GitHub PR-URL -> number | null;
 *  - filterSessionsByPr + list({ filterByPr }): the true/number/string PR filter;
 *  - list({ excludeId }): drop the live current session from its own picker;
 *  - checkCrossProjectResume: three-way cross-project / same-repo-worktree
 *    classification with a copyable `cd … && ami --resume` command;
 *  - cleanupOldSessions strict content guard: never delete a non-session `.json`.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  parsePrIdentifier,
  filterSessionsByPr,
  checkCrossProjectResume,
  type Session,
} from '../src/session';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021-pr-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function session(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [{ role: 'user', content: 'hello world' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'u' },
    ...over,
  } as Session;
}

describe('parsePrIdentifier', () => {
  it('accepts a bare positive integer', () => {
    assert.equal(parsePrIdentifier('42'), 42);
    assert.equal(parsePrIdentifier('1'), 1);
    assert.equal(parsePrIdentifier(' 7 '), 7); // parseInt tolerates surrounding space
  });

  it('rejects zero / negative / non-numeric leading garbage', () => {
    assert.equal(parsePrIdentifier('0'), null);
    assert.equal(parsePrIdentifier('-3'), null);
    assert.equal(parsePrIdentifier('abc'), null);
    assert.equal(parsePrIdentifier(''), null);
  });

  it('extracts the PR number from a GitHub pull-request URL', () => {
    assert.equal(parsePrIdentifier('https://github.com/owner/repo/pull/123'), 123);
    assert.equal(parsePrIdentifier('github.com/a-b/c.d/pull/9'), 9);
  });

  it('returns null for a URL that is not a GitHub PR', () => {
    assert.equal(parsePrIdentifier('https://github.com/owner/repo/issues/5'), null);
    assert.equal(parsePrIdentifier('https://example.com/pull/5'), null);
  });
});

describe('filterSessionsByPr', () => {
  const entries = [
    { id: 'a', pr: { number: 10 } },
    { id: 'b', pr: { number: 20 } },
    { id: 'c' }, // no PR
  ];

  it('true keeps only entries linked to any PR', () => {
    assert.deepEqual(filterSessionsByPr(entries, true).map(e => e.id), ['a', 'b']);
  });

  it('number keeps only the exact PR', () => {
    assert.deepEqual(filterSessionsByPr(entries, 20).map(e => e.id), ['b']);
    assert.deepEqual(filterSessionsByPr(entries, 99).map(e => e.id), []);
  });

  it('string parses (integer or URL) then matches', () => {
    assert.deepEqual(filterSessionsByPr(entries, '10').map(e => e.id), ['a']);
    assert.deepEqual(
      filterSessionsByPr(entries, 'https://github.com/o/r/pull/20').map(e => e.id),
      ['b'],
    );
  });

  it('an unparseable string matches nothing', () => {
    assert.deepEqual(filterSessionsByPr(entries, 'not-a-pr'), []);
  });
});

describe('SessionManager.list — filterByPr + excludeId', () => {
  it('filters the listing by PR and reuses filterSessionsByPr semantics', () => {
    const dir = mkdir();
    const mgr = new SessionManager(dir, dir);
    mgr.save(session('s-pr-10', { pr: { number: 10, url: 'u10' } }));
    mgr.save(session('s-pr-20', { pr: { number: 20, url: 'u20' } }));
    mgr.save(session('s-no-pr'));

    assert.deepEqual(mgr.list({ filterByPr: true }).map(e => String(e.id)).sort(), ['s-pr-10', 's-pr-20']);
    assert.deepEqual(mgr.list({ filterByPr: 20 }).map(e => String(e.id)), ['s-pr-20']);
    assert.deepEqual(
      mgr.list({ filterByPr: 'https://github.com/o/r/pull/10' }).map(e => String(e.id)),
      ['s-pr-10'],
    );
    assert.deepEqual(mgr.list({ filterByPr: 'junk' }), []);
    // No filter -> all three.
    assert.equal(mgr.list().length, 3);
  });

  it('excludeId drops the live current session from its own picker', () => {
    const dir = mkdir();
    const mgr = new SessionManager(dir, dir);
    mgr.save(session('live-one'));
    mgr.save(session('other-two'));
    const ids = mgr.list({ excludeId: 'live-one' }).map(e => String(e.id));
    assert.ok(!ids.includes('live-one'));
    assert.ok(ids.includes('other-two'));
    // Without excludeId both appear.
    assert.equal(mgr.list().length, 2);
  });
});

describe('checkCrossProjectResume', () => {
  const sep = path.sep;

  it('returns not-cross-project when the scope toggle is off', () => {
    const r = checkCrossProjectResume({ id: 'x', projectPath: '/other/proj' }, '/cwd', [], false);
    assert.deepEqual(r, { isCrossProject: false });
  });

  it('returns not-cross-project with no recorded path or same cwd', () => {
    assert.deepEqual(checkCrossProjectResume({ id: 'x' }, '/cwd', [], true), { isCrossProject: false });
    assert.deepEqual(
      checkCrossProjectResume({ id: 'x', projectPath: '/cwd' }, '/cwd', [], true),
      { isCrossProject: false },
    );
  });

  it('flags a same-repo git worktree so it can resume in place (no cd)', () => {
    const wt = `${sep}repo${sep}wt-a`;
    const r = checkCrossProjectResume(
      { id: 'x', projectPath: `${wt}${sep}sub` },
      `${sep}repo${sep}main`,
      [wt],
      true,
    );
    assert.deepEqual(r, { isCrossProject: true, isSameRepoWorktree: true, projectPath: `${wt}${sep}sub` });
  });

  it('matches a worktree path exactly (not only as a prefix)', () => {
    const wt = `${sep}repo${sep}wt-a`;
    const r = checkCrossProjectResume({ id: 'x', projectPath: wt }, `${sep}elsewhere`, [wt], true);
    assert.equal(r.isCrossProject && r.isSameRepoWorktree, true);
  });

  it('builds a copyable cd && ami --resume command for a different project', () => {
    const r = checkCrossProjectResume(
      { id: 'sess 1', projectPath: `${sep}other${sep}it's a proj` },
      `${sep}cwd`,
      [`${sep}repo${sep}wt`],
      true,
    );
    assert.equal(r.isCrossProject, true);
    assert.equal(r.isCrossProject && r.isSameRepoWorktree, false);
    if (r.isCrossProject && !r.isSameRepoWorktree) {
      // Single-quoted + embedded quote escaped as '\'' ; session id also quoted.
      assert.equal(r.command, `cd '${sep}other${sep}it'\\''s a proj' && ami --resume 'sess 1'`);
      assert.equal(r.projectPath, `${sep}other${sep}it's a proj`);
    }
  });

  it('a prefix that is not a path boundary is NOT a same-repo worktree', () => {
    const wt = `${sep}repo${sep}wt`;
    // projectPath starts with the worktree string but not followed by a separator.
    const r = checkCrossProjectResume({ id: 'x', projectPath: `${wt}-sibling` }, `${sep}cwd`, [wt], true);
    assert.equal(r.isCrossProject && r.isSameRepoWorktree, false);
  });
});

describe('cleanupOldSessions strict content guard', () => {
  it('deletes only real expired session files, never a foreign .json', () => {
    const dir = mkdir();
    const mgr = new SessionManager(dir, dir);
    mgr.save(session('keep-me'));

    // An expired real session.
    const old = session('old-session');
    mgr.save(old);
    const oldPath = path.join(dir, 'old-session.json');
    const past = Date.now() - 100 * 24 * 60 * 60 * 1000;
    fs.utimesSync(oldPath, past / 1000, past / 1000);

    // A foreign user file that merely LOOKS like it belongs here, also "expired".
    const notesPath = path.join(dir, 'notes.json');
    fs.writeFileSync(notesPath, JSON.stringify({ hello: 'world' }));
    fs.utimesSync(notesPath, past / 1000, past / 1000);

    const removed = mgr.cleanupOldSessions(30);
    assert.equal(removed, 1); // only the real expired session
    assert.equal(fs.existsSync(oldPath), false);
    assert.equal(fs.existsSync(notesPath), true, 'foreign non-session json must survive cleanup');
    assert.ok(mgr.load('keep-me'), 'recent session is retained');
  });
});
