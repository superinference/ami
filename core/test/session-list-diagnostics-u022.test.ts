import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SessionManager, SESSION_SKIP_REASONS } from '../src/session';
import type { Session, SessionListDiagnostics } from '../src/session';

// Unit U022 / gap: references/claude/claude-code-main/src/utils/sessionStorage.ts
// "Resume-discovery diagnostics (how many sessions found/enriched/filtered and why
//  each was hidden)". AMI's SessionManager.list() silently dropped unreadable/filtered
// rows; listWithDiagnostics()/list({diagnostics}) now tallies found/listed/skipped-by-
// reason so a missing session is diagnosable. Parity: Claude's `/resume: found N,
// M filtered out, K visible` debug counts.

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-diag-'));
}

function writeSession(dir: string, id: string, patch: Partial<Session> = {}): void {
  const { messages, ...scalarPatch } = patch;
  // Serialize scalar metadata BEFORE `messages` (exactly as SessionManager.save does)
  // so the bounded lite-metadata read sees top-level scalars like `isSidechain` in the
  // head buffer rather than after the (ignored) messages array.
  const session: Session = {
    id: id as Session['id'],
    createdAt: '2026-01-01T00:00:00.000Z',
    updatedAt: '2026-01-01T00:00:00.000Z',
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
    ...scalarPatch,
    messages: messages ?? [{ role: 'user', content: `hello from ${id}` }],
  };
  fs.writeFileSync(path.join(dir, `${id}.json`), JSON.stringify(session));
}

describe('SessionManager resume-discovery diagnostics (U022)', () => {
  let base: string;
  let dir: string;

  beforeEach(() => {
    base = makeTempDir();
    dir = path.join(base, 'store');
    fs.mkdirSync(dir, { recursive: true });
  });

  afterEach(() => {
    fs.rmSync(base, { recursive: true, force: true });
  });

  it('counts every discovered file as found and returns listed === entries', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002');
    const sm = new SessionManager(base, dir);
    const { entries, diagnostics } = sm.listWithDiagnostics();
    assert.equal(diagnostics.found, 2, 'both files discovered');
    assert.equal(diagnostics.listed, 2, 'both visible');
    assert.equal(diagnostics.listed, entries.length, 'listed matches returned entries');
    assert.equal(diagnostics.skipped, 0, 'nothing skipped');
    assert.deepEqual(diagnostics.skippedByReason, {});
  });

  it('records an unreadable (empty) file under the `unreadable` reason', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    // A zero-byte file is treated as unreadable by the lite/full read guards.
    fs.writeFileSync(path.join(dir, 'aaaaaaaa-0000-4000-8000-000000000099.json'), '');
    const sm = new SessionManager(base, dir);
    const { entries, diagnostics } = sm.listWithDiagnostics();
    assert.equal(diagnostics.found, 2);
    assert.equal(entries.length, 1);
    assert.equal(diagnostics.listed, 1);
    assert.equal(diagnostics.skipped, 1);
    assert.equal(diagnostics.skippedByReason['unreadable'], 1);
  });

  it('attributes the current session exclusion to `current-session`', () => {
    const live = 'aaaaaaaa-0000-4000-8000-000000000001';
    writeSession(dir, live);
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002');
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ excludeId: live });
    assert.equal(diagnostics.found, 2);
    assert.equal(diagnostics.listed, 1);
    assert.equal(diagnostics.skippedByReason['current-session'], 1);
  });

  it('attributes a sidechain drop to `sidechain`', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { isSidechain: true });
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics();
    assert.equal(diagnostics.skippedByReason['sidechain'], 1);
    assert.equal(diagnostics.listed, 1);
  });

  it('attributes a branch-filter drop to `branch-filter`', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001', { gitBranch: 'main' });
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { gitBranch: 'feature' });
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ filterByBranch: 'main' });
    assert.equal(diagnostics.found, 2);
    assert.equal(diagnostics.listed, 1);
    assert.equal(diagnostics.skippedByReason['branch-filter'], 1);
  });

  it('attributes a tag-filter drop to `tag-filter`', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001', { tag: 'keep' });
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { tag: 'other' });
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ filterByTag: 'keep' });
    assert.equal(diagnostics.listed, 1);
    assert.equal(diagnostics.skippedByReason['tag-filter'], 1);
  });

  it('attributes a dropEmpty metadata-only session to `empty`', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    // A session with no meaningful prompt and no title → summary undefined.
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { messages: [] });
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ dropEmpty: true });
    assert.equal(diagnostics.skippedByReason['empty'], 1);
  });

  it('attributes a pr-filter drop to `pr-filter`', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001', {
      pr: { number: 7, url: 'https://github.com/o/r/pull/7' },
    });
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002');
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ filterByPr: 7 });
    assert.equal(diagnostics.listed, 1);
    assert.equal(diagnostics.skippedByReason['pr-filter'], 1);
  });

  it('attributes a same-id stale copy to `duplicate-id`', () => {
    const sharedId = 'aaaaaaaa-0000-4000-8000-000000000001';
    // Two files carry the SAME internal id (a manually-copied/stale duplicate); the
    // newer-by-mtime wins and the other is dropped as `duplicate-id`.
    writeSession(dir, sharedId);
    fs.writeFileSync(
      path.join(dir, 'aaaaaaaa-0000-4000-8000-000000000002.json'),
      JSON.stringify({
        id: sharedId,
        messages: [{ role: 'user', content: 'dup' }],
        createdAt: '2026-01-01T00:00:00.000Z',
        updatedAt: '2026-01-01T00:00:00.000Z',
        provider: { model: 'test-model', baseUrl: 'http://localhost' },
      }),
    );
    const sm = new SessionManager(base, dir);
    const { entries, diagnostics } = sm.listWithDiagnostics();
    assert.equal(diagnostics.found, 2);
    assert.equal(entries.length, 1, 'only one row for the shared id');
    assert.equal(diagnostics.skippedByReason['duplicate-id'], 1);
  });

  it('skipped always equals the sum of skippedByReason', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001', { isSidechain: true });
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { gitBranch: 'other' });
    fs.writeFileSync(path.join(dir, 'aaaaaaaa-0000-4000-8000-000000000099.json'), '');
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ filterByBranch: 'main' });
    const sum = Object.values(diagnostics.skippedByReason).reduce((a, b) => a + b, 0);
    assert.equal(diagnostics.skipped, sum, 'skipped is the total of per-reason counts');
    // Every recorded reason is a known skip reason.
    for (const reason of Object.keys(diagnostics.skippedByReason)) {
      assert.ok((SESSION_SKIP_REASONS as readonly string[]).includes(reason), `${reason} is a known reason`);
    }
  });

  it('records skip reasons on the cheap lite-read path too (enrichCount: 0)', () => {
    // enrichCount:0 forces every row through the bounded lite-metadata read, so the
    // lite branch's skip accounting is exercised, not just the full-parse branch.
    const live = 'aaaaaaaa-0000-4000-8000-000000000001';
    writeSession(dir, live, { customTitle: 'keep me' });
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002', { customTitle: 'other', isSidechain: true });
    fs.writeFileSync(path.join(dir, 'aaaaaaaa-0000-4000-8000-000000000098.json'), '');
    const sm = new SessionManager(base, dir);
    const { entries, diagnostics } = sm.listWithDiagnostics({ enrichCount: 0, excludeId: live });
    assert.equal(diagnostics.found, 3);
    // live excluded (current-session), one sidechain, one empty file (unreadable).
    assert.equal(entries.length, 0);
    assert.equal(diagnostics.skippedByReason['current-session'], 1);
    assert.equal(diagnostics.skippedByReason['sidechain'], 1);
    assert.equal(diagnostics.skippedByReason['unreadable'], 1);
  });

  it('records a dropEmpty lite row (no title) as `empty` (enrichCount: 0)', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001', { customTitle: 'titled' });
    // No title → lite summary undefined → dropEmpty skips it as `empty`.
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002');
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ enrichCount: 0, dropEmpty: true });
    assert.equal(diagnostics.skippedByReason['empty'], 1);
  });

  it('records a duplicate-id lite row (enrichCount: 0)', () => {
    const sharedId = 'aaaaaaaa-0000-4000-8000-000000000001';
    writeSession(dir, sharedId, { customTitle: 'a' });
    fs.writeFileSync(
      path.join(dir, 'aaaaaaaa-0000-4000-8000-000000000002.json'),
      JSON.stringify({
        id: sharedId,
        customTitle: 'b',
        messages: [{ role: 'user', content: 'dup' }],
        createdAt: '2026-01-01T00:00:00.000Z',
        updatedAt: '2026-01-01T00:00:00.000Z',
        provider: { model: 'test-model', baseUrl: 'http://localhost' },
      }),
    );
    const sm = new SessionManager(base, dir);
    const { diagnostics } = sm.listWithDiagnostics({ enrichCount: 0 });
    assert.equal(diagnostics.skippedByReason['duplicate-id'], 1);
  });

  it('list({diagnostics}) returns exactly the same entries as listWithDiagnostics', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000002');
    const sm = new SessionManager(base, dir);
    const sink: SessionListDiagnostics = { found: 0, listed: 0, skipped: 0, skippedByReason: {} };
    const viaList = sm.list({ diagnostics: sink });
    const viaWrapper = sm.listWithDiagnostics();
    assert.deepEqual(viaList.map((e) => e.id), viaWrapper.entries.map((e) => e.id));
    assert.equal(sink.listed, viaWrapper.diagnostics.listed);
    assert.equal(sink.found, viaWrapper.diagnostics.found);
  });

  it('leaves list() behavior unchanged when no diagnostics sink is passed', () => {
    writeSession(dir, 'aaaaaaaa-0000-4000-8000-000000000001');
    const sm = new SessionManager(base, dir);
    const entries = sm.list();
    assert.equal(entries.length, 1);
  });
});
