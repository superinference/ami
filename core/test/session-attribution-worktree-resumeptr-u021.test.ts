import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  matchWorktreeForProjectDir,
  sha256Hex,
  recordFileContribution,
  agentContributionRatio,
  writeResumePointer,
  readResumePointer,
  clearResumePointer,
  RESUME_POINTER_FILE,
  RESUME_POINTER_TTL_MS,
  SessionManager,
  type FileAttributionState,
} from '../src/session';
import { projectSlug } from '../src/reference-journal';

function tmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

// ---------------------------------------------------------------------------
// Gap 6 — worktree-aware longest-prefix, truncation-aware project-dir matching
// ---------------------------------------------------------------------------

test('matchWorktreeForProjectDir: exact slug match returns the worktree cwd', () => {
  const cwd = '/home/u/repo';
  assert.equal(matchWorktreeForProjectDir(projectSlug(cwd), [cwd]), cwd);
});

test('matchWorktreeForProjectDir: no candidate matches → null', () => {
  assert.equal(matchWorktreeForProjectDir(projectSlug('/home/u/repo'), ['/home/u/other']), null);
});

test('matchWorktreeForProjectDir: longest slug wins over a shorter ancestor', () => {
  const parent = '/home/u/repo';
  const child = '/home/u/repo/packages/app';
  // The dir name is the child slug; both are candidates. Most specific must win.
  const matched = matchWorktreeForProjectDir(projectSlug(child), [parent, child]);
  assert.equal(matched, child);
});

test('matchWorktreeForProjectDir: a short (non-truncated) slug never prefix-matches a longer dir', () => {
  // projectSlug('/home/u/project') must NOT match the dir for '/home/u/project-foo'.
  const base = '/home/u/project';
  const sibling = '/home/u/project-foo';
  assert.equal(matchWorktreeForProjectDir(projectSlug(sibling), [base]), null);
});

test('matchWorktreeForProjectDir: a truncated slug matches via startsWith(slug + "-")', () => {
  // A very long cwd produces a truncated slug (length >= MAX_SLUG_LENGTH, hash
  // suffix). Its own dir name equals the slug → exact match still holds.
  const longCwd = '/home/u/' + 'x'.repeat(400);
  const slug = projectSlug(longCwd);
  assert.ok(slug.length >= 200, 'precondition: slug is truncated');
  assert.equal(matchWorktreeForProjectDir(slug, [longCwd]), longCwd);
});

test('SessionManager.listWorktreeSessionDirs + listAcrossProjects scope to worktree cwds', () => {
  const root = tmp('ami-wt-root-');
  const env = { SUPERINFERENCE_DATA_DIR: root } as NodeJS.ProcessEnv;
  const projectsRoot = path.join(root, 'projects');

  const cwdA = '/home/u/repoA';
  const cwdB = '/home/u/repoB';
  const cwdC = '/home/u/repoC';
  // Create a session store for each project under the global projects root.
  for (const cwd of [cwdA, cwdB, cwdC]) {
    const dir = path.join(projectsRoot, projectSlug(cwd), 'sessions');
    fs.mkdirSync(dir, { recursive: true });
    const mgr = new SessionManager('', dir);
    mgr.save({
      id: SessionManager.newId(),
      messages: [{ role: 'user', content: `hello from ${cwd}` } as any],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      provider: { model: 'm', baseUrl: 'u' },
    });
  }

  const dirs = SessionManager.listWorktreeSessionDirs([cwdA, cwdB], env);
  assert.equal(dirs.length, 2, 'only A and B stores selected, not C');
  assert.ok(dirs.some(d => d.includes(projectSlug(cwdA))));
  assert.ok(dirs.some(d => d.includes(projectSlug(cwdB))));
  assert.ok(!dirs.some(d => d.includes(projectSlug(cwdC))));

  const scoped = SessionManager.listAcrossProjects({ worktreePaths: [cwdA, cwdB], env });
  assert.equal(scoped.length, 2);
  const everything = SessionManager.listAcrossProjects({ env });
  assert.equal(everything.length, 3, 'omitting worktreePaths scans every project');

  fs.rmSync(root, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap 3 — per-file agent-attribution snapshots
// ---------------------------------------------------------------------------

test('sha256Hex is a stable 64-char hex of the UTF-8 content', () => {
  const h = sha256Hex('abc');
  assert.equal(h, 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  assert.match(h, /^[0-9a-f]{64}$/);
});

test('recordFileContribution accumulates authored chars and refreshes hash/mtime (pure)', () => {
  const s0: Record<string, FileAttributionState> = {};
  const s1 = recordFileContribution(s0, '/a.ts', 'hello', 5, 100);
  assert.equal(s1['/a.ts'].agentContribution, 5);
  assert.equal(s1['/a.ts'].contentHash, sha256Hex('hello'));
  assert.equal(s1['/a.ts'].mtime, 100);
  // Input map not mutated.
  assert.deepEqual(s0, {});

  const s2 = recordFileContribution(s1, '/a.ts', 'hello world', 6, 200);
  assert.equal(s2['/a.ts'].agentContribution, 11, 'contributions accumulate across edits');
  assert.equal(s2['/a.ts'].contentHash, sha256Hex('hello world'));
  assert.equal(s2['/a.ts'].mtime, 200);
});

test('recordFileContribution: a non-positive contribution refreshes hash/mtime without inflating', () => {
  const s1 = recordFileContribution({}, '/a.ts', 'abcd', 4, 1);
  const s2 = recordFileContribution(s1, '/a.ts', 'ab', -2, 2); // a delete-only edit
  assert.equal(s2['/a.ts'].agentContribution, 4, 'count unchanged by a non-positive edit');
  assert.equal(s2['/a.ts'].contentHash, sha256Hex('ab'), 'hash still refreshed to new content');
  assert.equal(s2['/a.ts'].mtime, 2);
});

test('agentContributionRatio clamps to [0,1] and is 0 for an empty file', () => {
  assert.equal(agentContributionRatio({ contentHash: 'x', agentContribution: 50, mtime: 0 }, 100), 0.5);
  assert.equal(agentContributionRatio({ contentHash: 'x', agentContribution: 200, mtime: 0 }, 100), 1, 'clamped at 1');
  assert.equal(agentContributionRatio({ contentHash: 'x', agentContribution: 0, mtime: 0 }, 0), 0, 'empty file → 0');
});

// ---------------------------------------------------------------------------
// Gap 11 — crash-recovery resume pointer (mtime TTL + read-time GC)
// ---------------------------------------------------------------------------

test('resume pointer: write → read round-trips and reports a small age', () => {
  const dir = tmp('ami-rp-');
  const id = SessionManager.newId();
  writeResumePointer(dir, { sessionId: String(id), surface: 'cli' });
  const got = readResumePointer(dir);
  assert.ok(got);
  assert.equal(got!.sessionId, String(id));
  assert.equal(got!.surface, 'cli');
  assert.ok(got!.ageMs >= 0 && got!.ageMs < RESUME_POINTER_TTL_MS);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('resume pointer: missing pointer reads as null', () => {
  const dir = tmp('ami-rp-');
  assert.equal(readResumePointer(dir), null);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('resume pointer: a stale pointer (age > TTL) is GC-deleted on read and yields null', () => {
  const dir = tmp('ami-rp-');
  writeResumePointer(dir, { sessionId: String(SessionManager.newId()) });
  const file = path.join(dir, RESUME_POINTER_FILE);
  assert.ok(fs.existsSync(file));
  // Inject a clock far in the future so the pointer reads as stale.
  const future = Date.now() + RESUME_POINTER_TTL_MS + 60_000;
  assert.equal(readResumePointer(dir, () => future), null);
  assert.ok(!fs.existsSync(file), 'stale pointer deleted on read');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('resume pointer: a corrupt / foreign pointer is GC-deleted on read', () => {
  const dir = tmp('ami-rp-');
  const file = path.join(dir, RESUME_POINTER_FILE);
  fs.writeFileSync(file, '{ not valid json');
  assert.equal(readResumePointer(dir), null);
  assert.ok(!fs.existsSync(file), 'corrupt pointer deleted on read');
  // A structurally-valid JSON but invalid sessionId is also rejected + GC'd.
  fs.writeFileSync(file, JSON.stringify({ sessionId: '../escape' }));
  assert.equal(readResumePointer(dir), null);
  assert.ok(!fs.existsSync(file));
  fs.rmSync(dir, { recursive: true, force: true });
});

test('resume pointer: a malformed sessionId is never persisted', () => {
  const dir = tmp('ami-rp-');
  writeResumePointer(dir, { sessionId: '../evil' });
  assert.ok(!fs.existsSync(path.join(dir, RESUME_POINTER_FILE)));
  fs.rmSync(dir, { recursive: true, force: true });
});

test('resume pointer: clearResumePointer removes a live pointer (clean exit)', () => {
  const dir = tmp('ami-rp-');
  writeResumePointer(dir, { sessionId: String(SessionManager.newId()) });
  assert.ok(fs.existsSync(path.join(dir, RESUME_POINTER_FILE)));
  clearResumePointer(dir);
  assert.equal(readResumePointer(dir), null);
  clearResumePointer(dir); // idempotent — missing file is a no-op
  fs.rmSync(dir, { recursive: true, force: true });
});

test('SessionManager resume-pointer methods mark/read/clear via the store dir', () => {
  const dir = tmp('ami-rp-mgr-');
  const mgr = new SessionManager('', dir);
  const id = SessionManager.newId();
  mgr.markResumable(id, 'detached');
  const got = mgr.resumePointer();
  assert.ok(got);
  assert.equal(got!.sessionId, String(id));
  assert.equal(got!.surface, 'detached');
  mgr.clearResumable();
  assert.equal(mgr.resumePointer(), null);
  fs.rmSync(dir, { recursive: true, force: true });
});
