import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  recordAgentFileContribution,
  recordBulkAgentFileContributions,
  getSessionFileAttribution,
  recordUserPrompt,
  recordPermissionPrompt,
  recordEscapeCancel,
  getSessionPromptCount,
  getSessionEffort,
  computeCommitEffort,
  advanceEffortBaselines,
  setStartingHeadSha,
  getStartingHeadSha,
  buildAttributionSnapshot,
  restoreAttributionStore,
} from '../src/tools/attribution-store';
import { gitCommitTool, ATTRIBUTION_NOTES_REF } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

let counter = 0;
function sid(): string {
  return `u264-w114-${process.pid}-${++counter}-${Math.random()}`;
}

// ---------------------------------------------------------------------------
// Gap #2 — bulk file-change tracking with a single map copy (O(n), not O(n²))
// ---------------------------------------------------------------------------

describe('U264/w114 — recordBulkAgentFileContributions', () => {
  it('accumulates every change in a single call with correct per-file totals', () => {
    const s = sid();
    recordBulkAgentFileContributions(s, [
      { path: 'a.ts', content: 'aaaa', contributedChars: 4, mtime: 1 },
      { path: 'b.ts', content: 'bb', contributedChars: 2, mtime: 2 },
      { path: 'a.ts', content: 'aaaaaa', contributedChars: 2, mtime: 3 },
    ]);
    const store = getSessionFileAttribution(s);
    // a.ts was touched twice in the one bulk call → contributions accumulate.
    assert.equal(store['a.ts'].agentContribution, 6);
    assert.equal(store['a.ts'].mtime, 3);
    assert.equal(store['b.ts'].agentContribution, 2);
  });

  it('is a no-op for an empty change list (no needless copy / no bucket created)', () => {
    const s = sid();
    recordBulkAgentFileContributions(s, []);
    assert.deepEqual(getSessionFileAttribution(s), {});
  });

  it('accumulates across successive bulk calls (running total survives)', () => {
    const s = sid();
    recordBulkAgentFileContributions(s, [{ path: 'x.ts', content: 'xx', contributedChars: 2, mtime: 1 }]);
    recordBulkAgentFileContributions(s, [{ path: 'x.ts', content: 'xxxx', contributedChars: 2, mtime: 2 }]);
    assert.equal(getSessionFileAttribution(s)['x.ts'].agentContribution, 4);
  });

  it('a deletion-only change (<=0 chars) refreshes hash/mtime without inflating', () => {
    const s = sid();
    recordBulkAgentFileContributions(s, [{ path: 'd.ts', content: 'kept', contributedChars: 5, mtime: 1 }]);
    recordBulkAgentFileContributions(s, [{ path: 'd.ts', content: 'ke', contributedChars: 0, mtime: 9 }]);
    const st = getSessionFileAttribution(s)['d.ts'];
    assert.equal(st.agentContribution, 5); // not inflated by the deletion
    assert.equal(st.mtime, 9); // but mtime refreshed
  });

  it('keeps sessions isolated (one session\'s bulk write does not leak into another)', () => {
    const a = sid();
    const b = sid();
    recordBulkAgentFileContributions(a, [{ path: 'f.ts', content: 'f', contributedChars: 1, mtime: 1 }]);
    assert.deepEqual(getSessionFileAttribution(b), {});
  });

  it('recordAgentFileContribution delegates to the bulk path (single-file still works)', () => {
    const s = sid();
    recordAgentFileContribution(s, 'g.ts', 'ggg', 3, 7);
    assert.equal(getSessionFileAttribution(s)['g.ts'].agentContribution, 3);
    assert.equal(getSessionFileAttribution(s)['g.ts'].mtime, 7);
  });
});

// ---------------------------------------------------------------------------
// Gap #1 — session-effort schema fields + per-commit baselines + startingHeadSha
// ---------------------------------------------------------------------------

describe('U264/w114 — session effort counters', () => {
  it('counts prompts, permission prompts and escapes independently', () => {
    const s = sid();
    recordUserPrompt(s);
    recordUserPrompt(s);
    recordPermissionPrompt(s);
    recordEscapeCancel(s);
    recordEscapeCancel(s);
    recordEscapeCancel(s);
    assert.equal(getSessionPromptCount(s), 2);
    const e = getSessionEffort(s);
    assert.equal(e.promptCount, 2);
    assert.equal(e.permissionPromptCount, 1);
    assert.equal(e.escapeCount, 3);
  });

  it('returns zeroed effort for an untouched session', () => {
    const e = getSessionEffort(sid());
    assert.deepEqual(e, {
      promptCount: 0,
      promptCountAtLastCommit: 0,
      permissionPromptCount: 0,
      permissionPromptCountAtLastCommit: 0,
      escapeCount: 0,
      escapeCountAtLastCommit: 0,
      startingHeadSha: null,
    });
  });

  it('getSessionEffort returns a COPY — mutating it cannot corrupt the store', () => {
    const s = sid();
    recordUserPrompt(s);
    const e = getSessionEffort(s);
    e.promptCount = 999;
    assert.equal(getSessionPromptCount(s), 1);
  });

  it('computeCommitEffort reports deltas since the last commit, clamped at 0', () => {
    const s = sid();
    recordUserPrompt(s);
    recordUserPrompt(s);
    recordUserPrompt(s);
    recordPermissionPrompt(s);
    recordEscapeCancel(s);
    assert.deepEqual(computeCommitEffort(s), { steers: 3, permissionPrompts: 1, escapes: 1 });
  });

  it('advanceEffortBaselines makes the NEXT commit measure only post-commit effort', () => {
    const s = sid();
    recordUserPrompt(s);
    recordUserPrompt(s);
    recordUserPrompt(s);
    assert.equal(computeCommitEffort(s).steers, 3); // first commit: 3 steers
    advanceEffortBaselines(s);
    assert.equal(computeCommitEffort(s).steers, 0); // nothing since
    recordUserPrompt(s);
    recordUserPrompt(s);
    // Second commit sees ONLY the 2 prompts since the baseline advance, not the
    // cumulative 5 — the core of Claude Code's per-commit steer accounting.
    assert.equal(computeCommitEffort(s).steers, 2);
  });

  it('setStartingHeadSha is idempotent (first non-empty wins; empty ignored)', () => {
    const s = sid();
    assert.equal(getStartingHeadSha(s), null);
    setStartingHeadSha(s, ''); // empty never sticks (repo with no commits yet)
    assert.equal(getStartingHeadSha(s), null);
    setStartingHeadSha(s, 'sha-first');
    setStartingHeadSha(s, 'sha-second'); // ignored — the session start is fixed
    assert.equal(getStartingHeadSha(s), 'sha-first');
  });
});

// ---------------------------------------------------------------------------
// Gap #3 — snapshot build + last-snapshot-only restore (quadratic-avoidance)
// ---------------------------------------------------------------------------

describe('U264/w114 — attribution snapshot build + restore', () => {
  it('builds a full-state snapshot carrying fileStates + effort counters', () => {
    const s = sid();
    recordAgentFileContribution(s, 'a.ts', 'abc', 3, 1);
    recordUserPrompt(s);
    recordPermissionPrompt(s);
    recordEscapeCancel(s);
    const snap = buildAttributionSnapshot(s, 'msg-1', 'cli');
    assert.equal(snap.type, 'attribution-snapshot');
    assert.equal(snap.messageId, 'msg-1');
    assert.equal(snap.surface, 'cli');
    assert.equal(snap.fileStates['a.ts'].agentContribution, 3);
    assert.equal(snap.promptCount, 1);
    assert.equal(snap.permissionPromptCount, 1);
    assert.equal(snap.escapeCount, 1);
  });

  it('snapshot fileStates are a copy — later store mutation does not alter it', () => {
    const s = sid();
    recordAgentFileContribution(s, 'a.ts', 'ab', 2, 1);
    const snap = buildAttributionSnapshot(s, 'm', 'cli');
    recordAgentFileContribution(s, 'b.ts', 'zz', 2, 1); // mutate the live store
    assert.equal(Object.prototype.hasOwnProperty.call(snap.fileStates, 'b.ts'), false);
  });

  it('restoreAttributionStore([]) returns false and changes nothing', () => {
    const s = sid();
    assert.equal(restoreAttributionStore(s, []), false);
    assert.deepEqual(getSessionFileAttribution(s), {});
  });

  it('restore reads ONLY the last snapshot — never sums across snapshots (quadratic bug)', () => {
    const src = sid();
    // Simulate 100 full-state snapshots: the per-file count GROWS each time (full
    // dump, not a delta). A buggy summing restore would yield 1+2+...+100 chars.
    const snapshots = [];
    for (let i = 1; i <= 100; i++) {
      recordAgentFileContribution(src, 'big.ts', 'x'.repeat(i), 1, i);
      snapshots.push(buildAttributionSnapshot(src, `m${i}`, 'cli'));
    }
    const lastValue = getSessionFileAttribution(src)['big.ts'].agentContribution;
    assert.equal(lastValue, 100); // one char per edit, accumulated to 100

    const target = sid();
    assert.equal(restoreAttributionStore(target, snapshots), true);
    // Must equal the LAST snapshot's value (100), NOT the sum of all snapshots (5050).
    assert.equal(getSessionFileAttribution(target)['big.ts'].agentContribution, 100);
  });

  it('round-trips the full store (files + effort) into a fresh session', () => {
    const src = sid();
    recordAgentFileContribution(src, 'r.ts', 'hello', 5, 2);
    recordUserPrompt(src);
    recordUserPrompt(src);
    recordPermissionPrompt(src);
    advanceEffortBaselines(src); // set a non-zero baseline too
    recordUserPrompt(src);
    const snap = buildAttributionSnapshot(src, 'm', 'detached');

    const dst = sid();
    restoreAttributionStore(dst, [snap]);
    assert.equal(getSessionFileAttribution(dst)['r.ts'].agentContribution, 5);
    const e = getSessionEffort(dst);
    assert.equal(e.promptCount, 3);
    assert.equal(e.promptCountAtLastCommit, 2);
    assert.equal(e.permissionPromptCount, 1);
    // computeCommitEffort on the restored session reflects the restored baselines.
    assert.equal(computeCommitEffort(dst).steers, 1);
  });

  it('restore preserves a pre-set startingHeadSha (not carried in the snapshot schema)', () => {
    const dst = sid();
    setStartingHeadSha(dst, 'head-abc');
    const snap = buildAttributionSnapshot(sid(), 'm', 'cli');
    restoreAttributionStore(dst, [snap]);
    assert.equal(getStartingHeadSha(dst), 'head-abc');
  });
});

// ---------------------------------------------------------------------------
// Gap #1 integration (e2e) — git_commit surfaces the PER-COMMIT steer count
// ---------------------------------------------------------------------------

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '', GIT_CONFIG_NOSYSTEM: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}

const dirs: string[] = [];
function mkRepo(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

function ctx(cwd: string, sessionId: string): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, sessionId };
}

function noteFor(dir: string): Record<string, unknown> {
  const sha = git(dir, 'rev-parse', 'HEAD');
  return JSON.parse(git(dir, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', sha));
}

describe('U264/w114 — git_commit reports per-commit steers, not a cumulative total (e2e)', () => {
  it('each commit records only the prompts submitted since the previous commit', async () => {
    const dir = mkRepo('u264-steers-');
    const s = sid();
    const prev = process.env.AMI_COMMIT_ATTRIBUTION_NOTES;
    process.env.AMI_COMMIT_ATTRIBUTION_NOTES = '1';
    try {
      // 3 prompts before the first commit.
      recordUserPrompt(s);
      recordUserPrompt(s);
      recordUserPrompt(s);
      fs.writeFileSync(path.join(dir, 'a.txt'), 'first\n');
      let res = await gitCommitTool.execute({ message: 'first', files: ['a.txt'] }, ctx(dir, s));
      assert.equal(res.isError, false, res.output);
      assert.equal(noteFor(dir).promptCount, 3, 'first commit → 3-shotted');

      // 2 more prompts before the second commit.
      recordUserPrompt(s);
      recordUserPrompt(s);
      fs.writeFileSync(path.join(dir, 'b.txt'), 'second\n');
      res = await gitCommitTool.execute({ message: 'second', files: ['b.txt'] }, ctx(dir, s));
      assert.equal(res.isError, false, res.output);
      // Per-commit: 2 (prompts since the last commit), NOT the cumulative 5. This is
      // the red-green discriminator vs. the old getSessionPromptCount behaviour.
      assert.equal(noteFor(dir).promptCount, 2, 'second commit → per-commit 2-shotted, not 5');
    } finally {
      if (prev === undefined) delete process.env.AMI_COMMIT_ATTRIBUTION_NOTES;
      else process.env.AMI_COMMIT_ATTRIBUTION_NOTES = prev;
    }
  });

  it('captures the session starting HEAD on the first commit for external-commit detection', async () => {
    const dir = mkRepo('u264-head-');
    const s = sid();
    const prev = process.env.AMI_COMMIT_ATTRIBUTION_NOTES;
    process.env.AMI_COMMIT_ATTRIBUTION_NOTES = '1';
    try {
      // Seed an initial commit so HEAD resolves before the tool runs.
      fs.writeFileSync(path.join(dir, 'seed.txt'), 'seed\n');
      git(dir, 'add', 'seed.txt');
      git(dir, 'commit', '-q', '-m', 'seed');
      const seededHead = git(dir, 'rev-parse', 'HEAD');

      fs.writeFileSync(path.join(dir, 'c.txt'), 'c\n');
      const res = await gitCommitTool.execute({ message: 'c', files: ['c.txt'] }, ctx(dir, s));
      assert.equal(res.isError, false, res.output);
      // startingHeadSha is the pre-commit HEAD (the seed), fixed for the session.
      assert.equal(getStartingHeadSha(s), seededHead);
    } finally {
      if (prev === undefined) delete process.env.AMI_COMMIT_ATTRIBUTION_NOTES;
      else process.env.AMI_COMMIT_ATTRIBUTION_NOTES = prev;
    }
  });
});
