import { describe, it, beforeEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  taskTool,
  resetAgentRegistry,
  SUBAGENT_RETURN_DIRECTIVE,
  FORK_NO_SUBSPAWN_DIRECTIVE,
  FORK_WORKER_DIRECTIVE,
} from '../src/tools/task';
import type { ToolContext } from '../src/types';

function gen(events: Array<Record<string, unknown>>) {
  return async function* (_p: string) {
    for (const e of events) yield e as never;
  };
}

/** Run the task tool capturing the submitted prompt AND the sub-engine config. */
async function run(
  input: Record<string, unknown>,
  overrides: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; submitted: string; cfg: { cwd?: string } }>
{
  let submitted = '';
  let cfg: { cwd?: string } = {};
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c: { cwd?: string }) => {
      cfg = c;
      return { submit: (p: string) => { submitted = p; return gen([{ type: 'text_delta', text: 'done' }])(p); } };
    },
    ...overrides,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, submitted, cfg };
}

// ===========================================================================
// Gap (forkSubagent.ts:L171-198): a `fork` child gets Claude's structured
// worker-discipline contract (fixed Scope/Result/Key files/Files changed+hash/
// Issues report schema, begin-with-"Scope:", commit-hash reporting, <500 words,
// no text between tool calls) — not just the generic return directive.
// ===========================================================================
describe('taskTool — fork structured worker directive', () => {
  beforeEach(() => resetAgentRegistry());

  it('fork child first turn carries the structured worker contract + schema', async () => {
    const { submitted } = await run({ prompt: 'refactor the parser', mode: 'fork' });
    // Regression: the prompt still LEADS with the generic return directive and
    // the no-subspawn recursion guard (pre-existing fork bounds).
    assert.ok(submitted.startsWith(SUBAGENT_RETURN_DIRECTIVE), 'return directive still leads');
    assert.ok(submitted.includes(FORK_NO_SUBSPAWN_DIRECTIVE), 'no-subspawn guard still present');
    // New: the full structured worker directive is stapled on.
    assert.ok(submitted.includes(FORK_WORKER_DIRECTIVE), 'fork worker directive stapled');
    // Spot-check the load-bearing report-schema markers the parent relies on.
    for (const marker of ['Scope:', 'Result:', 'Key files:', 'Files changed:', 'Issues:', 'commit hash', '500 words']) {
      assert.ok(submitted.includes(marker), `fork directive mentions "${marker}"`);
    }
    assert.ok(submitted.includes('refactor the parser'), 'task text preserved');
  });

  it('the structured worker directive mentions the no-subspawn rule only ONCE (no duplication)', () => {
    // The worker directive deliberately omits the no-subspawn rule (owned by
    // FORK_NO_SUBSPAWN_DIRECTIVE) so the two do not duplicate guidance.
    assert.ok(!/do not spawn sub-agents/i.test(FORK_WORKER_DIRECTIVE), 'worker directive does not restate the no-subspawn rule');
  });

  it('a non-fork (general) spawn does NOT get the fork worker directive', async () => {
    const { submitted } = await run({ prompt: 'do a thing', mode: 'general' });
    assert.ok(submitted.includes('do a thing'), 'general spawn still carries the task text');
    assert.ok(!submitted.includes(FORK_WORKER_DIRECTIVE), 'general mode must not carry the fork worker directive');
  });
});

// ===========================================================================
// Gap (task.ts resume branch): resume restores the subagent's prior isolated
// worktree / cwd (so the follow-up turn continues on the same files), with a
// stat-revalidation + parent-cwd fallback (never crash on a stale worktree) and
// an mtime bump to defeat stale-worktree cleanup.
// ===========================================================================
describe('taskTool — resume restores + revalidates the prior worktree cwd', () => {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'u011-wt-'));
  const sub = path.join(tmpRoot, 'work');

  beforeEach(() => {
    resetAgentRegistry();
    fs.rmSync(sub, { recursive: true, force: true });
    fs.mkdirSync(sub, { recursive: true });
  });
  after(() => fs.rmSync(tmpRoot, { recursive: true, force: true }));

  it('records the spawn cwd and restores it (with an mtime bump) on resume', async () => {
    const spawn = await run(
      { prompt: 'start', name: 'w', cwd: sub },
      { cwd: tmpRoot },
    );
    assert.ok(!spawn.isError, spawn.output);
    assert.equal(spawn.cfg.cwd, sub, 'spawn runs in the requested cwd');

    const before = fs.statSync(sub).mtimeMs;
    // Guarantee an observable mtime delta regardless of fs timestamp resolution.
    const old = new Date(before - 60_000);
    fs.utimesSync(sub, old, old);

    const resumed = await run({ prompt: 'keep going', resume: 'w' }, { cwd: tmpRoot });
    assert.ok(!resumed.isError, resumed.output);
    assert.equal(resumed.cfg.cwd, sub, 'resume restores the prior worktree cwd instead of the parent cwd');
    assert.ok(fs.statSync(sub).mtimeMs > before - 60_000, 'resume bumps the worktree mtime');
    assert.ok(!resumed.output.includes('no longer exists'), 'no fallback note when the worktree still exists');
  });

  it('falls back to the parent cwd (no crash) with a note when the worktree was removed', async () => {
    const spawn = await run({ prompt: 'start', name: 'w', cwd: sub }, { cwd: tmpRoot });
    assert.ok(!spawn.isError, spawn.output);
    // Simulate a stale-worktree cleanup removing the directory between turns.
    fs.rmSync(sub, { recursive: true, force: true });

    const resumed = await run({ prompt: 'keep going', resume: 'w' }, { cwd: tmpRoot });
    assert.ok(!resumed.isError, `resume must not crash on a removed worktree: ${resumed.output}`);
    assert.equal(resumed.cfg.cwd, tmpRoot, 'resume falls back to the parent cwd');
    assert.ok(resumed.output.includes('no longer exists'), 'resume surfaces a fallback introspection note');
  });

  it('an explicit cwd on resume overrides the restored worktree', async () => {
    await run({ prompt: 'start', name: 'w', cwd: sub }, { cwd: tmpRoot });
    const other = path.join(tmpRoot, 'other');
    fs.mkdirSync(other, { recursive: true });
    const resumed = await run({ prompt: 'keep going', resume: 'w', cwd: other }, { cwd: tmpRoot });
    assert.ok(!resumed.isError, resumed.output);
    assert.equal(resumed.cfg.cwd, other, 'an explicit cwd wins over the restored worktree');
  });
});
