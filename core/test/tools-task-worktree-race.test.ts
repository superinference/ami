import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import {
  taskTool,
  shouldRemoveWorktree,
  resetAgentRegistry,
} from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

function gen(events: any[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

// ===========================================================================
// Pure helper — shouldRemoveWorktree
// Covers every short-circuit arm of the `&&` chain.
// ===========================================================================

describe('shouldRemoveWorktree', () => {
  it('removes only when clean, pushed, and no background agent is running', () => {
    assert.equal(shouldRemoveWorktree('', '', 0), true);
  });

  it('keeps the worktree when there are uncommitted/untracked changes', () => {
    assert.equal(shouldRemoveWorktree('?? new-file.ts', '', 0), false);
  });

  it('keeps the worktree when there are unpushed commits', () => {
    assert.equal(shouldRemoveWorktree('', 'abc123', 0), false);
  });

  it('keeps the worktree while a background agent is still running in it', () => {
    assert.equal(shouldRemoveWorktree('', '', 1), false);
    assert.equal(shouldRemoveWorktree('', '', 3), false);
  });
});

// ===========================================================================
// Integration reproducer — the worktree-race bug.
// A foreground isolated subagent returns while a background subagent it
// started is still live in the same worktree. The clean-worktree cleanup must
// be deferred: the worktree must NOT be removed while runningAgentCount > 0.
// ===========================================================================

describe('taskTool — worktree not removed while a background agent is live', () => {
  beforeEach(() => resetAgentRegistry());

  it('defers cleanup of a change-free worktree when a background agent is still running', async () => {
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-race-'));
    const remote = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-race-remote-'));
    const g = (args: string, cwd = repo) =>
      child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: 'pipe' });
    try {
      child_process.execSync('git init -q -b main', { cwd: repo });
      g('config user.email t@t.t');
      g('config user.name t');
      fs.writeFileSync(path.join(repo, 'a.txt'), 'hi\n');
      g('add -A');
      g('commit -qm init');
      child_process.execSync('git init -q --bare', { cwd: remote });
      g(`remote add origin "${remote}"`);
      g('push -q origin main');

      // Simulate a background subagent still working inside the worktree.
      const pm = new ProcessManager(repo);
      pm.registerAgent('agent-bg', {
        command: '[agent] background worker',
        description: 'Background agent: background worker',
        outputPath: path.join(repo, 'bg.output'),
        abortFn: () => {},
      });
      assert.equal(pm.runningAgentCount(), 1);

      let runCwd = '';
      const r = await taskTool.execute(
        { prompt: 'parent isolated run', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          processManager: pm,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            runCwd = cfg.cwd;
            return { submit: gen([{ type: 'text_delta', text: 'done' }]) };
          },
        } as any),
      );

      assert.ok(!r.isError, r.output);
      assert.ok(runCwd.includes(path.join('.superinference', 'worktrees')),
        'subagent must run inside the worktree path');
      // DISCRIMINATING: on current source the worktree is removed because the
      // cleanup only checks git cleanliness, ignoring runningAgentCount().
      assert.ok(!r.output.includes('worktree cleaned up'),
        'worktree must NOT be cleaned up while a background agent is still running');
      assert.ok(fs.existsSync(runCwd),
        'the worktree directory must survive while a background agent is live in it');
    } finally {
      try { child_process.execSync('git worktree prune', { cwd: repo, stdio: 'ignore' }); } catch {}
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });
});
