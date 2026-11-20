import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { projectStateDir } from '../src/reference-journal';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

/** Create a clean, pushed git repo so an isolated worktree off it is removable. */
function makePushedRepo(): { repo: string; remote: string } {
  const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011w67-'));
  const remote = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011w67-remote-'));
  const g = (args: string, cwd = repo) =>
    child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: 'pipe' });
  child_process.execSync('git init -q -b main', { cwd: repo });
  g('config user.email t@t.t');
  g('config user.name t');
  fs.writeFileSync(path.join(repo, 'a.txt'), 'hi\n');
  g('add -A');
  g('commit -qm init');
  child_process.execSync('git init -q --bare', { cwd: remote });
  g(`remote add origin "${remote}"`);
  g('push -q origin main');
  return { repo, remote };
}

// ===========================================================================
// U011-w67 — `description` is a required schema field (parity: the reference's
// baseInputSchema requires description, not .optional()).
// ===========================================================================

describe('U011-w67 — description is a required schema field', () => {
  it('lists both prompt AND description in the required array', () => {
    const required = taskTool.inputSchema.required ?? [];
    assert.ok(required.includes('prompt'), 'prompt must stay required');
    assert.ok(required.includes('description'),
      `description must be required (parity), got required=${JSON.stringify(required)}`);
  });

  it('still declares a description property with a label hint', () => {
    const props = taskTool.inputSchema.properties;
    assert.ok(props.description, 'description property must exist');
    assert.match(String(props.description.description), /label/i);
  });
});

// ===========================================================================
// U011-w67 — cwd and isolation:"worktree" are mutually exclusive
// (parity: the reference's cwd param is documented "Mutually exclusive with
// isolation: worktree").
// ===========================================================================

describe('U011-w67 — cwd / isolation:"worktree" mutual exclusivity', () => {
  beforeEach(() => resetAgentRegistry());

  it('documents the mutual exclusivity in BOTH schema descriptions', () => {
    const props = taskTool.inputSchema.properties;
    assert.match(String(props.cwd.description), /mutually exclusive/i);
    assert.match(String(props.cwd.description), /isolation/i);
    assert.match(String(props.isolation.description), /mutually exclusive/i);
    assert.match(String(props.isolation.description), /cwd/i);
  });

  it('returns an explicit error (does not silently combine) when both are supplied', async () => {
    let factoryCalled = false;
    const r = await taskTool.execute(
      { prompt: 'do work', isolation: 'worktree', cwd: path.join(os.tmpdir(), 'anywhere-abs') },
      ctx({
        cwd: os.tmpdir(),
        allowedReadRoots: [os.tmpdir()],
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => { factoryCalled = true; return { submit: async function* () { yield { type: 'text_delta', text: 'x' }; } }; },
      } as unknown as Partial<ToolContext>),
    );
    assert.ok(r.isError, 'combining cwd + isolation:worktree must be rejected');
    assert.match(r.output, /mutually exclusive/i);
    assert.equal(factoryCalled, false, 'must fail fast before spawning a sub-engine / worktree');
  });

  it('allows cwd alone and isolation alone (no false positive)', async () => {
    // cwd alone: no worktree, factory runs with the override cwd.
    let runCwd = '';
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011w67-cwd-'));
    try {
      const r = await taskTool.execute(
        { prompt: 'cwd only', cwd: tmp },
        ctx({
          cwd: tmp,
          allowedReadRoots: [tmp],
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => { runCwd = cfg.cwd; return { submit: async function* () { yield { type: 'text_delta', text: 'ok' }; } }; },
        } as unknown as Partial<ToolContext>),
      );
      assert.ok(!r.isError, r.output);
      assert.equal(runCwd, tmp);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// U011-w67 — worked <example> blocks in the tool description
// (parity: the reference's Agent tool description embeds concrete <example> calls).
// ===========================================================================

describe('U011-w67 — worked examples in the task tool description', () => {
  it('embeds at least one <example> invocation showing a well-briefed call', () => {
    const d = taskTool.description;
    assert.ok(d.includes('<example>'), 'description must contain <example> blocks');
    assert.ok(d.includes('</example>'));
    // The example must model the expected call shape: a description + prompt.
    assert.ok(d.includes('description:') && d.includes('prompt:'),
      'the example must demonstrate the description/prompt call shape');
  });
});

// ===========================================================================
// U011-w67 — isolated-worktree cleanup on the ERROR / ABORT paths
// (parity: the reference runs cleanupWorktreeIfNeeded in finally "to handle
// abort/error paths"). AMI previously only cleaned up on the success return.
// ===========================================================================

describe('U011-w67 — foreground worktree cleanup on error path', () => {
  beforeEach(() => resetAgentRegistry());

  it('removes a change-free worktree even when the subagent THROWS', async () => {
    const { repo, remote } = makePushedRepo();
    let runCwd = '';
    try {
      const r = await taskTool.execute(
        { prompt: 'boom', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            runCwd = cfg.cwd;
            return { submit: async function* () { throw new Error('subagent exploded'); }, shutdown: () => {} };
          },
        } as unknown as Partial<ToolContext>),
      );
      assert.ok(r.isError, 'a thrown subagent must surface as an error');
      assert.ok(runCwd.includes(path.join('.ami', 'worktrees')), `must have run in a worktree, got ${runCwd}`);
      // DISCRIMINATING: before the fix the catch returned before cleanup, leaving the
      // worktree on disk. The finally-based cleanup now removes the change-free tree.
      assert.ok(!fs.existsSync(runCwd),
        'a change-free worktree must be torn down on the error path');
    } finally {
      try { child_process.execSync('git worktree prune', { cwd: repo, stdio: 'ignore' }); } catch { /* ignore */ }
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });

  it('RETAINS a dirty worktree on the error path (shouldRemoveWorktree guard preserved)', async () => {
    const { repo, remote } = makePushedRepo();
    let runCwd = '';
    try {
      const r = await taskTool.execute(
        { prompt: 'write then boom', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            runCwd = cfg.cwd;
            return {
              submit: async function* () {
                // Leave an uncommitted change behind, THEN throw.
                fs.writeFileSync(path.join(cfg.cwd, 'new-work.txt'), 'important\n');
                throw new Error('crashed after writing');
              },
              shutdown: () => {},
            };
          },
        } as unknown as Partial<ToolContext>),
      );
      assert.ok(r.isError);
      assert.ok(fs.existsSync(runCwd),
        'a worktree with uncommitted changes must be KEPT even when the agent errors');
      assert.ok(fs.existsSync(path.join(runCwd, 'new-work.txt')), 'the agent work must survive');
    } finally {
      try { child_process.execSync('git worktree remove --force ' + JSON.stringify(runCwd), { cwd: repo, stdio: 'ignore' }); } catch { /* ignore */ }
      try { child_process.execSync('git worktree prune', { cwd: repo, stdio: 'ignore' }); } catch { /* ignore */ }
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });
});

describe('U011-w67 — background worktree cleanup on completion', () => {
  beforeEach(() => resetAgentRegistry());

  it('removes a change-free worktree when a BACKGROUND agent completes', async () => {
    const { repo, remote } = makePushedRepo();
    const pm = new ProcessManager(repo);
    let runCwd = '';
    try {
      const res = await taskTool.execute(
        { prompt: 'bg clean', description: 'bg clean run', run_in_background: true, isolation: 'worktree' },
        ctx({
          cwd: repo,
          processManager: pm,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            runCwd = cfg.cwd;
            return { submit: async function* () { yield { type: 'text_delta', text: 'done' }; }, shutdown: () => {} };
          },
        } as unknown as Partial<ToolContext>),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      // Wait for the detached IIFE to finish (poll the output footer).
      const outPath = path.join(projectStateDir(repo, 'tasks'), `${taskId}.output`);
      for (let i = 0; i < 100 && !fs.readFileSync(outPath, 'utf-8').includes('[Agent stats'); i++) {
        await sleep(20);
      }
      const content = fs.readFileSync(outPath, 'utf-8');
      assert.ok(content.includes('[Agent stats'), `background run must finish, got: ${content}`);
      // DISCRIMINATING: before the fix the background path never cleaned the worktree.
      assert.ok(content.includes('worktree cleaned up'),
        `a change-free background worktree must be cleaned up, got: ${content}`);
      assert.ok(runCwd.includes(path.join('.ami', 'worktrees')));
      assert.ok(!fs.existsSync(runCwd), 'the background worktree directory must be removed');
    } finally {
      pm.cleanup();
      try { child_process.execSync('git worktree prune', { cwd: repo, stdio: 'ignore' }); } catch { /* ignore */ }
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });
});
