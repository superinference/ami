/**
 * Unit U007 (w81) — two self-contained core/core/src/hooks.ts parity gaps.
 *
 * 1. Running-hook progress affordance (Claude Code's hook_progress /
 *    getHookDisplayText). HookManager now emits `hookProgress` when a
 *    config-registered command/prompt/http/agent hook starts running and
 *    `hookResolved` (with `success`) when it settles — for BOTH the synchronous
 *    and the async (`async:true`) paths — so a frontend can render a
 *    "Running <event> hook…" indicator instead of a silent multi-second hang.
 *    The display label is the author-supplied `statusMessage` when present, else
 *    the hook's own (sanitized) content.
 *
 * 2. Process-tree kill on hook timeout (Claude Code's treeKill). A command hook
 *    that backgrounds a grandchild (`... & sleep`) previously leaked that
 *    grandchild when the hook timed out, because only the direct `bash -c` child
 *    was SIGKILLed. runCommandHookAsync now reaps the whole tree via killTree, so
 *    a backgrounded `touch SENTINEL` never fires after the hook is killed.
 *    RED on revert: replace killTree(proc) with proc.kill('SIGKILL') in the
 *    timeout/abort handlers and the SENTINEL file reappears.
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  getHookDisplayText,
  type SessionContext,
  type HookProgressEvent,
} from '../src/hooks';

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

function sessCtx(overrides: Partial<SessionContext> = {}): SessionContext {
  return { sessionId: 's1', cwd: process.cwd(), source: 'startup', ...overrides };
}

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w81-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

// ---------------------------------------------------------------------------
// getHookDisplayText / describeHookContent — pure label helper (unit)
// ---------------------------------------------------------------------------
describe('getHookDisplayText (progress label source)', () => {
  it('prefers statusMessage over content when set', () => {
    assert.equal(getHookDisplayText({ statusMessage: '  Running guard  ', content: 'echo hi' }), 'Running guard');
  });
  it('falls back to content when statusMessage is empty/unset', () => {
    assert.equal(getHookDisplayText({ content: 'echo hi' }), 'echo hi');
    assert.equal(getHookDisplayText({ statusMessage: '   ', content: 'echo hi' }), 'echo hi');
  });
});

// ---------------------------------------------------------------------------
// hookProgress / hookResolved — emitted around a sync command hook
// ---------------------------------------------------------------------------
describe('hookProgress / hookResolved events (sync command hook)', () => {
  it('emits progress then a successful resolved for a clean command hook', async () => {
    const progress: HookProgressEvent[] = [];
    const resolved: HookProgressEvent[] = [];
    hm.on('hookProgress', (p: HookProgressEvent) => progress.push(p));
    hm.on('hookResolved', (p: HookProgressEvent) => resolved.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'true' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(progress.length, 1, 'one hookProgress');
    assert.deepEqual(progress[0], { event: 'sessionStart', hookType: 'command', label: 'true' });
    assert.equal(resolved.length, 1, 'one hookResolved');
    assert.deepEqual(resolved[0], { event: 'sessionStart', hookType: 'command', label: 'true', success: true });
  });

  it('reports success:false on hookResolved when the command hook exits non-zero', async () => {
    const resolved: HookProgressEvent[] = [];
    hm.on('hookResolved', (p: HookProgressEvent) => resolved.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'exit 3' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(resolved.length, 1);
    assert.equal(resolved[0].success, false, 'a failed hook resolves with success:false');
  });

  it('uses the author-supplied statusMessage as the progress label', async () => {
    const progress: HookProgressEvent[] = [];
    hm.on('hookProgress', (p: HookProgressEvent) => progress.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'true', statusMessage: 'Verifying workspace' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(progress.length, 1);
    assert.equal(progress[0].label, 'Verifying workspace');
  });
});

// ---------------------------------------------------------------------------
// hookProgress / hookResolved — async hook settles after the arm returns
// ---------------------------------------------------------------------------
describe('hookProgress / hookResolved events (async command hook)', () => {
  it('emits progress immediately and resolved after the background hook settles', async () => {
    const progress: HookProgressEvent[] = [];
    const resolved: HookProgressEvent[] = [];
    hm.on('hookProgress', (p: HookProgressEvent) => progress.push(p));
    hm.on('hookResolved', (p: HookProgressEvent) => resolved.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'true', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());

    // The async arm returns before the background hook finishes, so progress has
    // already fired but resolved may still be pending until it settles.
    assert.equal(progress.length, 1, 'progress fires synchronously for an async hook');
    await hm.finalizePendingAsyncHooks();
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(resolved.length, 1, 'resolved fires after the async hook settles');
    assert.equal(resolved[0].success, true);
  });
});

// ---------------------------------------------------------------------------
// Process-tree kill on timeout — integration (RED on proc.kill revert)
// ---------------------------------------------------------------------------
describe('process-tree kill on hook timeout', () => {
  it('reaps a backgrounded grandchild so its SENTINEL is never written', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w81-tree-'));
    const sentinel = path.join(tmpDir, 'SENTINEL');
    // The hook backgrounds a subshell that, after 1.5s, touches SENTINEL, then the
    // hook itself sleeps 5s. With a 0.2s (200ms) hook timeout the hook is killed
    // long before 1.5s; a correct process-tree kill reaps the backgrounded
    // subshell too, so SENTINEL is NEVER created. A bare proc.kill leaves the
    // reparented subshell alive and SENTINEL appears ~1.5s later.
    const cmd = `( sleep 1.5; touch '${sentinel}' ) & sleep 5`;
    const tmpHooks = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: cmd, timeout: 0.2 } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpHooks);

    await hm.executeSessionStart(sessCtx());
    // Wait well past the subshell's 1.5s delay to let a leaked grandchild fire.
    await new Promise((r) => setTimeout(r, 3000));

    const leaked = fs.existsSync(sentinel);
    fs.rmSync(tmpHooks, { recursive: true, force: true });
    fs.rmSync(tmpDir, { recursive: true, force: true });
    assert.equal(leaked, false, 'backgrounded grandchild must be tree-killed on timeout');
  });
});
