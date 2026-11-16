/**
 * U007 parity: bounded async ('background') hooks with a dedicated timeout.
 *
 * Gap #24 — Per-async-hook timeout (`asyncTimeout`, default 15s) + elapsed-time
 *   tracking: a background hook must run on its OWN short clock, independent of
 *   the long synchronous command/HTTP default, and be aborted when it elapses.
 * Gap #25 — Pending async-hook registry data structure + in-flight introspection:
 *   each in-flight async hook is enumerable with its firing event and elapsed time
 *   (getPendingAsyncHooks), not just a bare count.
 *
 * RED before the fix: an async hook with `asyncTimeout: 150` and a `sleep 30`
 * command stayed pending for ~30s (no dedicated async clock), and
 * getPendingAsyncHooks did not exist. GREEN: it self-aborts at ~150ms and the
 * introspection surface reports the event + elapsed time.
 */
import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  DEFAULT_ASYNC_HOOK_TIMEOUT_MS,
  type PreToolUseContext,
} from '../src/hooks';

const ptuCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: {} };

function writeAsyncHook(cwd: string, command: string, asyncTimeout?: number): void {
  const amiDir = path.join(cwd, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  const hook: Record<string, unknown> = { type: 'command', command, async: true };
  if (asyncTimeout !== undefined) hook.asyncTimeout = asyncTimeout;
  fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook }],
  }));
}

describe('HookManager — async hook timeout + introspection (U007)', () => {
  let tmp: string;
  let hm: HookManager;
  const dirs: string[] = [];

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-async-to-'));
    dirs.push(tmp);
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  after(() => { for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('exposes a 15s default async-hook timeout constant', () => {
    assert.equal(DEFAULT_ASYNC_HOOK_TIMEOUT_MS, 15_000);
  });

  it('aborts a slow async hook at its configured asyncTimeout (not the long command default)', async () => {
    writeAsyncHook(tmp, 'sleep 30', 150);
    hm.loadFromFile(tmp);

    const decision = await hm.executePreToolUse(ptuCtx);
    // Async hooks never block the tool.
    assert.equal(decision.action, 'allow');
    assert.equal(hm.getPendingAsyncCount(), 1, 'async hook is tracked while running');

    const started = Date.now();
    await hm.finalizePendingAsyncHooks();
    const elapsed = Date.now() - started;

    // Self-aborted at ~150ms — nowhere near the 30s sleep or the 60s command default.
    assert.ok(elapsed < 5000, `async hook self-aborts at its asyncTimeout (took ${elapsed}ms, not ~30s)`);
    assert.equal(hm.getPendingAsyncCount(), 0, 'registry drains after the async hook is aborted');
  });

  it('getPendingAsyncHooks reports the firing event and a non-negative elapsed time', async () => {
    writeAsyncHook(tmp, 'sleep 30', 5000);
    hm.loadFromFile(tmp);

    await hm.executePreToolUse(ptuCtx);
    const pending = hm.getPendingAsyncHooks();
    assert.equal(pending.length, 1, 'one async hook in flight');
    assert.equal(pending[0].event, 'preToolUse', 'records the event the hook fired for');
    assert.ok(pending[0].elapsedMs >= 0, 'elapsed time is tracked');
    // getPendingAsyncCount is derived from the same registry.
    assert.equal(hm.getPendingAsyncCount(), pending.length);

    // Hard cancel so the test does not wait on the 5s timeout.
    hm.cancelAllAsyncHooks();
    await hm.finalizePendingAsyncHooks();
    assert.deepEqual(hm.getPendingAsyncHooks(), []);
  });

  it('a fast async hook settles well before the async timeout and clears the registry', async () => {
    writeAsyncHook(tmp, 'true', 5000);
    hm.loadFromFile(tmp);

    await hm.executePreToolUse(ptuCtx);
    await hm.finalizePendingAsyncHooks();
    assert.equal(hm.getPendingAsyncCount(), 0);
  });
});
