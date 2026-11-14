/**
 * Parity tests for the async ('background') hook registry (Claude->AMI parity):
 *  - async hooks are tracked (getPendingAsyncCount) instead of leaking as
 *    untracked fire-and-forget promises;
 *  - finalizePendingAsyncHooks() awaits in-flight async hooks (graceful shutdown);
 *  - cancelAllAsyncHooks() kills in-flight async hooks (hard shutdown) and resets
 *    the signal so subsequently-fired hooks are not born aborted.
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const ptuCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: {} };

function writeHook(cwd: string, command: string): void {
  const amiDir = path.join(cwd, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook: { type: 'command', command, async: true } }],
  }));
}

describe('HookManager — async hook registry', () => {
  let tmp: string;
  let hm: HookManager;
  const dirs: string[] = [];

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-async-hooks-'));
    dirs.push(tmp);
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  after(() => { for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('starts with no pending async hooks', () => {
    assert.equal(hm.getPendingAsyncCount(), 0);
  });

  it('tracks a fired async hook and finalize awaits it', async () => {
    writeHook(tmp, 'true');
    hm.loadFromFile(tmp);
    const decision = await hm.executePreToolUse(ptuCtx);
    // async hooks never block the tool: the decision is an immediate allow.
    assert.equal(decision.action, 'allow');
    assert.equal(hm.getPendingAsyncCount(), 1, 'async hook is tracked while running');
    await hm.finalizePendingAsyncHooks();
    assert.equal(hm.getPendingAsyncCount(), 0, 'finalize drains the registry');
  });

  it('cancelAllAsyncHooks kills an in-flight async hook fast', async () => {
    writeHook(tmp, 'sleep 30');
    hm.loadFromFile(tmp);
    await hm.executePreToolUse(ptuCtx);
    assert.equal(hm.getPendingAsyncCount(), 1);

    const started = Date.now();
    const cancelled = hm.cancelAllAsyncHooks();
    assert.equal(cancelled, 1, 'reports the number cancelled');
    await hm.finalizePendingAsyncHooks();
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 5000, `cancellation returns promptly (took ${elapsed}ms, not ~30s)`);
    assert.equal(hm.getPendingAsyncCount(), 0);
  });

  it('cancel resets the signal so later async hooks are not born aborted', async () => {
    writeHook(tmp, 'sleep 30');
    hm.loadFromFile(tmp);
    await hm.executePreToolUse(ptuCtx);
    hm.cancelAllAsyncHooks();
    await hm.finalizePendingAsyncHooks();

    // A NEW async hook fired after cancel must run (not instantly aborted).
    await hm.executePreToolUse(ptuCtx);
    assert.equal(hm.getPendingAsyncCount(), 1, 'post-cancel async hook runs');
    hm.cancelAllAsyncHooks();
    await hm.finalizePendingAsyncHooks();
  });
});
