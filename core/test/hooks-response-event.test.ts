/**
 * Unit tests for the structured async-hook completion record (parity: the reference
 * Code's finalizeHook/emitHookResponse, utils/hooks/AsyncHookRegistry.ts).
 *
 * The async-hook path previously only re-woke the model via `hookRewake` on a
 * DENY/exit-2/error. This verifies the HookManager now emits a `hookResponse`
 * event for EVERY async-hook completion, classified:
 *   - 'success'   — the command exited 0;
 *   - 'error'     — the command exited non-zero (blocking exit-2 or other);
 *   - 'cancelled' — the hook was aborted (shutdown / async-timeout budget),
 * each carrying the command hook's exitCode and SEPARATELY-captured stdout/stderr.
 *
 * Red-green: reverting the `this.emit('hookResponse', …)` in fireAsyncHook's
 * finally (or the `outcome.exitCode/stdout/stderr` capture in the command path)
 * makes these assertions fail — no event is emitted / fields are undefined.
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const ptuCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: {} };

interface HookResponsePayload {
  event: string;
  outcome: 'success' | 'error' | 'cancelled';
  exitCode?: number;
  stdout?: string;
  stderr?: string;
  pluginName?: string;
  pluginId?: string;
}

function writeHook(cwd: string, command: string): void {
  const amiDir = path.join(cwd, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook: { type: 'command', command, async: true } }],
  }));
}

function collectResponses(hm: HookManager): HookResponsePayload[] {
  const got: HookResponsePayload[] = [];
  hm.on('hookResponse', (p: HookResponsePayload) => got.push(p));
  return got;
}

describe('HookManager — structured async hook response (hookResponse)', () => {
  let tmp: string;
  let hm: HookManager;
  const dirs: string[] = [];

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hook-response-'));
    dirs.push(tmp);
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  after(() => { for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it("emits a 'success' response with exitCode 0 and captured stdout for a clean async hook", async () => {
    // A hook that exits 0 and prints to stdout — the old path emitted NOTHING
    // (hookRewake only fires on DENY/error), so a clean success was invisible.
    writeHook(tmp, 'echo hello-out');
    hm.loadFromFile(tmp);
    const got = collectResponses(hm);
    await hm.executePreToolUse(ptuCtx);
    await hm.finalizePendingAsyncHooks();

    assert.equal(got.length, 1, 'exactly one completion record is emitted');
    const r = got[0];
    assert.equal(r.event, 'preToolUse');
    assert.equal(r.outcome, 'success');
    assert.equal(r.exitCode, 0);
    assert.ok((r.stdout ?? '').includes('hello-out'), `stdout captured separately, got ${JSON.stringify(r.stdout)}`);
  });

  it("emits an 'error' response with the non-zero exitCode and captured stderr", async () => {
    // Exit 3 is a NON-blocking error — stderr is captured separately from stdout.
    writeHook(tmp, 'echo oops 1>&2; exit 3');
    hm.loadFromFile(tmp);
    const got = collectResponses(hm);
    await hm.executePreToolUse(ptuCtx);
    await hm.finalizePendingAsyncHooks();

    assert.equal(got.length, 1);
    const r = got[0];
    assert.equal(r.outcome, 'error');
    assert.equal(r.exitCode, 3);
    assert.ok((r.stderr ?? '').includes('oops'), `stderr captured separately, got ${JSON.stringify(r.stderr)}`);
  });

  it("emits an 'error' response for a blocking exit-2 async hook", async () => {
    writeHook(tmp, 'echo blocked 1>&2; exit 2');
    hm.loadFromFile(tmp);
    const got = collectResponses(hm);
    await hm.executePreToolUse(ptuCtx);
    await hm.finalizePendingAsyncHooks();

    assert.equal(got.length, 1);
    assert.equal(got[0].outcome, 'error');
    assert.equal(got[0].exitCode, 2);
  });

  it("emits a 'cancelled' response when an in-flight async hook is cancelled", async () => {
    writeHook(tmp, 'sleep 30');
    hm.loadFromFile(tmp);
    const got = collectResponses(hm);
    await hm.executePreToolUse(ptuCtx);
    hm.cancelAllAsyncHooks();
    await hm.finalizePendingAsyncHooks();

    assert.equal(got.length, 1, 'a cancelled hook still produces a completion record');
    assert.equal(got[0].outcome, 'cancelled');
  });
});
