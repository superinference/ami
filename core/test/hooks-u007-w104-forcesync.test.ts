/**
 * U007 (w104) — forceSyncExecution for SessionStart / Setup lifecycle hooks
 * (the reference parity: executeSessionStartHooks / executeSetupHooks
 * `forceSyncExecution` option — references/claude/claude-code-main/src/utils/
 * sessionStart.ts:L17, L139, L179-L207).
 *
 * Behavior: a lifecycle hook that self-declares `async:true` is normally
 * fire-and-forget (backgrounded) — runArmHook returns '' and executeSessionStart
 * never sees its initialUserMessage/additionalContexts. When the caller passes
 * `forceSyncExecution:true` on the SessionContext, the SAME hook is awaited inline
 * so its result is captured before the first turn is assembled (needed on resume /
 * print-mode). Only sessionStart + setup honor the flag.
 *
 * RED/GREEN discriminator: reverting the `&& !forceSync` guard in runArmHook makes
 * the forceSync run behave like the backgrounded run (returns undefined), failing
 * the "forceSyncExecution captures the message" assertions below.
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager } from '../src/hooks';

function writeLifecycleHook(cwd: string, event: string, hook: Record<string, unknown>): void {
  const amiDir = path.join(cwd, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({ hooks: [{ event, hook }] }));
}

describe('U007 w104 — forceSyncExecution on SessionStart/Setup hooks', () => {
  let tmp: string;
  let hm: HookManager;
  const dirs: string[] = [];

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-forcesync-'));
    dirs.push(tmp);
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  after(() => { for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

  it('backgrounds an async SessionStart hook by default (result NOT captured)', async () => {
    writeLifecycleHook(tmp, 'sessionStart', {
      type: 'command',
      command: `echo '{"initialUserMessage":"SS-FORCED"}'`,
      async: true,
    });
    hm.loadFromFile(tmp);

    const res = await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
    // async:true => fire-and-forget => runArmHook returned '' => no message captured.
    assert.equal(res, undefined, 'backgrounded async hook contributes no inline message');
    // Drain the backgrounded hook so it does not leak into other tests.
    await hm.finalizePendingAsyncHooks();
  });

  it('forceSyncExecution awaits the async SessionStart hook and captures its message', async () => {
    writeLifecycleHook(tmp, 'sessionStart', {
      type: 'command',
      command: `echo '{"initialUserMessage":"SS-FORCED"}'`,
      async: true,
    });
    hm.loadFromFile(tmp);

    const res = await hm.executeSessionStart({ sessionId: 's', cwd: tmp, forceSyncExecution: true });
    assert.ok(res, 'forceSyncExecution returns an inline result');
    assert.equal(res!.initialUserMessage, 'SS-FORCED', 'the async hook ran synchronously and its message was captured');
    // Nothing was backgrounded.
    assert.equal(hm.getPendingAsyncCount(), 0, 'forceSyncExecution never backgrounds the hook');
  });

  it('forceSyncExecution awaits an async Setup hook additionalContexts', async () => {
    writeLifecycleHook(tmp, 'setup', {
      type: 'command',
      command: `echo '{"additionalContexts":["SETUP-CTX"]}'`,
      async: true,
    });
    hm.loadFromFile(tmp);

    const bg = await hm.executeSetup({ sessionId: 's', cwd: tmp });
    assert.equal(bg, undefined, 'async Setup hook is backgrounded by default');
    await hm.finalizePendingAsyncHooks();

    const sync = await hm.executeSetup({ sessionId: 's', cwd: tmp, forceSyncExecution: true });
    assert.ok(sync, 'forceSyncExecution captures the Setup hook result');
    assert.deepEqual(sync!.additionalContexts, ['SETUP-CTX']);
    assert.equal(hm.getPendingAsyncCount(), 0);
  });

  it('does not affect a synchronous (non-async) SessionStart hook', async () => {
    writeLifecycleHook(tmp, 'sessionStart', {
      type: 'command',
      command: `echo '{"initialUserMessage":"SYNC-ALWAYS"}'`,
    });
    hm.loadFromFile(tmp);

    // A sync hook already runs inline; the flag is a no-op for it either way.
    const off = await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
    assert.equal(off!.initialUserMessage, 'SYNC-ALWAYS');
    const on = await hm.executeSessionStart({ sessionId: 's', cwd: tmp, forceSyncExecution: true });
    assert.equal(on!.initialUserMessage, 'SYNC-ALWAYS');
    assert.equal(hm.getPendingAsyncCount(), 0);
  });
});
