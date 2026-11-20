/**
 * Hooks parity — SessionEnd execution budget (the reference's
 * getSessionEndHookTimeoutMs / SESSION_END_HOOK_TIMEOUT_MS_DEFAULT).
 *
 * SessionEnd hooks run during shutdown/clear and must not block teardown. Before
 * this change, executeSessionEnd awaited every sessionEnd hook with NO overall
 * timeout and command hooks ran with the generic 60s default — so a slow or
 * hanging teardown hook could stall process exit. This closes the gap fully
 * inside core/core/src/hooks.ts: a tight default budget (1500ms), overridable via
 * CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS, bounds the whole run AND is wired as an
 * AbortSignal into the command-hook path so a lingering child is actually killed.
 *
 * Red-green discriminators are noted inline per test.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  getSessionEndHookTimeoutMs,
  DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS,
  MAX_HOOK_TIMEOUT_MS,
  type SessionContext,
} from '../src/hooks';

const ENV_KEY = 'CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS';

afterEach(() => { delete process.env[ENV_KEY]; });

function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-se-budget-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}

function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}

describe('getSessionEndHookTimeoutMs — env knob resolution', () => {
  it('defaults to DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS (1500ms) when unset', () => {
    assert.equal(getSessionEndHookTimeoutMs({}), DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS);
    assert.equal(DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS, 1_500);
  });

  it('honors a positive integer env override', () => {
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: '4200' }), 4200);
  });

  it('falls back to the default for non-numeric / zero / negative values', () => {
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: 'abc' }), DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS);
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: '0' }), DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS);
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: '-10' }), DEFAULT_SESSIONEND_HOOK_TIMEOUT_MS);
  });

  it('clamps to [100ms, MAX_HOOK_TIMEOUT_MS]', () => {
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: '10' }), 100);
    assert.equal(getSessionEndHookTimeoutMs({ [ENV_KEY]: String(MAX_HOOK_TIMEOUT_MS * 10) }), MAX_HOOK_TIMEOUT_MS);
  });

  it('reads process.env by default', () => {
    process.env[ENV_KEY] = '2500';
    assert.equal(getSessionEndHookTimeoutMs(), 2500);
  });
});

describe('executeSessionEnd — bounded by the budget', () => {
  it('bounds a hanging in-process sessionEnd hook to the budget (red-green)', async () => {
    const hm = new HookManager();
    // A teardown callback that never resolves — the pre-fix executeSessionEnd
    // would await it forever and block shutdown.
    hm.onSessionEnd(() => new Promise<void>(() => { /* never resolves */ }));
    process.env[ENV_KEY] = '120';

    const outcome = await Promise.race([
      hm.executeSessionEnd({ sessionId: 's', cwd: process.cwd(), reason: 'logout' } as SessionContext).then(() => 'returned'),
      new Promise<string>(r => setTimeout(() => r('hung'), 1_500)),
    ]);
    // RED without the fix: executeSessionEnd never resolves → 'hung'.
    // GREEN with the fix: it returns within the 120ms budget → 'returned'.
    assert.equal(outcome, 'returned');
  });

  it('still runs a fast in-process sessionEnd hook to completion', async () => {
    const hm = new HookManager();
    let ran = false;
    hm.onSessionEnd(async () => { ran = true; });
    await hm.executeSessionEnd({ sessionId: 's', cwd: process.cwd(), reason: 'logout' } as SessionContext);
    assert.equal(ran, true);
  });

  it('kills a slow command sessionEnd hook at the budget (red-green)', async () => {
    const marker = 'late-marker.txt';
    const dir = tmpCwdWithHooks([
      // Sleeps well past the budget, then would create a marker. The budget-scoped
      // AbortSignal must SIGKILL the child before the marker is ever written.
      { event: 'sessionEnd', hook: { type: 'command', command: `sleep 5 && touch ${marker}` } },
    ]);
    const hm = makeManager(dir);
    const markerPath = path.join(dir, marker);
    process.env[ENV_KEY] = '150';

    const start = Date.now();
    await hm.executeSessionEnd({ sessionId: 's', cwd: dir, reason: 'logout' } as SessionContext);
    const elapsed = Date.now() - start;

    // RED without the fix: the 5s sleep runs to completion (no abort, no overall
    // bound) → elapsed ≈ 5000ms and the marker is created.
    // GREEN with the fix: child killed at ~150ms → fast return, no marker.
    assert.ok(elapsed < 2_000, `executeSessionEnd should return within budget, took ${elapsed}ms`);
    assert.equal(fs.existsSync(markerPath), false, 'slow command hook must be killed before writing its marker');
    // Give any (incorrectly) un-killed child time to surface, then re-check.
    await new Promise(r => setTimeout(r, 200));
    assert.equal(fs.existsSync(markerPath), false, 'child must stay killed (no delayed marker)');
  });

  it('runs a fast command sessionEnd hook to completion under the budget', async () => {
    const marker = 'fast-marker.txt';
    const dir = tmpCwdWithHooks([
      { event: 'sessionEnd', hook: { type: 'command', command: `touch ${marker}` } },
    ]);
    const hm = makeManager(dir);
    await hm.executeSessionEnd({ sessionId: 's', cwd: dir, reason: 'logout' } as SessionContext);
    assert.equal(fs.existsSync(path.join(dir, marker)), true);
  });
});
