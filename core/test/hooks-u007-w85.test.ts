/**
 * U007-w85 — hooks.ts parity additions:
 *
 *  R1. Agent-hook model defaults to the operator-configured small/fast model
 *      (getSmallFastModel) when `hook.model` is unset, just like prompt hooks —
 *      Claude Code's execAgentHook `model: hook.model ?? getSmallFastModel()`
 *      (execAgentHook.ts:L118). Previously only prompt hooks got the fast default.
 *  R2. interpolateHookHeaderEnv surfaces an `onWarn(name)` callback for each
 *      distinct env var referenced in a header value that is NOT on the allowlist
 *      (Claude Code's "env var $<name> not in allowedEnvVars, skipping
 *      interpolation" warning — execHttpHook.ts:L89-108).
 *  R3. A Stop hook that prevents continuation without a `stopReason` is given the
 *      canonical default wording "Stop hook prevented continuation"
 *      (formatHookStoppedContinuation / DEFAULT_STOP_HOOK_PREVENT_REASON) —
 *      Claude Code's hook_stopped_continuation attachment default (stopHooks.ts:L268-280).
 *  R4. A failing SessionEnd hook is surfaced on the `hookError` channel instead of
 *      being swallowed into the debug log (Claude Code writes 'SessionEnd hook
 *      failed: …' to stderr — utils/hooks.ts:L4127-4134).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  interpolateHookHeaderEnv,
  formatHookStoppedContinuation,
  DEFAULT_STOP_HOOK_PREVENT_REASON,
  type PreToolUseContext,
  type HookContext,
  type SessionContext,
} from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function captureEngine(onCfg: (cfg: any) => void) {
  return (cfg: any) => {
    onCfg(cfg);
    return { async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":true}' }; }, shutdown() {} };
  };
}

function tmpCwdWithHook(hook: Record<string, unknown>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w85-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: [{ event: 'preToolUse', hook }] }));
  return dir;
}

async function runPreToolUseHook(dir: string): Promise<any> {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  hm.setProviderConfig({ provider: 'anthropic', model: 'claude-opus-5', apiKey: 'sk-test' });
  let cfg: any = null;
  hm.setEngineFactory(captureEngine(c => { cfg = c; }));
  await hm.executePreToolUse(preCtx);
  return cfg;
}

describe('U007-w85 R1: agent-hook small/fast model default', () => {
  const saved = { a: process.env.ANTHROPIC_SMALL_FAST_MODEL, b: process.env.AMI_SMALL_FAST_MODEL };
  afterEach(() => {
    if (saved.a === undefined) delete process.env.ANTHROPIC_SMALL_FAST_MODEL; else process.env.ANTHROPIC_SMALL_FAST_MODEL = saved.a;
    if (saved.b === undefined) delete process.env.AMI_SMALL_FAST_MODEL; else process.env.AMI_SMALL_FAST_MODEL = saved.b;
  });

  it('an agent hook with no model uses the small/fast model when set', async () => {
    delete process.env.AMI_SMALL_FAST_MODEL;
    process.env.ANTHROPIC_SMALL_FAST_MODEL = 'claude-haiku-4';
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify', timeout: 5 });
    const cfg = await runPreToolUseHook(dir);
    assert.equal(cfg.provider.model, 'claude-haiku-4');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('an agent hook keeps the session model when no small/fast env is set (no regression)', async () => {
    delete process.env.ANTHROPIC_SMALL_FAST_MODEL;
    delete process.env.AMI_SMALL_FAST_MODEL;
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify', timeout: 5 });
    const cfg = await runPreToolUseHook(dir);
    assert.equal(cfg.provider.model, 'claude-opus-5');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('an explicit agent hook.model overrides the small/fast default', async () => {
    process.env.ANTHROPIC_SMALL_FAST_MODEL = 'claude-haiku-4';
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify', timeout: 5, model: 'claude-sonnet-4-6' });
    const cfg = await runPreToolUseHook(dir);
    assert.equal(cfg.provider.model, 'claude-sonnet-4-6');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('U007-w85 R2: header env-var not-in-allowedEnvVars warning', () => {
  it('warns once per distinct disallowed env var referenced in a header', () => {
    const warned: string[] = [];
    process.env.W85_SECRET = 'top-secret';
    const out = interpolateHookHeaderEnv(
      'Bearer $W85_SECRET $W85_SECRET ${W85_OTHER}',
      new Set<string>(), // nothing allowed
      process.env,
      (name) => warned.push(name),
    );
    delete process.env.W85_SECRET;
    // Disallowed refs resolve to empty (secret never leaks into the header):
    // "Bearer " + "" + " " + "" + " " + "" => "Bearer   ".
    assert.equal(out, 'Bearer   ');
    // Warned once per DISTINCT var, not per occurrence.
    assert.deepEqual(warned.sort(), ['W85_OTHER', 'W85_SECRET']);
  });

  it('does NOT warn for an allowlisted var and still interpolates it', () => {
    const warned: string[] = [];
    process.env.W85_ALLOWED = 'value';
    const out = interpolateHookHeaderEnv(
      'X: $W85_ALLOWED',
      new Set(['W85_ALLOWED']),
      process.env,
      (name) => warned.push(name),
    );
    delete process.env.W85_ALLOWED;
    assert.equal(out, 'X: value');
    assert.deepEqual(warned, []);
  });

  it('is backward-compatible when no onWarn callback is supplied', () => {
    const out = interpolateHookHeaderEnv('A: $NOPE', new Set<string>(), {});
    assert.equal(out, 'A: ');
  });
});

describe('U007-w85 R3: default stop-hook prevent-continuation reason', () => {
  it('formatHookStoppedContinuation returns the hook reason when present', () => {
    assert.equal(formatHookStoppedContinuation('tests still failing'), 'tests still failing');
  });

  it('formatHookStoppedContinuation falls back to the canonical default', () => {
    assert.equal(formatHookStoppedContinuation(), DEFAULT_STOP_HOOK_PREVENT_REASON);
    assert.equal(formatHookStoppedContinuation('   '), DEFAULT_STOP_HOOK_PREVENT_REASON);
    assert.equal(DEFAULT_STOP_HOOK_PREVENT_REASON, 'Stop hook prevented continuation');
  });

  it('executeStop defaults the stopReason when a hook prevents continuation terse', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({ preventContinuation: true }));
    const ctx: HookContext = { messages: [], turnCount: 0 };
    const result = await hm.executeStop(ctx);
    assert.equal(result.preventContinuation, true);
    assert.equal(result.stopReason, DEFAULT_STOP_HOOK_PREVENT_REASON);
  });

  it('executeStop keeps an explicit stopReason', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({ preventContinuation: true, stopReason: 'budget exhausted' }));
    const ctx: HookContext = { messages: [], turnCount: 0 };
    const result = await hm.executeStop(ctx);
    assert.equal(result.stopReason, 'budget exhausted');
  });

  it('executeStop leaves stopReason undefined when continuation is NOT prevented', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({ blockingError: 'keep going' }));
    const ctx: HookContext = { messages: [], turnCount: 0 };
    const result = await hm.executeStop(ctx);
    assert.equal(result.preventContinuation, false);
    assert.equal(result.stopReason, undefined);
  });
});

describe('U007-w85 R4: SessionEnd hook failures surfaced on hookError', () => {
  it('emits a hookError event when a SessionEnd hook throws', async () => {
    const hm = new HookManager();
    const errors: Array<{ hookType: string; hook: string; error: string }> = [];
    hm.on('hookError', (d: any) => errors.push(d));
    hm.onSessionEnd(async () => { throw new Error('teardown blew up'); });
    const ctx: SessionContext = { sessionId: 's1', cwd: process.cwd() };
    await hm.executeSessionEnd(ctx);
    assert.equal(errors.length, 1);
    assert.equal(errors[0].hook, 'SessionEnd hook');
    assert.match(errors[0].error, /teardown blew up/);
  });

  it('does not emit hookError when a SessionEnd hook succeeds', async () => {
    const hm = new HookManager();
    let fired = 0;
    hm.on('hookError', () => { fired++; });
    hm.onSessionEnd(async () => { /* ok */ });
    await hm.executeSessionEnd({ sessionId: 's2', cwd: process.cwd() });
    assert.equal(fired, 0);
  });
});
