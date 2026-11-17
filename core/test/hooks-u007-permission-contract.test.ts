/**
 * U007 — PermissionRequest hook contract parity (core/core/src/hooks.ts).
 *
 * Claude Code's permission `runHooks` threads `permissionMode` + `suggestions`
 * into a PermissionRequest hook and lets its decision carry, on ALLOW, an
 * `updatedInput` rewrite and durable `updatedPermissions` rule mutations, and on
 * DENY an `interrupt` flag that aborts the whole turn. The engine abort signal is
 * also threaded in so a slow/hanging hook is cancelled on user interrupt. These
 * tests drive the real HookManager (on-disk .ami/hooks.json command hooks + an
 * in-process hook) to prove each new field round-trips.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  HookManager,
  raceAbort,
  validateHookDecisionOutput,
  serializeHookEvent,
  type HookConfigEntry,
  type PermissionContext,
} from '../src/hooks';

const tmpDirs: string[] = [];
function newCwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'u007-perm-'));
  tmpDirs.push(d);
  return d;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

function makeManager(cwd: string, entries: HookConfigEntry[]): HookManager {
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  const mgr = new HookManager();
  mgr.setWorkspaceTrusted(true);
  mgr.setCwd(cwd);
  mgr.loadFromFile(cwd);
  return mgr;
}

const baseCtx: PermissionContext = { toolName: 'bash', toolInput: { command: 'ls' } };

describe('U007 — PermissionRequest hook richer decision', () => {
  it('ALLOW carries updatedInput (tool-input rewrite at permission time)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' '{"permissionDecision":"allow","updatedInput":{"command":"ls -la"}}'` },
    }]);
    const res = await mgr.executePermissionRequest(baseCtx);
    assert.equal(res?.action, 'allow');
    assert.deepEqual(res?.updatedInput, { command: 'ls -la' });
  });

  it('ALLOW carries durable updatedPermissions rule mutations', async () => {
    const cwd = newCwd();
    const upd = '[{"type":"addRules","behavior":"allow","destination":"session","rules":[{"toolName":"bash","ruleContent":"ls"}]}]';
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' '{"permissionDecision":"allow","updatedPermissions":${upd}}'` },
    }]);
    const res = await mgr.executePermissionRequest(baseCtx);
    assert.equal(res?.action, 'allow');
    assert.equal(res?.updatedPermissions?.length, 1);
    assert.equal(res?.updatedPermissions?.[0].type, 'addRules');
  });

  it('DENY with interrupt:true surfaces the hard-abort flag', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' '{"permissionDecision":"deny","reason":"dangerous","interrupt":true}'` },
    }]);
    const res = await mgr.executePermissionRequest(baseCtx);
    assert.equal(res?.action, 'deny');
    assert.match(res?.reason ?? '', /dangerous/);
    assert.equal(res?.interrupt, true);
  });

  it('DENY without interrupt leaves interrupt unset (soft skip)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' '{"permissionDecision":"deny","reason":"nope"}'` },
    }]);
    const res = await mgr.executePermissionRequest(baseCtx);
    assert.equal(res?.action, 'deny');
    assert.equal(res?.interrupt, undefined);
  });

  it('ALLOW without rewrites returns a bare allow (no spurious fields)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' '{"permissionDecision":"allow"}'` },
    }]);
    const res = await mgr.executePermissionRequest(baseCtx);
    assert.deepEqual(res, { action: 'allow' });
  });

  it('threads permissionMode into the hook stdin payload', async () => {
    const cwd = newCwd();
    // The hook allows ONLY when it can read the active mode from its stdin.
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `grep -q '"permission_mode":"deny-all"' && printf 'ALLOW' || printf 'DENY:no-mode'` },
    }]);
    const allowed = await mgr.executePermissionRequest({ ...baseCtx, permissionMode: 'deny-all' });
    assert.equal(allowed?.action, 'allow');
    const denied = await mgr.executePermissionRequest(baseCtx);
    assert.equal(denied?.action, 'deny');
  });

  it('threads suggestions into the hook stdin payload', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `grep -q '"suggestions"' && printf 'ALLOW' || printf 'DENY:no-suggestions'` },
    }]);
    const res = await mgr.executePermissionRequest({
      ...baseCtx,
      suggestions: [{ type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'ls' }] }],
    });
    assert.equal(res?.action, 'allow');
  });
});

describe('U007 — abort propagation into permission hooks', () => {
  it('an already-aborted signal short-circuits before any hook runs', async () => {
    const mgr = new HookManager();
    let ran = false;
    mgr.onPermissionRequest(async () => { ran = true; return { action: 'deny' as const }; });
    const ac = new AbortController();
    ac.abort();
    const res = await mgr.executePermissionRequest({ ...baseCtx, abortSignal: ac.signal });
    assert.equal(res, undefined);
    assert.equal(ran, false, 'aborted request consults no hook');
  });

  it('a mid-flight abort cancels a hanging in-process permission hook', async () => {
    const mgr = new HookManager();
    mgr.onPermissionRequest(() => new Promise(() => { /* never resolves */ }));
    const ac = new AbortController();
    const p = mgr.executePermissionRequest({ ...baseCtx, abortSignal: ac.signal });
    setTimeout(() => ac.abort(), 10);
    const res = await p;
    assert.equal(res, undefined, 'abort unblocks the caller and yields no decision');
  });
});

describe('U007 — raceAbort helper', () => {
  it('passes work through unchanged when no signal is given', async () => {
    assert.equal(await raceAbort(Promise.resolve(42), undefined), 42);
  });
  it('rejects immediately when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(raceAbort(new Promise(() => {}), ac.signal), /aborted/);
  });
  it('rejects when the signal aborts before the work settles', async () => {
    const ac = new AbortController();
    const p = raceAbort(new Promise(() => {}), ac.signal);
    setTimeout(() => ac.abort(), 5);
    await assert.rejects(p, /aborted/);
  });
  it('resolves normally when the work wins the race', async () => {
    const ac = new AbortController();
    assert.equal(await raceAbort(Promise.resolve('ok'), ac.signal), 'ok');
  });
});

describe('U007 — validateHookDecisionOutput for new permission fields', () => {
  it('rejects a non-boolean interrupt', () => {
    assert.match(validateHookDecisionOutput({ interrupt: 'yes' }) ?? '', /interrupt/);
  });
  it('accepts a boolean interrupt', () => {
    assert.equal(validateHookDecisionOutput({ permissionDecision: 'deny', interrupt: true }), undefined);
  });
  it('rejects a non-array updatedPermissions', () => {
    assert.match(validateHookDecisionOutput({ updatedPermissions: {} }) ?? '', /updatedPermissions/);
  });
  it('accepts an array updatedPermissions', () => {
    assert.equal(validateHookDecisionOutput({ permissionDecision: 'allow', updatedPermissions: [] }), undefined);
  });
});

describe('U007 — serializeHookEvent exposes permission_mode alias', () => {
  it('emits both permissionMode and the snake_case permission_mode alias', () => {
    const line = serializeHookEvent({ event: 'permissionRequest', permissionMode: 'ask' });
    const parsed = JSON.parse(line);
    assert.equal(parsed.permissionMode, 'ask');
    assert.equal(parsed.permission_mode, 'ask');
  });
});
