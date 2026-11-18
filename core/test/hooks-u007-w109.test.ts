/**
 * U007 / w109 — hooks.ts parity additions:
 *
 *  - getPluginAffectingSettingsSnapshot() + reloadPluginHooksIfChanged():
 *    a deterministic key-sorted snapshot of the inputs that affect which plugin
 *    hooks load, used to SKIP a redundant clear+re-register when nothing changed
 *    (Claude Code's loadPluginHooks "skipping reload, plugin-affecting settings
 *    unchanged" path). (gap keys: loadPluginHooks settings-change hot-reload ×2)
 *
 *  - RuntimeHookMeta.internal + getSummary split: a system-registered in-process
 *    callback (Claude Code's HookCallback `internal:true`) is hidden from the
 *    user-facing `callbackHooks` count and reported separately as
 *    `internalCallbackHooks`, while still firing normally. (gap key:
 *    sessionFileAccessHooks in-process callback `internal` flag)
 *
 * HOME is isolated so real ~/.ami / ~/.claude hooks never leak into the counts.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type HookDecisionOutput } from '../src/hooks';

describe('HookManager — plugin-affecting settings snapshot + gated reload (w109)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w109-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writePlugin(name: string, command: string): string {
    const root = path.join(tmp, '.ami', 'plugins', name);
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name, version: '1.0.0' }));
    fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command } }] }));
    return root;
  }

  it('snapshot is deterministic and order-independent for the same config', () => {
    writePlugin('alpha', 'echo a');
    writePlugin('beta', 'echo b');
    const s1 = hm.getPluginAffectingSettingsSnapshot(tmp);
    const s2 = hm.getPluginAffectingSettingsSnapshot(tmp);
    assert.equal(s1, s2, 'identical config yields an identical fingerprint');
    // The fingerprint carries the policy knobs that gate plugin hooks.
    assert.match(s1, /allowManagedHooksOnly/);
    assert.match(s1, /workspaceTrusted/);
    assert.match(s1, /disableAllHooks/);
  });

  it('snapshot changes when a plugin source is added or a gating policy flips', () => {
    const base = hm.getPluginAffectingSettingsSnapshot(tmp);
    writePlugin('alpha', 'echo a');
    const withPlugin = hm.getPluginAffectingSettingsSnapshot(tmp);
    assert.notEqual(withPlugin, base, 'adding a plugin source changes the fingerprint');
    // A policy flip (workspace trust) also changes the fingerprint.
    hm.setWorkspaceTrusted(false);
    assert.notEqual(hm.getPluginAffectingSettingsSnapshot(tmp), withPlugin);
  });

  it('reloadPluginHooksIfChanged reloads on first call, then skips when unchanged', () => {
    writePlugin('alpha', 'echo a');
    const first = hm.reloadPluginHooksIfChanged(tmp);
    assert.equal(first.skipped, false, 'the first gated reload is never a skip');
    assert.equal(first.registered, 1);
    assert.equal(hm.getSummary().preToolUse, 1);

    const second = hm.reloadPluginHooksIfChanged(tmp);
    assert.equal(second.skipped, true, 'an unchanged snapshot skips the reload');
    assert.equal(second.cleared, 0);
    assert.equal(second.registered, 0);
    // The skip did NOT clear the live plugin hook.
    assert.equal(hm.getSummary().preToolUse, 1, 'a skipped reload leaves hooks in place');
  });

  it('reloadPluginHooksIfChanged reloads again after the plugin set actually changes', () => {
    writePlugin('alpha', 'echo a');
    assert.equal(hm.reloadPluginHooksIfChanged(tmp).skipped, false);
    assert.equal(hm.reloadPluginHooksIfChanged(tmp).skipped, true);

    // Add a second plugin — the snapshot changes, so the next gated reload runs.
    writePlugin('beta', 'echo b');
    const r = hm.reloadPluginHooksIfChanged(tmp);
    assert.equal(r.skipped, false, 'a changed plugin set triggers a real reload');
    assert.equal(hm.getSummary().preToolUse, 2, 'both plugin hooks are now registered');
    // ...and it settles back to skip once nothing changes again.
    assert.equal(hm.reloadPluginHooksIfChanged(tmp).skipped, true);
  });

  it('reloadPluginHooksIfChanged defaults cwd to the session cwd', () => {
    writePlugin('alpha', 'echo a');
    const r = hm.reloadPluginHooksIfChanged(); // no arg → uses setCwd(tmp)
    assert.equal(r.skipped, false);
    assert.equal(r.registered, 1);
  });
});

describe('HookManager — internal (system) callback hooks hidden from the user count (w109)', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('an internal callback is excluded from callbackHooks and counted as internalCallbackHooks', async () => {
    let userFired = false;
    let internalFired = false;
    // A user-configured runtime callback (default: NOT internal).
    hm.registerCallback('preToolUse', async () => { userFired = true; });
    // A system-registered internal callback (Claude Code HookCallback internal:true).
    hm.registerCallback('preToolUse', async () => { internalFired = true; }, undefined, { internal: true });

    const summary = hm.getSummary();
    assert.equal(summary.callbackHooks, 1, 'only the user callback is counted as user-facing');
    assert.equal(summary.internalCallbackHooks, 1, 'the internal callback is reported separately');

    // Both still fire — internal only affects the count, never execution.
    await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.ok(userFired && internalFired, 'both the user and internal callbacks ran');
  });

  it('addSessionHook and addFunctionHook can mark a hook internal', () => {
    hm.addSessionHook('sess-1', 'notification', async () => {}, { internal: true });
    assert.equal(hm.getSummary().callbackHooks, 0, 'an internal session hook is not user-facing');
    assert.equal(hm.getSummary().internalCallbackHooks, 1);

    hm.addFunctionHook('stop', () => true, 'blocked', { internal: true });
    assert.equal(hm.getSummary().internalCallbackHooks, 2, 'an internal function hook is hidden too');
    assert.equal(hm.getSummary().callbackHooks, 0);
  });

  it('a non-internal callback returning a decision is still honored', async () => {
    hm.registerCallback('preToolUse', async (): Promise<HookDecisionOutput> => ({ decision: 'block', reason: 'nope' }));
    assert.equal(hm.getSummary().callbackHooks, 1);
    assert.equal(hm.getSummary().internalCallbackHooks, 0);
    const r = await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.deepEqual(r, { decision: 'block', reason: 'nope' });
  });
});
