/**
 * U007 / w87 — plugin-hook lifecycle management (gh-36995 parity).
 *
 * A disabled or uninstalled plugin's hooks MUST stop firing immediately, and a
 * plugin management operation (/reload-plugins) MUST re-register plugin hooks as an
 * atomic clear-then-register swap (idempotent — unchanged plugins register exactly
 * once, disabled plugins stay gone). These tests cover:
 *   - clearPluginHooks()           — remove only plugin hooks, leave filesystem hooks
 *   - pruneRemovedPluginHooks()    — immediate revocation by enabled-root set
 *   - reloadPluginHooks()          — atomic clear-then-register swap + idempotence
 *   - behavioral e2e               — a pruned plugin preToolUse hook no longer denies
 *
 * HOME is isolated so real ~/.ami / ~/.claude hooks never leak into the counts.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

describe('HookManager — plugin-hook lifecycle (clear / prune / reload)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pluglife-'));
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

  /** Create a project-tier plugin bundle contributing a single preToolUse hook. */
  function writePlugin(name: string, hook: Record<string, unknown>): string {
    const root = path.join(tmp, '.ami', 'plugins', name);
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name, version: '1.0.0' }));
    fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks: [{ event: 'preToolUse', hook }] }));
    return root;
  }

  function writeProjectHook(command: string): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command } }] }),
    );
  }

  const cmd = (c: string) => ({ type: 'command', command: c });

  it('clearPluginHooks removes only plugin hooks, leaving filesystem hooks intact', () => {
    writeProjectHook('echo project');
    writePlugin('alpha', cmd('echo alpha'));
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 2, 'project + plugin hooks both registered');
    assert.equal(hm.describeHooks().filter(d => d.pluginName === 'alpha').length, 1);

    const removed = hm.clearPluginHooks();
    assert.equal(removed, 1, 'exactly the one plugin hook was cleared');
    assert.equal(hm.getSummary().preToolUse, 1, 'the project filesystem hook survives');
    assert.equal(hm.describeHooks().filter(d => d.pluginName).length, 0, 'no plugin descriptors remain');
    // Idempotent: a second clear removes nothing.
    assert.equal(hm.clearPluginHooks(), 0);
  });

  it('pruneRemovedPluginHooks drops hooks of plugins absent from the enabled set (gh-36995)', () => {
    const rootA = writePlugin('alpha', cmd('echo alpha'));
    const rootB = writePlugin('beta', cmd('echo beta'));
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 2);

    // beta stays enabled, alpha is gone.
    const pruned = hm.pruneRemovedPluginHooks([rootB]);
    assert.equal(pruned, 1, 'only alpha (not in the enabled set) is pruned');
    assert.equal(hm.getSummary().preToolUse, 1);
    const remaining = hm.describeHooks().filter(d => d.pluginName);
    assert.equal(remaining.length, 1);
    assert.equal(remaining[0].pluginName, 'beta');
    void rootA;
  });

  it('pruneRemovedPluginHooks keeps every hook when all roots remain enabled', () => {
    const rootA = writePlugin('alpha', cmd('echo alpha'));
    const rootB = writePlugin('beta', cmd('echo beta'));
    hm.loadFromFile(tmp);
    assert.equal(hm.pruneRemovedPluginHooks([rootA, rootB]), 0);
    assert.equal(hm.getSummary().preToolUse, 2);
  });

  it('reloadPluginHooks is an atomic clear-then-register swap and is idempotent', () => {
    const rootA = writePlugin('alpha', cmd('echo alpha'));
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1);

    // Disable alpha mid-session, then reload: the disabled plugin's hook is dropped.
    fs.writeFileSync(path.join(rootA, '.disabled'), '');
    const r1 = hm.reloadPluginHooks(tmp);
    assert.equal(r1.cleared, 1, 'the live plugin hook was cleared');
    assert.equal(r1.registered, 0, 'a disabled plugin contributes nothing on reload');
    assert.equal(hm.getSummary().preToolUse, 0);

    // Re-enable alpha and reload: its hook comes back exactly once.
    fs.rmSync(path.join(rootA, '.disabled'));
    const r2 = hm.reloadPluginHooks(tmp);
    assert.equal(r2.registered, 1);
    assert.equal(hm.getSummary().preToolUse, 1);

    // Idempotent: reloading again with no change does NOT double-register.
    hm.reloadPluginHooks(tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'unchanged plugin registers exactly once per reload');
  });

  it('reloadPluginHooks defaults cwd to the session cwd', () => {
    writePlugin('alpha', cmd('echo alpha'));
    hm.loadFromFile(tmp);
    hm.clearPluginHooks();
    assert.equal(hm.getSummary().preToolUse, 0);
    const r = hm.reloadPluginHooks(); // no cwd arg → uses setCwd(tmp)
    assert.equal(r.registered, 1);
    assert.equal(hm.getSummary().preToolUse, 1);
  });

  it('behavioral: a pruned plugin preToolUse hook no longer denies the tool', async () => {
    const rootA = writePlugin('guard', { type: 'prompt', prompt: 'verify', timeout: 5 });
    hm.loadFromFile(tmp);
    hm.setEngineFactory(() => ({
      async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":false,"reason":"blocked by plugin"}' }; },
      shutdown() {},
    }) as never);

    const before = await hm.executePreToolUse({ ...preCtx });
    assert.equal(before.action, 'deny', 'the plugin hook denies while the plugin is enabled');

    // Uninstall the plugin (root no longer enabled) → immediate revocation.
    const pruned = hm.pruneRemovedPluginHooks([]);
    assert.equal(pruned, 1);
    const after = await hm.executePreToolUse({ ...preCtx });
    assert.equal(after.action, 'allow', 'after pruning, the removed plugin no longer gates the tool');
    void rootA;
  });
});
