/**
 * U007 / w90 — Plugin provenance on the hook REWAKE payload (Claude Code parity:
 * per-hook `pluginName`/`pluginId`).
 *
 * A backgrounded (asyncRewake) hook that completes/blocks emits a `hookRewake`
 * event the engine turns into a `[hookRewake:<event>] <message>` system message.
 * Claude Code attributes such a wake to the PLUGIN that installed the hook (its
 * `pluginName`/`pluginId`). AMI previously emitted an ANONYMOUS rewake — the
 * payload carried only `{ event, message }`, so a plugin-contributed hook's wake
 * was indistinguishable from a filesystem hook's.
 *
 * This wires plugin provenance end to end: loadFromFile stamps the plugin's
 * name/root onto EVERY hook config type, runHookByType carries it into the
 * `hookRewake` payload, and the engine consumer renders `(plugin: <name>/<id>)`.
 *
 * RED discriminators (fail without the feature):
 *  - resolvePluginId is not exported / returns the full path instead of the id.
 *  - the command-hook `hookRewake` payload has no `pluginName` / `pluginId`.
 *  - the prompt-hook `hookRewake` payload has no `pluginName` / `pluginId`.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, resolvePluginId, resolvePluginDataDir } from '../src/hooks';

describe('resolvePluginId (pure helper)', () => {
  it('returns the sanitized basename of the bundle root', () => {
    assert.equal(resolvePluginId('/home/u/.ami/plugins/my-plugin'), 'my-plugin');
    // sanitizePluginIdSegment lower-cases + keeps safe id chars.
    assert.equal(resolvePluginId('/a/b/c/Sec.Scan'), 'sec.scan');
  });

  it('returns undefined for an empty / whitespace / unsanitizable root', () => {
    assert.equal(resolvePluginId(undefined), undefined);
    assert.equal(resolvePluginId(''), undefined);
    assert.equal(resolvePluginId('   '), undefined);
  });

  it('is the id component of the per-plugin data dir (single source of truth)', () => {
    const id = resolvePluginId('/x/y/zed');
    const dir = resolvePluginDataDir('/x/y/zed');
    assert.ok(id && dir && dir.endsWith(path.join('plugin-data', id)));
  });
});

/** Build a plugin bundle under <cwd>/.ami/plugins/<name> with one hooks.json hook. */
function makePluginBundle(cwd: string, name: string, hook: Record<string, unknown>): string {
  const root = path.join(cwd, '.ami', 'plugins', name);
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(
    path.join(root, '.ami-plugin', 'plugin.json'),
    JSON.stringify({ name, version: '1.0.0' }),
  );
  fs.writeFileSync(
    path.join(root, 'hooks.json'),
    JSON.stringify({ hooks: [{ event: 'preToolUse', hook }] }),
  );
  return root;
}

describe('hookRewake carries plugin provenance (U007/w90)', () => {
  it('attributes a backgrounded COMMAND hook wake to its plugin', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-rewake-cmd-'));
    makePluginBundle(cwd, 'cmd-rewaker', {
      type: 'command',
      // exit 2 is a BLOCKING error; asyncRewake:true emits a hookRewake for it.
      command: 'echo blocked-by-plugin >&2; exit 2',
      asyncRewake: true,
    });

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);

    const rewakes: Array<{ event: string; message: string; pluginName?: string; pluginId?: string }> = [];
    hm.on('hookRewake', (e) => rewakes.push(e));

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
    });
    // exit 2 denies the tool.
    assert.equal(decision.action, 'deny');

    assert.equal(rewakes.length, 1, 'the backgrounded command hook must emit one rewake');
    assert.equal(rewakes[0].event, 'command');
    assert.match(rewakes[0].message, /blocked-by-plugin/);
    // RED without the feature: pluginName / pluginId are undefined.
    assert.equal(rewakes[0].pluginName, 'cmd-rewaker');
    assert.equal(rewakes[0].pluginId, 'cmd-rewaker');

    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('attributes a backgrounded PROMPT hook wake to its plugin', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-rewake-prompt-'));
    makePluginBundle(cwd, 'prompt-rewaker', {
      type: 'prompt',
      prompt: 'Is this safe?',
      asyncRewake: true,
    });

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);
    // Fake engine that errors — runPromptHook's error branch emits a rewake.
    hm.setEngineFactory(() => ({
      async *submit() { yield { type: 'error', error: 'engine blew up' }; },
      shutdown() {},
    }));

    const rewakes: Array<{ event: string; message: string; pluginName?: string; pluginId?: string }> = [];
    hm.on('hookRewake', (e) => rewakes.push(e));

    await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
    });

    assert.equal(rewakes.length, 1, 'the backgrounded prompt hook must emit one rewake');
    assert.equal(rewakes[0].event, 'prompt');
    assert.match(rewakes[0].message, /engine blew up/);
    // RED without the feature: pluginName / pluginId are undefined.
    assert.equal(rewakes[0].pluginName, 'prompt-rewaker');
    assert.equal(rewakes[0].pluginId, 'prompt-rewaker');

    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('a non-plugin (filesystem) hook rewake carries NO plugin attribution', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-rewake-fs-'));
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(cwd, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'exit 2', asyncRewake: true } }] }),
    );

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);

    const rewakes: Array<{ pluginName?: string; pluginId?: string }> = [];
    hm.on('hookRewake', (e) => rewakes.push(e));
    await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } });

    assert.equal(rewakes.length, 1);
    assert.equal(rewakes[0].pluginName, undefined);
    assert.equal(rewakes[0].pluginId, undefined);

    fs.rmSync(cwd, { recursive: true, force: true });
  });
});
