import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  discoverPlugins,
  loadCommands,
  validatePluginManifest,
  resolvePluginManifestHooks,
  buildPluginRecommendation,
  type PluginBundle,
  type CommandDefinition,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w77
//   Gap 5 (ami-partial): manifest.hooks as string path / array / inline object +
//     realpath de-dup + strict duplicate detection (the reference parity: pluginLoader.ts
//     L1652-1755). AMI previously resolved a SINGLE hooks file and never consulted a
//     manifest `hooks` array / inline form — closing that.
//   Gap 9 (ami-missing): plugin-recommendation input schema fields on a command
//     (sourceCommand provenance + pluginName/marketplaceName/pluginDescription;
//     the reference parity: PluginHintMenu Props).
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u029-w77-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writePlugin(name: string, manifest: Record<string, unknown>): string {
  const root = path.join(tmpDir, name);
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
  return root;
}

const INLINE_HOOKS = { PreToolUse: [{ matcher: '*', hooks: [{ type: 'command', command: 'echo hi' }] }] };

describe('resolvePluginManifestHooks (Gap 5)', () => {
  it('collects an inline hook OBJECT entry into inlineHooks', () => {
    const root = writePlugin('p', { name: 'p', hooks: INLINE_HOOKS });
    const res = resolvePluginManifestHooks(root, { hooks: INLINE_HOOKS });
    assert.deepEqual(res.hookFiles, []);
    assert.equal(res.inlineHooks.length, 1);
    assert.deepEqual(res.inlineHooks[0], INLINE_HOOKS);
    assert.deepEqual(res.errors, []);
  });

  it('resolves an ARRAY mixing a file-path string and an inline object', () => {
    const root = writePlugin('p', { name: 'p' });
    fs.writeFileSync(path.join(root, 'extra-hooks.json'), JSON.stringify({ hooks: {} }));
    const res = resolvePluginManifestHooks(root, { hooks: ['extra-hooks.json', INLINE_HOOKS] });
    assert.equal(res.hookFiles.length, 1);
    assert.equal(path.basename(res.hookFiles[0]!), 'extra-hooks.json');
    assert.equal(res.inlineHooks.length, 1);
    assert.deepEqual(res.errors, []);
  });

  it('DEDUPES a manifest entry that re-points at the already-loaded standard hooks file', () => {
    const root = writePlugin('p', { name: 'p' });
    const standard = path.join(root, 'hooks.json');
    fs.writeFileSync(standard, JSON.stringify({ hooks: {} }));
    const res = resolvePluginManifestHooks(root, { hooks: ['hooks.json'] }, { standardHooksFile: standard });
    assert.deepEqual(res.hookFiles, [], 'the standard file must not be loaded twice');
    assert.deepEqual(res.errors, [], 'a silent dedup is not an error when non-strict');
  });

  it('under STRICT, a duplicate of the standard hooks file is a hook-load-failed error', () => {
    const root = writePlugin('p', { name: 'p' });
    const standard = path.join(root, 'hooks.json');
    fs.writeFileSync(standard, JSON.stringify({ hooks: {} }));
    const res = resolvePluginManifestHooks(root, { hooks: ['hooks.json'] }, { standardHooksFile: standard, strict: true, pluginName: 'p' });
    assert.deepEqual(res.hookFiles, []);
    assert.equal(res.errors.length, 1);
    assert.equal(res.errors[0]!.type, 'hook-load-failed');
  });

  it('DEDUPES two manifest entries pointing at the same file (second dropped)', () => {
    const root = writePlugin('p', { name: 'p' });
    fs.writeFileSync(path.join(root, 'a.json'), JSON.stringify({ hooks: {} }));
    const res = resolvePluginManifestHooks(root, { hooks: ['a.json', 'a.json'] });
    assert.equal(res.hookFiles.length, 1);
  });

  it('a missing manifest hook file yields a path-not-found error', () => {
    const root = writePlugin('p', { name: 'p' });
    const res = resolvePluginManifestHooks(root, { hooks: ['nope.json'] }, { source: root, pluginName: 'p' });
    assert.deepEqual(res.hookFiles, []);
    assert.equal(res.errors.length, 1);
    assert.equal(res.errors[0]!.type, 'path-not-found');
    if (res.errors[0]!.type === 'path-not-found') assert.equal(res.errors[0]!.component, 'hooks');
  });

  it('a bare string manifest.hooks contributes nothing additional (handled by hooksFile)', () => {
    const root = writePlugin('p', { name: 'p' });
    fs.writeFileSync(path.join(root, 'h.json'), JSON.stringify({ hooks: {} }));
    const res = resolvePluginManifestHooks(root, { hooks: 'h.json' });
    assert.deepEqual(res.hookFiles, []);
    assert.deepEqual(res.inlineHooks, []);
    assert.deepEqual(res.errors, []);
  });

  it('an absent manifest.hooks is a no-op', () => {
    const root = writePlugin('p', { name: 'p' });
    const res = resolvePluginManifestHooks(root, {});
    assert.deepEqual(res, { hookFiles: [], inlineHooks: [], errors: [] });
  });
});

describe('discoverPlugins — manifest.hooks additional/inline (Gap 5)', () => {
  it('populates hookFiles and inlineHooks from a manifest hooks array', () => {
    const root = writePlugin('p', { name: 'p', hooks: ['extra.json', INLINE_HOOKS] });
    fs.writeFileSync(path.join(root, 'extra.json'), JSON.stringify({ hooks: {} }));
    const [bundle] = discoverPlugins(tmpDir) as PluginBundle[];
    assert.ok(bundle);
    assert.equal(bundle.hookFiles?.length, 1);
    assert.equal(path.basename(bundle.hookFiles![0]!), 'extra.json');
    assert.equal(bundle.inlineHooks?.length, 1);
  });

  it('does NOT re-list the standard hooks.json when the manifest hooks array re-points at it', () => {
    const root = writePlugin('p', { name: 'p', hooks: ['hooks.json'] });
    fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks: {} }));
    const [bundle] = discoverPlugins(tmpDir) as PluginBundle[];
    assert.ok(bundle);
    assert.equal(bundle.hooksFile, path.join(root, 'hooks.json'));
    assert.equal(bundle.hookFiles, undefined, 'the standard file is deduped out of the additional list');
  });

  it('leaves hookFiles/inlineHooks undefined for a plugin without a manifest hooks field', () => {
    writePlugin('p', { name: 'p' });
    const [bundle] = discoverPlugins(tmpDir) as PluginBundle[];
    assert.ok(bundle);
    assert.equal(bundle.hookFiles, undefined);
    assert.equal(bundle.inlineHooks, undefined);
  });
});

describe('validatePluginManifest — hooks forms (Gap 5)', () => {
  it('accepts a single inline hooks OBJECT', () => {
    const r = validatePluginManifest({ name: 'p', hooks: INLINE_HOOKS });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('accepts an array mixing a file-path string and an inline object', () => {
    const r = validatePluginManifest({ name: 'p', hooks: ['hooks/extra.json', INLINE_HOOKS] });
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('rejects a hooks array entry that escapes the plugin dir via ".."', () => {
    const r = validatePluginManifest({ name: 'p', hooks: ['../evil.json'] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'hooks[0]' && /escape/.test(e.message)));
  });

  it('rejects a numeric hooks array entry (not a path string or inline object)', () => {
    const r = validatePluginManifest({ name: 'p', hooks: [42] });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'hooks[0]'));
  });

  it('rejects a non-string / non-object hooks value', () => {
    const r = validatePluginManifest({ name: 'p', hooks: 123 });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'hooks'));
  });

  it('rejects a hooks string path that is absolute', () => {
    const r = validatePluginManifest({ name: 'p', hooks: '/etc/hooks.json' });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'hooks'));
  });
});

describe('command plugin recommendation (Gap 9)', () => {
  function writeCommand(file: string, body: string): string {
    const dir = path.join(tmpDir, 'commands');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, file), body);
    return dir;
  }

  it('parses recommend-plugin + recommend-marketplace from a .toml command', () => {
    const dir = writeCommand(
      'deploy.toml',
      'prompt = "do it"\nrecommend-plugin = "deployer"\nrecommend-marketplace = "acme"\nrecommend-plugin-description = "Ships your app"\n',
    );
    const [cmd] = loadCommands(dir) as CommandDefinition[];
    assert.ok(cmd);
    assert.deepEqual(cmd.recommendsPlugin, {
      pluginName: 'deployer',
      marketplaceName: 'acme',
      pluginDescription: 'Ships your app',
    });
  });

  it('parses recommend-plugin + recommend-marketplace from a .md command frontmatter', () => {
    const dir = writeCommand(
      'ship.md',
      '---\nrecommend-plugin: deployer\nrecommend-marketplace: acme\n---\nShip it\n',
    );
    const [cmd] = loadCommands(dir) as CommandDefinition[];
    assert.ok(cmd);
    assert.deepEqual(cmd.recommendsPlugin, { pluginName: 'deployer', marketplaceName: 'acme' });
  });

  it('produces no recommendation when only the plugin name is declared (marketplace missing)', () => {
    const dir = writeCommand('x.toml', 'prompt = "p"\nrecommend-plugin = "deployer"\n');
    const [cmd] = loadCommands(dir) as CommandDefinition[];
    assert.ok(cmd);
    assert.equal(cmd.recommendsPlugin, undefined);
  });

  it('buildPluginRecommendation stamps sourceCommand (displayName preferred over name)', () => {
    const hint = buildPluginRecommendation({
      name: 'ns:deploy',
      displayName: 'deploy',
      recommendsPlugin: { pluginName: 'deployer', marketplaceName: 'acme', pluginDescription: 'd' },
    });
    assert.deepEqual(hint, {
      pluginName: 'deployer',
      marketplaceName: 'acme',
      pluginDescription: 'd',
      sourceCommand: 'deploy',
    });
  });

  it('buildPluginRecommendation falls back to the lookup name when no displayName', () => {
    const hint = buildPluginRecommendation({
      name: 'deploy',
      recommendsPlugin: { pluginName: 'deployer', marketplaceName: 'acme' },
    });
    assert.ok(hint);
    assert.equal(hint!.sourceCommand, 'deploy');
    assert.equal(hint!.pluginDescription, undefined);
  });

  it('buildPluginRecommendation returns undefined for a command with no recommendation', () => {
    assert.equal(buildPluginRecommendation({ name: 'deploy' }), undefined);
  });
});
