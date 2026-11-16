import { test, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  loadCommands,
  discoverPlugins,
  discoverPluginsDetailed,
  getPluginErrorMessage,
  mergePluginMcpServers,
  type PluginError,
  type PluginMcpServer,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w68 — Claude-Code parity building blocks in core/core/src/plugins.ts:
//   - Typed discriminated PluginError union + getPluginErrorMessage formatter,
//     produced by discoverPluginsDetailed alongside the human reason.
//   - mergePluginMcpServers: cross-plugin MCP-server duplicate suppression
//     (dedup by launch target) + mcp-server-suppressed-duplicate error.
//   - Manifest commandsPaths/skillsPaths: additional command/skill locations,
//     path-hardened, consumed by loadCommands(extraPaths).
// ---------------------------------------------------------------------------

let tmpDir: string;

before(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'plugins-u029-w68-'));
});

after(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writePlugin(root: string, manifest: Record<string, unknown>): void {
  const dir = path.join(root, '.ami-plugin');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'plugin.json'), JSON.stringify(manifest));
}

function writeFile(p: string, body: string): void {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body);
}

// --- getPluginErrorMessage: exhaustive over the union -----------------------

test('getPluginErrorMessage renders every PluginError variant', () => {
  const cases: Array<[PluginError, RegExp]> = [
    [{ type: 'manifest-parse-error', source: 's', manifestPath: '/m.json', parseError: 'Unexpected token' }, /Manifest parse error: Unexpected token/],
    [{ type: 'manifest-validation-error', source: 's', manifestPath: '/m.json', validationErrors: ['name: bad', 'version: bad'] }, /Manifest validation failed: name: bad, version: bad/],
    [{ type: 'path-not-found', source: 's', path: 'skills', component: 'skills' }, /Path not found: skills \(skills\)/],
    [{ type: 'component-load-failed', source: 's', plugin: 'p', component: 'commands', path: 'commands/x.toml', reason: 'boom' }, /commands load failed from commands\/x.toml: boom/],
    [{ type: 'lsp-config-invalid', source: 's', plugin: 'p', serverName: 'ts', validationError: 'bad command' }, /Plugin "p" has invalid LSP server config for "ts": bad command/],
    [{ type: 'mcp-config-invalid', source: 's', plugin: 'p', serverName: 'db', validationError: 'bad url' }, /Plugin "p" has invalid MCP server config for "db": bad url/],
    [{ type: 'mcp-server-suppressed-duplicate', source: 's', plugin: 'p', serverName: 'db', duplicateOf: 'plugin:other' }, /MCP server "db" skipped — same command\/URL as server provided by plugin "other"/],
    [{ type: 'mcp-server-suppressed-duplicate', source: 's', plugin: 'p', serverName: 'db', duplicateOf: 'already' }, /MCP server "db" skipped — same command\/URL as already-configured "already"/],
    [{ type: 'hook-load-failed', source: 's', plugin: 'p', hookPath: 'hooks.json', reason: 'oops' }, /Hook load failed: oops/],
    [{ type: 'generic-error', source: 's', error: 'something went wrong' }, /something went wrong/],
  ];
  for (const [err, re] of cases) {
    assert.match(getPluginErrorMessage(err), re, `variant ${err.type}`);
  }
});

// --- discoverPluginsDetailed attaches a TYPED error to each failure ---------

test('discoverPluginsDetailed tags a corrupt manifest with a typed manifest-parse-error', () => {
  const base = path.join(tmpDir, 'disc-corrupt');
  const root = path.join(base, 'broken');
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), '{ not json ');
  const { bundles, failures } = discoverPluginsDetailed(base);
  assert.equal(bundles.length, 0);
  assert.equal(failures.length, 1);
  const f = failures[0]!;
  // Human reason preserved (back-compat with existing consumers).
  assert.match(f.reason, /Corrupt plugin.json/);
  // Typed error lets a consumer branch WITHOUT string-matching the reason.
  assert.ok(f.error);
  assert.equal(f.error!.type, 'manifest-parse-error');
  assert.equal(f.error!.source, root);
  assert.equal((f.error as Extract<PluginError, { type: 'manifest-parse-error' }>).manifestPath, path.join(root, '.ami-plugin', 'plugin.json'));
});

test('discoverPluginsDetailed tags an unsafe/traversal manifest with a typed manifest-validation-error', () => {
  const base = path.join(tmpDir, 'disc-unsafe');
  writePlugin(path.join(base, 'evil'), { name: 'evil', version: '1.0.0', skills: '../../etc' });
  const { failures } = discoverPluginsDetailed(base);
  const f = failures.find(x => x.error?.type === 'manifest-validation-error');
  assert.ok(f, 'expected a manifest-validation-error failure');
  const err = f!.error as Extract<PluginError, { type: 'manifest-validation-error' }>;
  assert.equal(err.plugin, 'evil');
  assert.ok(err.validationErrors.length >= 1);
  assert.ok(err.validationErrors.some(m => /\.\.|relative/.test(m)));
  // The reason is the joined validation errors (existing behavior).
  assert.match(f!.reason, /Invalid plugin.json/);
});

test('discoverPluginsDetailed tags a non-object manifest with manifest-validation-error', () => {
  const base = path.join(tmpDir, 'disc-array');
  const root = path.join(base, 'arr');
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), '[1,2,3]');
  const { failures } = discoverPluginsDetailed(base);
  assert.equal(failures.length, 1);
  assert.equal(failures[0]!.error!.type, 'manifest-validation-error');
});

// --- mergePluginMcpServers: cross-plugin duplicate suppression --------------

test('mergePluginMcpServers dedups by stdio command+args across plugins', () => {
  const a: PluginMcpServer = { command: 'node', args: ['server.js'] };
  const b: PluginMcpServer = { command: 'node', args: ['server.js'] }; // same launch target
  const { servers, errors } = mergePluginMcpServers([
    { plugin: 'alpha', source: '/a', servers: { shared: a } },
    { plugin: 'beta', source: '/b', servers: { shared: b } },
  ]);
  assert.deepEqual(Object.keys(servers), ['shared']);
  assert.equal(servers.shared, a); // first occurrence wins
  assert.equal(errors.length, 1);
  const e = errors[0]!;
  assert.equal(e.type, 'mcp-server-suppressed-duplicate');
  assert.equal((e as Extract<PluginError, { type: 'mcp-server-suppressed-duplicate' }>).plugin, 'beta');
  assert.match(getPluginErrorMessage(e), /skipped — same command\/URL/);
});

test('mergePluginMcpServers dedups by url and keeps distinct servers', () => {
  const { servers, errors } = mergePluginMcpServers([
    { plugin: 'alpha', source: '/a', servers: { one: { url: 'https://x.test/mcp' }, two: { command: 'db', args: ['--port', '1'] } } },
    { plugin: 'beta', source: '/b', servers: { dup: { url: 'https://x.test/mcp' }, three: { command: 'db', args: ['--port', '2'] } } },
  ]);
  // one/two from alpha + three from beta survive; dup (same url as one) suppressed.
  assert.deepEqual(Object.keys(servers).sort(), ['one', 'three', 'two']);
  assert.equal(errors.length, 1);
  assert.equal((errors[0] as Extract<PluginError, { type: 'mcp-server-suppressed-duplicate' }>).serverName, 'dup');
});

test('mergePluginMcpServers suppresses a plugin server duplicating an existing configured one', () => {
  const existing: Record<string, PluginMcpServer> = { preconfigured: { command: 'node', args: ['s.js'] } };
  const { servers, errors } = mergePluginMcpServers(
    [{ plugin: 'alpha', source: '/a', servers: { mine: { command: 'node', args: ['s.js'] } } }],
    existing,
  );
  assert.deepEqual(Object.keys(servers), ['preconfigured']);
  assert.equal(errors.length, 1);
  const e = errors[0] as Extract<PluginError, { type: 'mcp-server-suppressed-duplicate' }>;
  assert.equal(e.duplicateOf, 'preconfigured');
  assert.match(getPluginErrorMessage(e), /already-configured "preconfigured"/);
});

test('mergePluginMcpServers flags a same-name self-collision as plugin:<name>', () => {
  // Two distinct-target servers sharing a NAME: the second is a key collision
  // against a plugin-provided server (not an existing one) → duplicateOf plugin:<plugin>.
  const { errors } = mergePluginMcpServers([
    { plugin: 'alpha', source: '/a', servers: { db: { command: 'x', args: ['1'] } } },
    { plugin: 'alpha', source: '/a', servers: { db: { command: 'x', args: ['1'] } } },
  ]);
  assert.equal(errors.length, 1);
  assert.equal((errors[0] as Extract<PluginError, { type: 'mcp-server-suppressed-duplicate' }>).duplicateOf, 'plugin:alpha');
});

// --- commandsPaths / skillsPaths manifest fields ----------------------------

test('discoverPlugins resolves + hardens manifest commandsPaths/skillsPaths', () => {
  const base = path.join(tmpDir, 'cmdpaths');
  const root = path.join(base, 'p');
  writePlugin(root, {
    name: 'p', version: '1.0.0',
    commandsPaths: ['extra-cmds', 'missing-dir', '../escape'],
    skillsPaths: 'extra-skills',
  });
  writeFile(path.join(root, 'extra-cmds', 'deploy.toml'), 'prompt = "do it"\n');
  fs.mkdirSync(path.join(root, 'extra-skills'), { recursive: true });
  const bundle = discoverPlugins(base).find(b => b.name === 'p')!;
  // Only existing, contained paths survive; '..' escape and missing dir dropped.
  assert.deepEqual(bundle.commandsPaths, [path.join(root, 'extra-cmds')]);
  assert.deepEqual(bundle.skillsPaths, [path.join(root, 'extra-skills')]);
});

test('discoverPlugins leaves commandsPaths/skillsPaths undefined when none declared', () => {
  const base = path.join(tmpDir, 'nopaths');
  writePlugin(path.join(base, 'p'), { name: 'p', version: '1.0.0' });
  const bundle = discoverPlugins(base).find(b => b.name === 'p')!;
  assert.equal(bundle.commandsPaths, undefined);
  assert.equal(bundle.skillsPaths, undefined);
});

test('loadCommands walks extraPaths: directory + single file + missing', () => {
  const base = path.join(tmpDir, 'loadextra');
  const primary = path.join(base, 'commands');
  writeFile(path.join(primary, 'main.toml'), 'prompt = "main"\n');
  const extraDir = path.join(base, 'more');
  writeFile(path.join(extraDir, 'sub', 'nested.toml'), 'prompt = "nested"\n');
  const extraFile = path.join(base, 'solo.md');
  writeFile(extraFile, '# Solo\n\nrun solo\n');
  const cmds = loadCommands(primary, 'myplugin', [extraDir, extraFile, path.join(base, 'does-not-exist.toml')]);
  const names = cmds.map(c => c.name).sort();
  // primary main + nested (namespaced by its subdir) + solo file.
  assert.deepEqual(names, ['myplugin:main', 'myplugin:solo', 'myplugin:sub:nested']);
  const solo = cmds.find(c => c.name === 'myplugin:solo')!;
  assert.deepEqual(solo.namespace, []);
  assert.equal(solo.source, 'plugin');
});

test('loadCommands without extraPaths is unchanged (back-compat)', () => {
  const base = path.join(tmpDir, 'noextra');
  const primary = path.join(base, 'commands');
  writeFile(path.join(primary, 'only.toml'), 'prompt = "only"\n');
  const cmds = loadCommands(primary);
  assert.deepEqual(cmds.map(c => c.name), ['only']);
});
