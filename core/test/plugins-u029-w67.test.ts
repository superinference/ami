import { test, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  loadCommands,
  discoverPlugins,
  discoverPluginsDetailed,
  isENOENT,
  sanitizePluginIdSegment,
  generatePluginId,
  COMMAND_SOURCES,
  COMMAND_TYPES,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 w67 — additive command-definition metadata + plugin-bundle metadata +
// ENOENT I/O-error discrimination + deterministic plugin-id generation.
//
// Every feature below is a reference parity building block landed in
// core/core/src/plugins.ts (the types/validators/parsers layer):
//   - CommandDefinition: namespace, progressMessage, argNames, kind, aliases,
//     source, type, supportsNonInteractive
//   - PluginBundle: description, author, id (generatePluginId)
//   - isENOENT discrimination in discoverPluginsDetailed
// ---------------------------------------------------------------------------

let tmpDir: string;

before(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u029-w67-'));
});

after(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writeCmd(dir: string, rel: string, body: string): void {
  const full = path.join(dir, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  fs.writeFileSync(full, body);
}

// --- namespace (the reference parity: walkPluginMarkdown namespace array) ----------

test('namespace: top-level command has an empty namespace; nested tracks the sub-path', () => {
  const dir = path.join(tmpDir, 'ns-cmds');
  writeCmd(dir, 'top.toml', 'prompt = "top"');
  writeCmd(dir, 'git/commit.toml', 'prompt = "commit"');
  writeCmd(dir, 'git/remote/add.toml', 'prompt = "add remote"');
  const cmds = loadCommands(dir);
  const byName = new Map(cmds.map(c => [c.name, c]));
  assert.deepEqual(byName.get('top')!.namespace, []);
  assert.deepEqual(byName.get('git:commit')!.namespace, ['git']);
  assert.deepEqual(byName.get('git:remote:add')!.namespace, ['git', 'remote']);
});

test('namespace: a plugin-name prefix is NOT conflated with the on-disk sub-path', () => {
  const dir = path.join(tmpDir, 'ns-plugin');
  writeCmd(dir, 'git/commit.toml', 'prompt = "c"');
  const cmds = loadCommands(dir, 'my-plugin');
  const cmd = cmds[0]!;
  // name carries the plugin prefix; namespace carries only the folder path.
  assert.equal(cmd.name, 'my-plugin:git:commit');
  assert.deepEqual(cmd.namespace, ['git']);
});

// --- source / type discriminators -------------------------------------------

test('type defaults to "prompt" for a command file; source defaults to "plugin" under a bundle walk', () => {
  const dir = path.join(tmpDir, 'src-type');
  writeCmd(dir, 'a.toml', 'prompt = "a"');
  const bundleCmd = loadCommands(dir, 'pkg')[0]!;
  assert.equal(bundleCmd.type, 'prompt');
  assert.equal(bundleCmd.source, 'plugin');
  // A bare-directory load (no plugin name) leaves source unset.
  const bareCmd = loadCommands(dir)[0]!;
  assert.equal(bareCmd.type, 'prompt');
  assert.equal(bareCmd.source, undefined);
});

test('an explicitly declared valid source/type overrides the default; an invalid one is ignored', () => {
  const dir = path.join(tmpDir, 'src-type-decl');
  writeCmd(dir, 'ok.toml', ['prompt = "p"', 'source = "user"', 'type = "local-jsx"'].join('\n'));
  writeCmd(dir, 'bad.toml', ['prompt = "p"', 'source = "bogus"', 'type = "nope"'].join('\n'));
  const cmds = new Map(loadCommands(dir, 'pkg').map(c => [c.name, c]));
  const ok = cmds.get('pkg:ok')!;
  assert.equal(ok.source, 'user');
  assert.equal(ok.type, 'local-jsx');
  const bad = cmds.get('pkg:bad')!;
  // invalid enum values are rejected: source falls back to the bundle default,
  // type falls back to 'prompt'.
  assert.equal(bad.source, 'plugin');
  assert.equal(bad.type, 'prompt');
});

test('exported enum lists match the discriminator unions', () => {
  assert.deepEqual([...COMMAND_SOURCES], ['builtin', 'plugin', 'user', 'project']);
  assert.deepEqual([...COMMAND_TYPES], ['prompt', 'local', 'local-jsx']);
});

// --- progressMessage / argNames / kind / aliases / supportsNonInteractive ---

test('TOML command parses progressMessage, argNames, kind, aliases, supportsNonInteractive', () => {
  const dir = path.join(tmpDir, 'knobs-toml');
  writeCmd(dir, 'c.toml', [
    'prompt = "do it"',
    'progress-message = "working on it"',
    'arg-names = ["file", "mode"]',
    'kind = "workflow"',
    'aliases = ["cc", "do"]',
    'supports-non-interactive = false',
  ].join('\n'));
  const cmd = loadCommands(dir)[0]!;
  assert.equal(cmd.progressMessage, 'working on it');
  assert.deepEqual(cmd.argNames, ['file', 'mode']);
  assert.equal(cmd.kind, 'workflow');
  assert.deepEqual(cmd.aliases, ['cc', 'do']);
  assert.equal(cmd.supportsNonInteractive, false);
});

test('markdown command parses the same knobs from YAML frontmatter (camelCase accepted)', () => {
  const dir = path.join(tmpDir, 'knobs-md');
  writeCmd(dir, 'c.md', [
    '---',
    'progressMessage: committing',
    'argNames: a, b',
    'kind: workflow',
    'aliases: x, y',
    'supportsNonInteractive: true',
    '---',
    'The body is the prompt.',
  ].join('\n'));
  const cmd = loadCommands(dir)[0]!;
  assert.equal(cmd.progressMessage, 'committing');
  assert.deepEqual(cmd.argNames, ['a', 'b']);
  assert.equal(cmd.kind, 'workflow');
  assert.deepEqual(cmd.aliases, ['x', 'y']);
  assert.equal(cmd.supportsNonInteractive, true);
});

test('knobs are OMITTED when the file declares none (no spurious fields)', () => {
  const dir = path.join(tmpDir, 'knobs-none');
  writeCmd(dir, 'c.toml', 'prompt = "plain"');
  const cmd = loadCommands(dir)[0]!;
  assert.equal('progressMessage' in cmd, false);
  assert.equal('argNames' in cmd, false);
  assert.equal('kind' in cmd, false);
  assert.equal('aliases' in cmd, false);
  assert.equal('supportsNonInteractive' in cmd, false);
});

// --- plugin manifest description / author / id on the bundle ----------------

function writePlugin(root: string, manifest: Record<string, unknown>): void {
  const dir = path.join(root, '.ami-plugin');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'plugin.json'), JSON.stringify(manifest));
}

test('discoverPlugins surfaces the manifest description on the bundle', () => {
  const base = path.join(tmpDir, 'disc-desc');
  writePlugin(path.join(base, 'alpha'), { name: 'alpha', version: '1.0.0', description: '  A neat plugin  ' });
  writePlugin(path.join(base, 'beta'), { name: 'beta', version: '1.0.0' });
  const bundles = new Map(discoverPlugins(base).map(b => [b.name, b]));
  assert.equal(bundles.get('alpha')!.description, 'A neat plugin');
  assert.equal(bundles.get('beta')!.description, undefined);
});

test('discoverPlugins sets author + deterministic id from author.name + name', () => {
  const base = path.join(tmpDir, 'disc-id');
  writePlugin(path.join(base, 'strp'), { name: 'my-plugin', version: '1', author: 'Ada Lovelace' });
  writePlugin(path.join(base, 'objp'), { name: 'other', version: '1', author: { name: 'Grace Hopper', email: 'g@h.io' } });
  writePlugin(path.join(base, 'noauth'), { name: 'anon', version: '1' });
  const bundles = new Map(discoverPlugins(base).map(b => [b.name, b]));
  assert.equal(bundles.get('my-plugin')!.author, 'Ada Lovelace');
  assert.equal(bundles.get('my-plugin')!.id, 'ada-lovelace.my-plugin');
  assert.equal(bundles.get('other')!.author, 'Grace Hopper');
  assert.equal(bundles.get('other')!.id, 'grace-hopper.other');
  assert.equal(bundles.get('anon')!.author, undefined);
  assert.equal(bundles.get('anon')!.id, undefined);
});

// --- generatePluginId / sanitizePluginIdSegment (the reference parity) -------------

test('sanitizePluginIdSegment applies the ordered Claude sanitize pipeline', () => {
  assert.equal(sanitizePluginIdSegment('Ada Lovelace'), 'ada-lovelace');
  assert.equal(sanitizePluginIdSegment('  --Hello__World!!  '), 'hello__world');
  assert.equal(sanitizePluginIdSegment('A  B   C'), 'a-b-c');
  // Non-[a-z0-9-_.] chars (accents, slash) are DROPPED, not replaced with '-'.
  assert.equal(sanitizePluginIdSegment('Café/Über'), 'cafber');
  assert.equal(sanitizePluginIdSegment('v1.2.3'), 'v1.2.3');
});

test('generatePluginId composes sanitized author.name and supports a prefix', () => {
  assert.equal(generatePluginId('Ada Lovelace', 'My Plugin'), 'ada-lovelace.my-plugin');
  assert.equal(generatePluginId('Ada', 'P', 'local.unpacked'), 'local.unpacked.ada.p');
  assert.equal(generatePluginId('Ada', 'P', 'local.dxt'), 'local.dxt.ada.p');
  // Deterministic: identical input → identical id.
  assert.equal(generatePluginId('x', 'y'), generatePluginId('x', 'y'));
});

// --- isENOENT discrimination in discoverPluginsDetailed ---------------------

test('isENOENT returns true only for an ENOENT-coded error', () => {
  assert.equal(isENOENT({ code: 'ENOENT' }), true);
  assert.equal(isENOENT({ code: 'EACCES' }), false);
  assert.equal(isENOENT(new Error('x')), false);
  assert.equal(isENOENT(undefined), false);
  assert.equal(isENOENT('ENOENT'), false);
});

test('discoverPluginsDetailed: a missing plugins dir (ENOENT) is NOT a failure', () => {
  const missing = path.join(tmpDir, 'does', 'not', 'exist');
  const res = discoverPluginsDetailed(missing);
  assert.deepEqual(res.bundles, []);
  assert.deepEqual(res.failures, []);
});

test('discoverPluginsDetailed: a real I/O error (reading a FILE as a dir) surfaces a failure instead of silently returning empty', () => {
  const filePath = path.join(tmpDir, 'not-a-dir.txt');
  fs.writeFileSync(filePath, 'hello');
  const res = discoverPluginsDetailed(filePath);
  assert.deepEqual(res.bundles, []);
  assert.equal(res.failures.length, 1);
  assert.equal(res.failures[0]!.root, filePath);
  assert.match(res.failures[0]!.reason, /Failed to read plugins directory/);
});
