import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { validatePluginManifest, discoverPluginsDetailed } from '../src/plugins';

test('validatePluginManifest accepts a minimal valid manifest', () => {
  const r = validatePluginManifest({ name: 'myplugin', version: '1.0.0' });
  assert.equal(r.valid, true);
  assert.deepEqual(r.errors, []);
});

test('validatePluginManifest rejects non-object and missing name', () => {
  assert.equal(validatePluginManifest(null).valid, false);
  assert.equal(validatePluginManifest([]).valid, false);
  const r = validatePluginManifest({ version: '1' });
  assert.equal(r.valid, false);
  assert.ok(r.errors.some((e) => e.path === 'name'));
});

test('validatePluginManifest rejects unsafe name and non-string version', () => {
  assert.ok(validatePluginManifest({ name: '../evil' }).errors.some((e) => /Unsafe plugin name/.test(e.message)));
  assert.ok(validatePluginManifest({ name: 'ok', version: 1 }).errors.some((e) => e.path === 'version'));
});

test('validatePluginManifest flags path-traversal in component paths', () => {
  const r = validatePluginManifest({ name: 'p', skills: '../../etc' });
  assert.equal(r.valid, false);
  assert.ok(r.errors.some((e) => e.path === 'skills' && /escape/.test(e.message)));
  const abs = validatePluginManifest({ name: 'p', commands: '/abs/path' });
  assert.ok(abs.errors.some((e) => e.path === 'commands' && /relative/.test(e.message)));
});

test('validatePluginManifest warns about marketplace-only fields', () => {
  const r = validatePluginManifest({ name: 'p', category: 'x', tags: ['a'] });
  assert.equal(r.valid, true);
  assert.ok(r.warnings.some((w) => w.path === 'category'));
  assert.ok(r.warnings.some((w) => w.path === 'tags'));
});

let tmp: string;
before(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'plugin-detailed-'));
});
after(() => fs.rmSync(tmp, { recursive: true, force: true }));

test('discoverPluginsDetailed reports structured failures for broken JSON', () => {
  const good = path.join(tmp, 'good');
  fs.mkdirSync(path.join(good, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(good, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'good', version: '1' }));

  const bad = path.join(tmp, 'bad');
  fs.mkdirSync(path.join(bad, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(bad, '.ami-plugin', 'plugin.json'), '{ not json');

  const arr = path.join(tmp, 'arr');
  fs.mkdirSync(path.join(arr, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(arr, '.ami-plugin', 'plugin.json'), '[]');

  const { bundles, failures } = discoverPluginsDetailed(tmp);
  assert.deepEqual(bundles.map((b) => b.name), ['good']);
  assert.equal(failures.length, 2);
  assert.ok(failures.some((f) => f.root === bad && /Invalid plugin.json/.test(f.reason)));
  assert.ok(failures.some((f) => f.root === arr && /must be a JSON object/.test(f.reason)));
});

test('discoverPluginsDetailed keeps lenient name/version fallback', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plugin-lenient-'));
  const p = path.join(dir, 'noname');
  fs.mkdirSync(path.join(p, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(p, '.ami-plugin', 'plugin.json'), '{}');
  const { bundles, failures } = discoverPluginsDetailed(dir);
  assert.equal(failures.length, 0);
  assert.equal(bundles[0].name, 'noname');
  assert.equal(bundles[0].version, '0.0.0');
  fs.rmSync(dir, { recursive: true, force: true });
});
