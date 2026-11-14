import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { validatePluginContents, validatePlugin, discoverPluginsDetailed } from '../src/plugins';

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pvc-')); });
afterEach(() => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ } });

function mkplugin(dir: string, manifest: Record<string, unknown> = { name: 'p', version: '1.0.0' }): void {
  fs.mkdirSync(path.join(dir, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami-plugin', 'plugin.json'), JSON.stringify(manifest));
}

describe('validatePluginContents', () => {
  it('passes a well-formed bundle with skills/commands/hooks', () => {
    mkplugin(tmp);
    fs.mkdirSync(path.join(tmp, 'skills', 'a'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'skills', 'a', 'SKILL.md'), '---\nname: a\n---\nBody.\n');
    fs.mkdirSync(path.join(tmp, 'commands'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'commands', 'c.toml'), 'description = "d"\nprompt = "p"\n');
    fs.writeFileSync(path.join(tmp, 'hooks.json'), JSON.stringify({ hooks: {} }));
    const r = validatePluginContents(tmp);
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('errors on an empty skill body and a command TOML missing prompt', () => {
    mkplugin(tmp);
    fs.mkdirSync(path.join(tmp, 'skills', 'bad'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'skills', 'bad', 'SKILL.md'), '---\nname: bad\n---\n');
    fs.mkdirSync(path.join(tmp, 'commands'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'commands', 'c.toml'), 'description = "d"\n');
    const r = validatePluginContents(tmp);
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /body is empty/.test(e.message)));
    assert.ok(r.errors.some(e => /description.*prompt/.test(e.message)));
  });

  it('errors on invalid hooks JSON', () => {
    mkplugin(tmp);
    fs.writeFileSync(path.join(tmp, 'hooks.json'), '{not json');
    const r = validatePluginContents(tmp);
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /Invalid hooks JSON/.test(e.message)));
  });

  it('flags a component directory that escapes the plugin root via symlink', () => {
    mkplugin(tmp);
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pvc-out-'));
    fs.symlinkSync(outside, path.join(tmp, 'skills'), 'dir');
    const r = validatePluginContents(tmp);
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'skills' && /escapes the plugin directory/.test(e.message)));
    fs.rmSync(outside, { recursive: true, force: true });
  });

  it('tolerates a plugin that ships no component dirs', () => {
    mkplugin(tmp);
    assert.equal(validatePluginContents(tmp).valid, true);
  });
});

describe('validatePlugin (manifest + contents)', () => {
  it('merges manifest and content validation', () => {
    mkplugin(tmp, { name: 'p', version: '1.0.0', category: 'x' }); // category = marketplace-only warning
    fs.mkdirSync(path.join(tmp, 'commands'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'commands', 'c.toml'), 'description = "d"\n'); // missing prompt = error
    const r = validatePlugin(tmp);
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.path === 'category'));
    assert.ok(r.errors.some(e => e.path === 'commands/c.toml'));
  });

  it('errors when no manifest is present', () => {
    const r = validatePlugin(tmp);
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => /No plugin manifest/.test(e.message)));
  });
});

describe('discoverPluginsDetailed — corrupt vs invalid diagnostics', () => {
  it('reports unparseable JSON as corrupt and a security-unsafe name as a failure', () => {
    const corrupt = path.join(tmp, 'corrupt');
    fs.mkdirSync(path.join(corrupt, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(corrupt, '.ami-plugin', 'plugin.json'), '{ not json');

    const unsafe = path.join(tmp, 'unsafe');
    fs.mkdirSync(path.join(unsafe, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(unsafe, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: '../evil', version: '1.0.0' }));

    const good = path.join(tmp, 'good');
    fs.mkdirSync(path.join(good, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(good, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'good', version: '1.0.0' }));

    const { bundles, failures } = discoverPluginsDetailed(tmp);
    assert.deepEqual(bundles.map(b => b.name), ['good']);
    assert.ok(failures.some(f => f.root === corrupt && /Corrupt plugin.json/.test(f.reason)));
    assert.ok(failures.some(f => f.root === unsafe && /Invalid plugin.json.*Unsafe plugin name/.test(f.reason)));
  });
});
