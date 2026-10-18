import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { discoverPlugins } from '../src/plugins';
import type { PluginBundle } from '../src/plugins';

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-plugins-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function makePlugin(name: string, manifest: Record<string, unknown>, opts?: {
  skills?: boolean;
  agents?: boolean;
  hooks?: boolean;
}): string {
  const root = path.join(tmpDir, name);
  fs.mkdirSync(path.join(root, '.superinference-plugin'), { recursive: true });
  fs.writeFileSync(
    path.join(root, '.superinference-plugin', 'plugin.json'),
    JSON.stringify(manifest),
  );
  if (opts?.skills) {
    fs.mkdirSync(path.join(root, 'skills'), { recursive: true });
  }
  if (opts?.agents) {
    fs.mkdirSync(path.join(root, 'agents'), { recursive: true });
  }
  if (opts?.hooks) {
    fs.writeFileSync(path.join(root, 'hooks.json'), '{}');
  }
  return root;
}

describe('discoverPlugins', () => {
  it('returns empty array for non-existent directory', () => {
    const bundles = discoverPlugins(path.join(tmpDir, 'nope'));
    assert.deepEqual(bundles, []);
  });

  it('returns empty array for empty directory', () => {
    const bundles = discoverPlugins(tmpDir);
    assert.deepEqual(bundles, []);
  });

  it('discovers a plugin with full manifest', () => {
    makePlugin('my-plugin', { name: 'My Plugin', version: '1.2.3' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'My Plugin');
    assert.equal(bundles[0]!.version, '1.2.3');
  });

  it('defaults name to directory name when manifest omits it', () => {
    makePlugin('fallback-name', {});
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'fallback-name');
  });

  it('defaults version to 0.0.0 when manifest omits it', () => {
    makePlugin('no-version', { name: 'NoVer' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles[0]!.version, '0.0.0');
  });

  it('populates skillsDir when skills/ exists', () => {
    makePlugin('with-skills', { name: 'SK' }, { skills: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.skillsDir);
    assert.ok(bundles[0]!.skillsDir!.endsWith('skills'));
  });

  it('populates agentsDir when agents/ exists', () => {
    makePlugin('with-agents', { name: 'AG' }, { agents: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.agentsDir);
    assert.ok(bundles[0]!.agentsDir!.endsWith('agents'));
  });

  it('populates hooksFile when hooks.json exists', () => {
    makePlugin('with-hooks', { name: 'HK' }, { hooks: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.hooksFile);
    assert.ok(bundles[0]!.hooksFile!.endsWith('hooks.json'));
  });

  it('returns undefined for missing component dirs', () => {
    makePlugin('minimal', { name: 'Min' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles[0]!.skillsDir, undefined);
    assert.equal(bundles[0]!.agentsDir, undefined);
    assert.equal(bundles[0]!.hooksFile, undefined);
  });

  it('discovers multiple plugins', () => {
    makePlugin('a', { name: 'Alpha' });
    makePlugin('b', { name: 'Beta' });
    makePlugin('c', { name: 'Gamma' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 3);
    const names = bundles.map(b => b.name).sort();
    assert.deepEqual(names, ['Alpha', 'Beta', 'Gamma']);
  });

  it('skips directories without .superinference-plugin/plugin.json', () => {
    fs.mkdirSync(path.join(tmpDir, 'not-a-plugin'));
    makePlugin('real-plugin', { name: 'Real' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'Real');
  });

  it('skips files (non-directories) at the top level', () => {
    fs.writeFileSync(path.join(tmpDir, 'README.md'), 'hello');
    makePlugin('real', { name: 'Real' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
  });

  it('skips plugins with malformed JSON manifest', () => {
    const badRoot = path.join(tmpDir, 'bad-json');
    fs.mkdirSync(path.join(badRoot, '.superinference-plugin'), { recursive: true });
    fs.writeFileSync(path.join(badRoot, '.superinference-plugin', 'plugin.json'), 'not json');
    makePlugin('good', { name: 'Good' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'Good');
  });

  it('rejects symlink-escaped component paths', () => {
    const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-escape-'));
    fs.writeFileSync(path.join(outsideDir, 'hooks.json'), '{}');

    const pluginRoot = path.join(tmpDir, 'escape-plugin');
    fs.mkdirSync(path.join(pluginRoot, '.superinference-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(pluginRoot, '.superinference-plugin', 'plugin.json'),
      JSON.stringify({ name: 'escape' }),
    );
    // Symlink hooks.json to outside the plugin root
    try {
      fs.symlinkSync(path.join(outsideDir, 'hooks.json'), path.join(pluginRoot, 'hooks.json'));
    } catch {
      // Skip on platforms without symlink support
      fs.rmSync(outsideDir, { recursive: true, force: true });
      return;
    }

    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.hooksFile, undefined, 'symlink-escaped hooks.json must be rejected');

    fs.rmSync(outsideDir, { recursive: true, force: true });
  });

  it('sets root to the plugin directory path', () => {
    makePlugin('my-plugin', { name: 'MP' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles[0]!.root, path.join(tmpDir, 'my-plugin'));
  });
});
