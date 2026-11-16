import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  discoverPlugins,
  loadCommands,
  validatePluginContents,
  validatePlugin,
} from '../src/plugins';

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-plugins-u029-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writeFile(p: string, content: string): void {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, content);
}

// ---------------------------------------------------------------------------
// Gap: Case-insensitive SKILL.md and .md/.toml extension matching
// (walkPluginMarkdown.ts:L5,L35,L40,L54)
// ---------------------------------------------------------------------------
describe('U029 — case-insensitive command/skill file matching', () => {
  it('loads a command from an upper-case .TOML extension', () => {
    const dir = path.join(tmpDir, 'commands');
    writeFile(path.join(dir, 'Deploy.TOML'), 'prompt = "do the deploy"\n');
    const cmds = loadCommands(dir);
    const names = cmds.map(c => c.name);
    assert.ok(names.includes('Deploy'), `expected Deploy, got ${names.join(',')}`);
    assert.equal(cmds[0]!.prompt, 'do the deploy');
  });

  it('parses an upper-case .MD command file as markdown (body = prompt)', () => {
    const dir = path.join(tmpDir, 'commands');
    writeFile(path.join(dir, 'Review.MD'), '---\ndescription: Review code\n---\nReview the diff.');
    const cmds = loadCommands(dir);
    const review = cmds.find(c => c.name === 'Review');
    assert.ok(review, 'expected a Review command');
    assert.equal(review!.prompt, 'Review the diff.');
    assert.equal(review!.description, 'Review code');
  });

  it('recognizes a lower-case skill.md in a skill subdirectory (no missing-SKILL.md warning)', () => {
    const pluginDir = path.join(tmpDir, 'plug');
    writeFile(path.join(pluginDir, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'plug', version: '1.0.0' }));
    writeFile(path.join(pluginDir, 'skills', 'foo', 'skill.md'), '---\nname: foo\n---\nSkill body here.');
    const res = validatePluginContents(pluginDir);
    const missing = res.warnings.find(w => /no SKILL\.md/.test(w.message));
    assert.equal(missing, undefined, `unexpected missing-SKILL.md warning: ${JSON.stringify(res.warnings)}`);
  });
});

// ---------------------------------------------------------------------------
// Gap: Empty-commands-directory affordance warning naming the expected format
// (loadPluginCommands.ts:L497-L502)
// ---------------------------------------------------------------------------
describe('U029 — empty commands directory warning', () => {
  it('warns when a commands/ directory exists but has no loadable commands', () => {
    const pluginDir = path.join(tmpDir, 'plug');
    writeFile(path.join(pluginDir, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'plug', version: '1.0.0' }));
    fs.mkdirSync(path.join(pluginDir, 'commands'), { recursive: true });
    const res = validatePluginContents(pluginDir);
    const warn = res.warnings.find(w => w.path === 'commands' && /no loadable commands/.test(w.message));
    assert.ok(warn, `expected empty-commands warning, got ${JSON.stringify(res.warnings)}`);
    assert.match(warn!.message, /\.toml|\.md/i);
  });

  it('does NOT warn when commands live only in a subdirectory', () => {
    const pluginDir = path.join(tmpDir, 'plug');
    writeFile(path.join(pluginDir, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'plug', version: '1.0.0' }));
    writeFile(path.join(pluginDir, 'commands', 'git', 'commit.toml'), 'description = "c"\nprompt = "commit it"\n');
    const res = validatePluginContents(pluginDir);
    const warn = res.warnings.find(w => w.path === 'commands' && /no loadable commands/.test(w.message));
    assert.equal(warn, undefined, `unexpected empty-commands warning: ${JSON.stringify(res.warnings)}`);
  });
});

// ---------------------------------------------------------------------------
// Gap: Legacy root-level plugin.json manifest fallback
// (pluginLoader.ts:L979-981)
// ---------------------------------------------------------------------------
describe('U029 — legacy root-level plugin.json fallback', () => {
  it('discovers a plugin with only a root-level plugin.json manifest', () => {
    const root = path.join(tmpDir, 'legacy');
    writeFile(path.join(root, 'plugin.json'), JSON.stringify({ name: 'legacy', version: '2.1.0' }));
    const bundles = discoverPlugins(tmpDir);
    const found = bundles.find(b => b.name === 'legacy');
    assert.ok(found, `expected to discover legacy plugin, got ${bundles.map(b => b.name).join(',')}`);
    assert.equal(found!.version, '2.1.0');
  });

  it('validatePlugin accepts a root-level plugin.json manifest', () => {
    const root = path.join(tmpDir, 'legacy');
    writeFile(path.join(root, 'plugin.json'), JSON.stringify({ name: 'legacy', version: '2.1.0' }));
    const res = validatePlugin(root);
    const noManifest = res.errors.find(e => /No plugin manifest/.test(e.message));
    assert.equal(noManifest, undefined, `should find root-level manifest, got ${JSON.stringify(res.errors)}`);
  });

  it('prefers .ami-plugin over the root-level manifest', () => {
    const root = path.join(tmpDir, 'both');
    writeFile(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'preferred', version: '9.9.9' }));
    writeFile(path.join(root, 'plugin.json'), JSON.stringify({ name: 'legacy', version: '0.0.1' }));
    const bundles = discoverPlugins(tmpDir);
    const found = bundles.find(b => b.root === root);
    assert.ok(found);
    assert.equal(found!.name, 'preferred');
    assert.equal(found!.version, '9.9.9');
  });
});
