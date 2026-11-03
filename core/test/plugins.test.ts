import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { discoverPlugins, loadCommands } from '../src/plugins';
import type { PluginBundle, CommandDefinition } from '../src/plugins';

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
    assert.equal(bundles[0]!.commandsDir, undefined);
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

  it('skips directories without .superinference-plugin or .claude-plugin manifest', () => {
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

  it('discovers a plugin via .claude-plugin fallback', () => {
    const root = path.join(tmpDir, 'claude-plugin');
    fs.mkdirSync(path.join(root, '.claude-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.claude-plugin', 'plugin.json'),
      JSON.stringify({ name: 'Claude Plugin', version: '2.0.0' }),
    );
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'Claude Plugin');
    assert.equal(bundles[0]!.version, '2.0.0');
  });

  it('prefers .superinference-plugin over .claude-plugin when both exist', () => {
    const root = path.join(tmpDir, 'dual-plugin');
    fs.mkdirSync(path.join(root, '.superinference-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.superinference-plugin', 'plugin.json'),
      JSON.stringify({ name: 'SI Plugin' }),
    );
    fs.mkdirSync(path.join(root, '.claude-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.claude-plugin', 'plugin.json'),
      JSON.stringify({ name: 'CC Plugin' }),
    );
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'SI Plugin');
  });

  it('populates commandsDir when commands/ exists', () => {
    makePlugin('with-commands', { name: 'CMD' }, { skills: false, agents: false, hooks: false });
    const root = path.join(tmpDir, 'with-commands');
    fs.mkdirSync(path.join(root, 'commands'), { recursive: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.commandsDir);
    assert.ok(bundles[0]!.commandsDir!.endsWith('commands'));
  });
});

describe('loadCommands', () => {
  it('returns empty array for non-existent directory', () => {
    const cmds = loadCommands(path.join(tmpDir, 'nope'));
    assert.deepEqual(cmds, []);
  });

  it('returns empty array for directory with no .toml files', () => {
    const dir = path.join(tmpDir, 'no-toml');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'README.md'), 'hello');
    const cmds = loadCommands(dir);
    assert.deepEqual(cmds, []);
  });

  it('loads a valid command from a .toml file', () => {
    const dir = path.join(tmpDir, 'commands');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'review.toml'), [
      'description = "Quality review of the current changes"',
      'prompt = "Review the current code changes carefully"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'review');
    assert.equal(cmds[0]!.description, 'Quality review of the current changes');
    assert.equal(cmds[0]!.prompt, 'Review the current code changes carefully');
    assert.equal(cmds[0]!.filePath, path.join(dir, 'review.toml'));
  });

  it('handles backslash escapes in TOML values', () => {
    const dir = path.join(tmpDir, 'esc-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'test.toml'), [
      'description = "A \\"quoted\\" desc"',
      'prompt = "Line one\\nLine two\\ttabbed"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.description, 'A "quoted" desc');
    assert.equal(cmds[0]!.prompt, 'Line one\nLine two\ttabbed');
  });

  it('skips .toml files missing required fields', () => {
    const dir = path.join(tmpDir, 'partial-cmds');
    fs.mkdirSync(dir, { recursive: true });
    // Missing prompt
    fs.writeFileSync(path.join(dir, 'no-prompt.toml'), 'description = "desc only"');
    // Missing description
    fs.writeFileSync(path.join(dir, 'no-desc.toml'), 'prompt = "prompt only"');
    // Valid
    fs.writeFileSync(path.join(dir, 'valid.toml'), [
      'description = "Valid"',
      'prompt = "Do something"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'valid');
  });

  it('loads multiple commands from multiple .toml files', () => {
    const dir = path.join(tmpDir, 'multi-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'alpha.toml'), [
      'description = "Alpha command"',
      'prompt = "Do alpha"',
    ].join('\n'));
    fs.writeFileSync(path.join(dir, 'beta.toml'), [
      'description = "Beta command"',
      'prompt = "Do beta"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 2);
    const names = cmds.map(c => c.name).sort();
    assert.deepEqual(names, ['alpha', 'beta']);
  });

  it('ignores non-.toml files in the directory', () => {
    const dir = path.join(tmpDir, 'mixed-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'readme.md'), '# Commands');
    fs.writeFileSync(path.join(dir, 'valid.toml'), [
      'description = "Valid"',
      'prompt = "Run"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'valid');
  });

  it('handles backslash-backslash escape', () => {
    const dir = path.join(tmpDir, 'bslash-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'bs.toml'), [
      'description = "Path is C:\\\\Users"',
      'prompt = "Use path C:\\\\Users"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.description, 'Path is C:\\Users');
    assert.equal(cmds[0]!.prompt, 'Use path C:\\Users');
  });

  it('handles TOML values without a closing quote', () => {
    const dir = path.join(tmpDir, 'noclose-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'open.toml'), [
      'description = "no closing quote',
      'prompt = "also no close',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    // parseTomlString returns what it accumulated before EOF
    assert.equal(cmds[0]!.description, 'no closing quote');
    assert.equal(cmds[0]!.prompt, 'also no close');
  });

  it('skips unreadable .toml files gracefully', () => {
    const dir = path.join(tmpDir, 'unreadable-cmds');
    fs.mkdirSync(dir, { recursive: true });
    // Create a directory named like a .toml file so readFileSync throws
    fs.mkdirSync(path.join(dir, 'bad.toml'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'good.toml'), [
      'description = "Works"',
      'prompt = "Do it"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'good');
  });

  it('handles empty string values in TOML', () => {
    const dir = path.join(tmpDir, 'empty-val-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'empty.toml'), [
      'description = ""',
      'prompt = ""',
    ].join('\n'));
    const cmds = loadCommands(dir);
    // Empty strings are falsy, so both desc and prompt fail the if (desc && prompt) check
    assert.equal(cmds.length, 0);
  });

  it('uses filename minus .toml extension as command name', () => {
    const dir = path.join(tmpDir, 'name-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'my-custom-command.toml'), [
      'description = "Custom"',
      'prompt = "Run custom"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'my-custom-command');
  });
});

describe('CommandDefinition shape', () => {
  it('has name, description, prompt, and filePath fields', () => {
    const dir = path.join(tmpDir, 'shape-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'check.toml'), [
      'description = "Shape test"',
      'prompt = "Verify shape"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    const cmd: CommandDefinition = cmds[0]!;
    assert.equal(typeof cmd.name, 'string');
    assert.equal(typeof cmd.description, 'string');
    assert.equal(typeof cmd.prompt, 'string');
    assert.equal(typeof cmd.filePath, 'string');
    // Verify no extra enumerable properties
    const keys = Object.keys(cmd).sort();
    assert.deepEqual(keys, ['description', 'filePath', 'name', 'prompt']);
  });
});
