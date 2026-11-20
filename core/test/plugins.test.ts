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
  fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
  fs.writeFileSync(
    path.join(root, '.ami-plugin', 'plugin.json'),
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

  it('discovers a plugin with full manifest (namespace-safe name wins over dir)', () => {
    // A valid, namespacing-safe manifest name is used verbatim (overriding the dir
    // name). A name with spaces/non-ASCII would instead fall back to the dir name —
    // see the dedicated name-sanitization test below.
    makePlugin('my-plugin-dir', { name: 'custom-name', version: '1.2.3' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'custom-name');
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

  it('reads manifest defaultEnabled (true/false) and leaves it undefined when omitted or non-boolean', () => {
    makePlugin('ships-disabled', { name: 'OFF', defaultEnabled: false });
    makePlugin('ships-enabled', { name: 'ON', defaultEnabled: true });
    makePlugin('no-default', { name: 'NONE' });
    makePlugin('bad-default', { name: 'BAD', defaultEnabled: 'yes' });
    const byName = new Map(discoverPlugins(tmpDir).map(b => [b.name, b]));
    assert.equal(byName.get('OFF')!.defaultEnabled, false);
    assert.equal(byName.get('ON')!.defaultEnabled, true);
    assert.equal(byName.get('NONE')!.defaultEnabled, undefined);
    assert.equal(byName.get('BAD')!.defaultEnabled, undefined);
  });

  it('populates hooksFile when hooks.json exists', () => {
    makePlugin('with-hooks', { name: 'HK' }, { hooks: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.hooksFile);
    assert.ok(bundles[0]!.hooksFile!.endsWith('hooks.json'));
  });

  it('populates hooksFile from the Claude Code hooks/hooks.json subdir (impeccable layout)', () => {
    const root = makePlugin('cc-hooks', { name: 'CC' });
    fs.mkdirSync(path.join(root, 'hooks'), { recursive: true });
    fs.writeFileSync(path.join(root, 'hooks', 'hooks.json'), '{"hooks":{}}');
    const bundles = discoverPlugins(tmpDir);
    const b = bundles.find(x => x.name === 'CC')!;
    assert.ok(b.hooksFile, 'hooksFile should resolve to the hooks/ subdir');
    assert.ok(b.hooksFile!.endsWith(path.join('hooks', 'hooks.json')));
  });

  it('prefers root hooks.json over the hooks/ subdir when both exist', () => {
    const root = makePlugin('plugin-both', { name: 'BOTH' }, { hooks: true });
    fs.mkdirSync(path.join(root, 'hooks'), { recursive: true });
    fs.writeFileSync(path.join(root, 'hooks', 'hooks.json'), '{"hooks":{}}');
    const b = discoverPlugins(tmpDir).find(x => x.name === 'BOTH')!;
    // Root file is <root>/hooks.json, NOT the <root>/hooks/hooks.json subdir.
    assert.equal(b.hooksFile, path.join(root, 'hooks.json'));
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

  it('skips directories without .ami-plugin or .claude-plugin manifest', () => {
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
    fs.mkdirSync(path.join(badRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(badRoot, '.ami-plugin', 'plugin.json'), 'not json');
    makePlugin('good', { name: 'Good' });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'Good');
  });

  it('rejects symlink-escaped component paths', () => {
    const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-escape-'));
    fs.writeFileSync(path.join(outsideDir, 'hooks.json'), '{}');

    const pluginRoot = path.join(tmpDir, 'escape-plugin');
    fs.mkdirSync(path.join(pluginRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(pluginRoot, '.ami-plugin', 'plugin.json'),
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
      JSON.stringify({ name: 'cc-manifest', version: '2.0.0' }),
    );
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'cc-manifest');
    assert.equal(bundles[0]!.version, '2.0.0');
  });

  it('prefers .ami-plugin over .claude-plugin when both exist', () => {
    const root = path.join(tmpDir, 'dual-plugin');
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name: 'si-manifest' }),
    );
    fs.mkdirSync(path.join(root, '.claude-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.claude-plugin', 'plugin.json'),
      JSON.stringify({ name: 'cc-manifest' }),
    );
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.equal(bundles[0]!.name, 'si-manifest');
  });

  it('populates commandsDir when commands/ exists', () => {
    makePlugin('with-commands', { name: 'CMD' }, { skills: false, agents: false, hooks: false });
    const root = path.join(tmpDir, 'with-commands');
    fs.mkdirSync(path.join(root, 'commands'), { recursive: true });
    const bundles = discoverPlugins(tmpDir);
    assert.ok(bundles[0]!.commandsDir);
    assert.ok(bundles[0]!.commandsDir!.endsWith('commands'));
  });

  it('honors manifest-declared relative component path overrides (skills/agents/commands/hooks)', () => {
    makePlugin('overrides', {
      name: 'OV',
      skills: 'custom/skills',
      agents: 'custom/agents',
      commands: 'custom/cmds',
      hooks: 'custom/hooks.json',
    }, { skills: false, agents: false, hooks: false });
    const root = path.join(tmpDir, 'overrides');
    fs.mkdirSync(path.join(root, 'custom', 'skills'), { recursive: true });
    fs.mkdirSync(path.join(root, 'custom', 'agents'), { recursive: true });
    fs.mkdirSync(path.join(root, 'custom', 'cmds'), { recursive: true });
    fs.writeFileSync(path.join(root, 'custom', 'hooks.json'), '{}');
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.ok(bundles[0]!.skillsDir!.endsWith(path.join('custom', 'skills')));
    assert.ok(bundles[0]!.agentsDir!.endsWith(path.join('custom', 'agents')));
    assert.ok(bundles[0]!.commandsDir!.endsWith(path.join('custom', 'cmds')));
    assert.ok(bundles[0]!.hooksFile!.endsWith(path.join('custom', 'hooks.json')));
  });

  it('falls back to default dirs when a manifest override is absent or non-string', () => {
    // `skills` is an array (object-mapping/array forms not yet supported) → default dir used.
    makePlugin('fallback-override', { name: 'FB', skills: ['a', 'b'] }, { skills: true, agents: false, hooks: false });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    assert.ok(bundles[0]!.skillsDir!.endsWith('skills'));
  });

  it('leaves a component undefined when its manifest override points at a missing dir', () => {
    makePlugin('missing-override', { name: 'MO', skills: 'nope/here' }, { skills: true, agents: false, hooks: false });
    const bundles = discoverPlugins(tmpDir);
    assert.equal(bundles.length, 1);
    // The declared override does not exist, so hardenedResolve yields undefined
    // (the default `skills/` dir is NOT consulted once an override is declared).
    assert.equal(bundles[0]!.skillsDir, undefined);
  });

  it('rejects a bundle whose manifest override escapes the plugin dir via ".."', () => {
    makePlugin('escape-override', { name: 'ESC', skills: '../outside' });
    const bundles = discoverPlugins(tmpDir);
    // The ".." override is a hard manifest error → the bundle is a failure, not loaded.
    assert.equal(bundles.length, 0);
  });
});

describe('loadCommands', () => {
  it('returns empty array for non-existent directory', () => {
    const cmds = loadCommands(path.join(tmpDir, 'nope'));
    assert.deepEqual(cmds, []);
  });

  // Functional change: .md command files are now a supported command format
  // (the reference parity), so a command dir is "empty" only when it has no .toml/.md
  // command files — a non-command extension (e.g. .txt) is still ignored.
  it('returns empty array for directory with no command files', () => {
    const dir = path.join(tmpDir, 'no-toml');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'NOTES.txt'), 'hello');
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

  it('drops a .toml with no prompt but loads a prompt-only one with a prompt-derived description', () => {
    const dir = path.join(tmpDir, 'partial-cmds');
    fs.mkdirSync(dir, { recursive: true });
    // Missing prompt → dropped (a command needs a prompt to do anything).
    fs.writeFileSync(path.join(dir, 'no-prompt.toml'), 'description = "desc only"');
    // Missing description → loaded with a description DERIVED from the prompt
    // (the reference parity: extractDescriptionFromMarkdown), not a generic label.
    fs.writeFileSync(path.join(dir, 'no-desc.toml'), 'prompt = "prompt only"');
    // Valid (both fields).
    fs.writeFileSync(path.join(dir, 'valid.toml'), [
      'description = "Valid"',
      'prompt = "Do something"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 2);
    const byName = new Map(cmds.map(c => [c.name, c]));
    assert.equal(byName.has('no-prompt'), false, 'prompt-less command must be dropped');
    assert.equal(byName.get('no-desc')!.description, 'prompt only');
    assert.equal(byName.get('no-desc')!.prompt, 'prompt only');
    assert.equal(byName.get('valid')!.description, 'Valid');
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

  // Functional change: .md is now a command format (the reference parity); only files
  // with neither a .toml nor .md extension are ignored.
  it('ignores files that are not .toml or .md command files', () => {
    const dir = path.join(tmpDir, 'mixed-cmds');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'notes.txt'), '# Commands');
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

  it('discovers commands in a nested subdirectory and namespaces by sub-path (the reference parity)', () => {
    const dir = path.join(tmpDir, 'nested-cmds');
    fs.mkdirSync(path.join(dir, 'git'), { recursive: true });
    // Top-level command keeps its bare name.
    fs.writeFileSync(path.join(dir, 'review.toml'), [
      'description = "Review"',
      'prompt = "Review changes"',
    ].join('\n'));
    // Subdirectory command is namespaced by its directory path with ':'.
    fs.writeFileSync(path.join(dir, 'git', 'commit.toml'), [
      'description = "Commit helper"',
      'prompt = "Write a commit"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    const byName = new Map(cmds.map(c => [c.name, c]));
    assert.equal(cmds.length, 2);
    assert.ok(byName.has('review'), 'top-level command keeps bare name');
    assert.ok(byName.has('git:commit'), 'nested command is namespaced git:commit');
    assert.equal(byName.get('git:commit')!.prompt, 'Write a commit');
    assert.equal(byName.get('git:commit')!.filePath, path.join(dir, 'git', 'commit.toml'));
  });

  it('namespaces deeply nested commands by the full sub-path joined with ":"', () => {
    const dir = path.join(tmpDir, 'deep-cmds');
    fs.mkdirSync(path.join(dir, 'a', 'b'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'a', 'b', 'deep.toml'), [
      'description = "Deep"',
      'prompt = "Go deep"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'a:b:deep');
  });

  it('does not follow a symlinked subdirectory when walking commands', () => {
    const dir = path.join(tmpDir, 'symlink-cmds');
    const outside = path.join(tmpDir, 'symlink-cmds-outside');
    fs.mkdirSync(dir, { recursive: true });
    fs.mkdirSync(outside, { recursive: true });
    fs.writeFileSync(path.join(outside, 'sneaky.toml'), [
      'description = "Sneaky"',
      'prompt = "Should not load"',
    ].join('\n'));
    try {
      fs.symlinkSync(outside, path.join(dir, 'linked'), 'dir');
    } catch {
      return; // platform without symlink support — skip
    }
    fs.writeFileSync(path.join(dir, 'real.toml'), [
      'description = "Real"',
      'prompt = "Load me"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    const names = cmds.map(c => c.name);
    assert.deepEqual(names, ['real'], 'symlinked dir is not traversed');
  });
});

describe('loadCommands plugin-name provenance (CommandDefinition.pluginName)', () => {
  it('stamps the originating plugin name on every command loaded for a bundle', () => {
    const dir = path.join(tmpDir, 'pn-cmds');
    fs.mkdirSync(path.join(dir, 'git'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'deploy.toml'), [
      'description = "Deploy"',
      'prompt = "Ship it"',
    ].join('\n'));
    fs.writeFileSync(path.join(dir, 'git', 'commit.toml'), [
      'description = "Commit"',
      'prompt = "Commit it"',
    ].join('\n'));
    const cmds = loadCommands(dir, 'acme-tools');
    assert.equal(cmds.length, 2);
    for (const c of cmds) {
      assert.equal(c.pluginName, 'acme-tools', `${c.name} carries its plugin name`);
    }
    // Namespaced lookup name still prefixes the plugin; pluginName is the SEPARATE
    // provenance field (not conflated with the on-disk folder structure).
    const commit = cmds.find(c => c.name === 'acme-tools:git:commit')!;
    assert.ok(commit, 'nested command keeps its namespaced name');
    assert.deepEqual(commit.namespace, ['git']);
    assert.equal(commit.pluginName, 'acme-tools');
  });

  it('stamps the plugin name on a command loaded from an extraPaths file', () => {
    const dir = path.join(tmpDir, 'pn-primary');
    fs.mkdirSync(dir, { recursive: true });
    const extraDir = path.join(tmpDir, 'pn-extra');
    fs.mkdirSync(extraDir, { recursive: true });
    const extraFile = path.join(extraDir, 'extra.toml');
    fs.writeFileSync(extraFile, [
      'description = "Extra"',
      'prompt = "Extra prompt"',
    ].join('\n'));
    const cmds = loadCommands(dir, 'acme-tools', [extraFile]);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.name, 'acme-tools:extra');
    assert.equal(cmds[0]!.pluginName, 'acme-tools');
  });

  it('leaves pluginName unset for a bare (non-bundle) loadCommands call', () => {
    const dir = path.join(tmpDir, 'pn-bare');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'solo.toml'), [
      'description = "Solo"',
      'prompt = "Solo prompt"',
    ].join('\n'));
    const cmds = loadCommands(dir);
    assert.equal(cmds.length, 1);
    assert.equal(cmds[0]!.pluginName, undefined, 'no plugin name without a bundle');
    assert.ok(!('pluginName' in cmds[0]!), 'pluginName key is omitted, not set to undefined');
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
    // Verify no UNEXPECTED enumerable properties. `namespace` (the on-disk
    // sub-path, empty for a top-level command) and `type` ('prompt' for a command
    // file) are always-present parity fields; `hasUserSpecifiedDescription` is set
    // because this file declares an EXPLICIT description (the reference parity); every other
    // knob stays omitted when the file declares none.
    const keys = Object.keys(cmd).sort();
    assert.deepEqual(keys, ['description', 'filePath', 'hasUserSpecifiedDescription', 'name', 'namespace', 'prompt', 'type']);
    assert.equal(cmd.hasUserSpecifiedDescription, true);
    assert.deepEqual(cmd.namespace, []);
    assert.equal(cmd.type, 'prompt');
  });
});
