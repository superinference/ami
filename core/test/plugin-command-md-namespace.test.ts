import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { loadCommands, validatePluginContents, DEFAULT_COMMAND_DESCRIPTION } from '../src/plugins';

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cmd-md-')); });
afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

describe('loadCommands — markdown command files (Claude native format)', () => {
  it('loads a .md command: body is the prompt, frontmatter carries knobs', () => {
    fs.writeFileSync(path.join(dir, 'review.md'), [
      '---',
      'description: Review the diff',
      'argument-hint: <pr>',
      'model: claude-opus-4-8',
      'effort: high',
      'when_to_use: before merge',
      'version: 3.0.0',
      'name: Code Review',
      'allowed-tools: bash, file_read',
      'disable-model-invocation: true',
      'user-invocable: false',
      '---',
      'Please review the following changes carefully.',
    ].join('\n'));
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.name, 'review');
    assert.equal(cmd.description, 'Review the diff');
    assert.equal(cmd.prompt, 'Please review the following changes carefully.');
    assert.equal(cmd.argumentHint, '<pr>');
    assert.equal(cmd.model, 'claude-opus-4-8');
    assert.equal(cmd.effort, 'high');
    assert.equal(cmd.whenToUse, 'before merge');
    assert.equal(cmd.version, '3.0.0');
    assert.equal(cmd.displayName, 'Code Review');
    assert.deepEqual(cmd.allowedTools, ['bash', 'file_read']);
    assert.equal(cmd.disableModelInvocation, true);
    assert.equal(cmd.userInvocable, false);
  });

  it('a .md with no description falls back to the default label', () => {
    fs.writeFileSync(path.join(dir, 'min.md'), '---\n---\nJust do the thing.\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.description, DEFAULT_COMMAND_DESCRIPTION);
    assert.equal(cmd.prompt, 'Just do the thing.');
  });

  it('a .md command with a frontmatter-only, empty body is skipped', () => {
    fs.writeFileSync(path.join(dir, 'empty.md'), '---\ndescription: x\n---\n');
    assert.deepEqual(loadCommands(dir), []);
  });

  it('a .md with no frontmatter uses the whole file as the prompt', () => {
    fs.writeFileSync(path.join(dir, 'plain.md'), 'Say hi to $ARGUMENTS.');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.prompt, 'Say hi to $ARGUMENTS.');
    assert.equal(cmd.description, DEFAULT_COMMAND_DESCRIPTION);
  });
});

describe('loadCommands — TOML displayName via name key', () => {
  it('parses the display name from the TOML `name` key', () => {
    fs.writeFileSync(path.join(dir, 'deploy.toml'), 'description = "d"\nprompt = "p"\nname = "Ship It"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.name, 'deploy');
    assert.equal(cmd.displayName, 'Ship It');
  });
});

describe('loadCommands — pluginName namespacing (cross-plugin uniqueness)', () => {
  it('prepends the plugin name as the first namespace segment', () => {
    fs.writeFileSync(path.join(dir, 'deploy.toml'), 'description = "d"\nprompt = "p"\n');
    const [cmd] = loadCommands(dir, 'infra');
    assert.equal(cmd.name, 'infra:deploy');
  });

  it('nests the plugin name ahead of the subdirectory namespace', () => {
    fs.mkdirSync(path.join(dir, 'git'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'git', 'commit.md'), '---\ndescription: c\n---\nCommit it.\n');
    const [cmd] = loadCommands(dir, 'vcs');
    assert.equal(cmd.name, 'vcs:git:commit');
  });

  it('keeps the bare (un-prefixed) name when no plugin name is given', () => {
    fs.writeFileSync(path.join(dir, 'review.toml'), 'description = "d"\nprompt = "p"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.name, 'review');
  });

  it('treats an empty/whitespace plugin name as no prefix', () => {
    fs.writeFileSync(path.join(dir, 'x.toml'), 'description = "d"\nprompt = "p"\n');
    assert.equal(loadCommands(dir, '  ')[0]!.name, 'x');
  });

  it('loads both .toml and .md files from the same dir', () => {
    fs.writeFileSync(path.join(dir, 'a.toml'), 'description = "d"\nprompt = "p"\n');
    fs.writeFileSync(path.join(dir, 'b.md'), '---\ndescription: d\n---\nbody\n');
    const names = loadCommands(dir).map(c => c.name).sort();
    assert.deepEqual(names, ['a', 'b']);
  });
});

describe('validatePluginContents — markdown command files', () => {
  it('accepts a .md command with a non-empty body', () => {
    fs.mkdirSync(path.join(dir, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'p', version: '1.0.0' }));
    fs.mkdirSync(path.join(dir, 'commands'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'commands', 'ok.md'), '---\ndescription: d\n---\nRun it.\n');
    const r = validatePluginContents(dir);
    assert.equal(r.valid, true, JSON.stringify(r.errors));
  });

  it('flags a .md command whose body (prompt) is empty', () => {
    fs.mkdirSync(path.join(dir, 'commands'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'commands', 'bad.md'), '---\ndescription: d\n---\n');
    const r = validatePluginContents(dir);
    assert.equal(r.valid, false);
    assert.ok(r.errors.some(e => e.path === 'commands/bad.md' && /non-empty body/.test(e.message)));
  });
});
