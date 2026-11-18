import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  loadCommands,
  isImmediateCommand,
  parseCommandShell,
  COMMAND_SHELLS,
  type CommandShell,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U028 w117 — two CommandDefinition parity knobs missing from plugins.ts:
//
//   1. `shell` (Claude parity: parseShellFrontmatter / loadPluginCommands L298):
//      a command may declare the shell its embedded `!`-block execution runs in
//      (`bash` | `powershell`). Validated to the enum — an unrecognized value
//      degrades to undefined (caller defaults to bash), matching how `effort`
//      degrades. Carried on CommandDefinition so the common in-prompt executor,
//      scoped to the command's `allowedTools` (alwaysAllowRules.command), can
//      pick the shell.
//
//   2. `immediate` (Claude parity: Command.immediate, commands/bridge/index.ts
//      L22): a command flagged `immediate` runs without the normal defer/confirm
//      path. Declarative boolean, read via isImmediateCommand(); default false.
// ---------------------------------------------------------------------------

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u028-w117-')); });
afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } });

describe('parseCommandShell — validated shell selector', () => {
  it('accepts the recognized shells (case/space-insensitive)', () => {
    assert.equal(parseCommandShell('bash'), 'bash');
    assert.equal(parseCommandShell('powershell'), 'powershell');
    assert.equal(parseCommandShell('  PowerShell  '), 'powershell');
    assert.equal(parseCommandShell('BASH'), 'bash');
  });

  it('degrades an unrecognized / empty / non-string value to undefined', () => {
    assert.equal(parseCommandShell('zsh'), undefined);
    assert.equal(parseCommandShell(''), undefined);
    assert.equal(parseCommandShell('   '), undefined);
    assert.equal(parseCommandShell(undefined), undefined);
    assert.equal(parseCommandShell(null), undefined);
    assert.equal(parseCommandShell(42 as unknown as string), undefined);
  });

  it('exposes the canonical shell list', () => {
    assert.deepEqual([...COMMAND_SHELLS], ['bash', 'powershell']);
    const s: CommandShell = 'bash';
    assert.equal(s, 'bash');
  });
});

describe('loadCommands — shell field', () => {
  it('TOML: parses a valid shell declaration', () => {
    fs.writeFileSync(path.join(dir, 'deploy.toml'), [
      'description = "Deploy"',
      'prompt = "Run !`uname -a`"',
      'shell = "powershell"',
    ].join('\n'));
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.shell, 'powershell');
  });

  it('markdown: parses a valid shell declaration from frontmatter', () => {
    fs.writeFileSync(path.join(dir, 'probe.md'), '---\ndescription: Probe\nshell: bash\n---\nRun !`echo hi`\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.shell, 'bash');
  });

  it('omits shell when absent, and degrades an invalid value to undefined', () => {
    fs.writeFileSync(path.join(dir, 'min.toml'), 'description = "d"\nprompt = "p"\n');
    fs.writeFileSync(path.join(dir, 'bad.toml'), 'description = "d"\nprompt = "p"\nshell = "fish"\n');
    const cmds = loadCommands(dir);
    const min = cmds.find(c => c.name === 'min')!;
    const bad = cmds.find(c => c.name === 'bad')!;
    assert.equal(min.shell, undefined);
    assert.equal(bad.shell, undefined); // unrecognized → caller defaults to bash
  });
});

describe('immediate flag + isImmediateCommand', () => {
  it('TOML: parses immediate = true', () => {
    fs.writeFileSync(path.join(dir, 'rc.toml'), 'description = "Remote control"\nprompt = "connect"\nimmediate = true\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.immediate, true);
    assert.equal(isImmediateCommand(cmd), true);
  });

  it('TOML: accepts the is-immediate alias and parses false', () => {
    fs.writeFileSync(path.join(dir, 'a.toml'), 'description = "d"\nprompt = "p"\nis-immediate = true\n');
    fs.writeFileSync(path.join(dir, 'b.toml'), 'description = "d"\nprompt = "p"\nimmediate = false\n');
    const cmds = loadCommands(dir);
    const a = cmds.find(c => c.name === 'a')!;
    const b = cmds.find(c => c.name === 'b')!;
    assert.equal(a.immediate, true);
    assert.equal(b.immediate, false);
    assert.equal(isImmediateCommand(a), true);
    assert.equal(isImmediateCommand(b), false);
  });

  it('markdown: parses immediate from frontmatter', () => {
    fs.writeFileSync(path.join(dir, 'c.md'), '---\ndescription: C\nimmediate: true\n---\nbody\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.immediate, true);
    assert.equal(isImmediateCommand(cmd), true);
  });

  it('omits immediate when absent and treats absence as deferred (not immediate)', () => {
    fs.writeFileSync(path.join(dir, 'min.toml'), 'description = "d"\nprompt = "p"\n');
    const [cmd] = loadCommands(dir);
    assert.equal(cmd.immediate, undefined);
    assert.equal(isImmediateCommand(cmd), false);
    assert.equal(isImmediateCommand({}), false);
  });
});
