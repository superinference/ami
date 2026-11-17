import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  loadCommands,
  isCommandEnabled,
  isCommandHidden,
  getCommandDisplayName,
  redactSensitiveCommandArgs,
  registerCommandAliases,
  REDACTED_COMMAND_ARGS,
  type CommandDefinition,
  type CommandScanDiagnostics,
} from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 — per-command enablement / visibility / sensitivity gates + alias
// collision avoidance + scan diagnostics (Claude parity):
//   - CommandBase.isEnabled (types/command.ts L180, isCommandEnabled L213-216)
//   - CommandBase.isHidden  (types/command.ts L182)
//   - CommandBase.isSensitive (types/command.ts L200 — args redacted from history)
//   - getCommandName / userFacingName (types/command.ts L208-211)
//   - conditional alias registration (commands/branch/index.ts L8)
//   - walkPluginMarkdown logLabel + swallowed-with-a-debug-log scan failure
//     (utils/plugins/walkPluginMarkdown.ts L24-65)
// ---------------------------------------------------------------------------

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u029-gate-'));
}

function writeCmd(dir: string, name: string, body: string): void {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, name), body, 'utf-8');
}

// --- isCommandEnabled -------------------------------------------------------

test('isCommandEnabled defaults to true when the field is absent', () => {
  assert.equal(isCommandEnabled({}), true);
  assert.equal(isCommandEnabled({ isEnabled: undefined }), true);
});

test('isCommandEnabled honors an explicit false / true', () => {
  assert.equal(isCommandEnabled({ isEnabled: false }), false);
  assert.equal(isCommandEnabled({ isEnabled: true }), true);
});

// --- isCommandHidden --------------------------------------------------------

test('isCommandHidden defaults to false and honors an explicit true', () => {
  assert.equal(isCommandHidden({}), false);
  assert.equal(isCommandHidden({ isHidden: false }), false);
  assert.equal(isCommandHidden({ isHidden: true }), true);
});

// --- getCommandDisplayName --------------------------------------------------

test('getCommandDisplayName falls back to name and honors displayName', () => {
  assert.equal(getCommandDisplayName({ name: 'plug:deploy' }), 'plug:deploy');
  assert.equal(getCommandDisplayName({ name: 'plug:deploy', displayName: 'Deploy' }), 'Deploy');
  // Blank displayName is ignored (falls back to the lookup name).
  assert.equal(getCommandDisplayName({ name: 'plug:deploy', displayName: '  ' }), 'plug:deploy');
});

// --- redactSensitiveCommandArgs --------------------------------------------

test('redactSensitiveCommandArgs leaves a non-sensitive command untouched', () => {
  assert.equal(redactSensitiveCommandArgs({}, 'tok-123'), 'tok-123');
  assert.equal(redactSensitiveCommandArgs({ isSensitive: false }, 'tok-123'), 'tok-123');
});

test('redactSensitiveCommandArgs redacts a sensitive command with non-empty args', () => {
  assert.equal(redactSensitiveCommandArgs({ isSensitive: true }, 'my-secret-token'), REDACTED_COMMAND_ARGS);
  assert.equal(REDACTED_COMMAND_ARGS, '[redacted]');
});

test('redactSensitiveCommandArgs leaves an empty arg string unchanged (nothing to hide)', () => {
  assert.equal(redactSensitiveCommandArgs({ isSensitive: true }, ''), '');
  assert.equal(redactSensitiveCommandArgs({ isSensitive: true }, '   '), '   ');
});

// --- registerCommandAliases (collision avoidance) ---------------------------

function cmd(name: string, aliases?: string[]): CommandDefinition {
  return { name, description: 'd', prompt: 'p', filePath: `/x/${name}.toml`, aliases };
}

test('registerCommandAliases registers a non-colliding alias', () => {
  const index = new Map<string, CommandDefinition>();
  const c = cmd('branch', ['fork']);
  registerCommandAliases(c, index, new Set(['branch']));
  assert.equal(index.get('fork'), c);
});

test('registerCommandAliases skips an alias that shadows a primary command name', () => {
  const index = new Map<string, CommandDefinition>();
  const c = cmd('branch', ['tag']);
  registerCommandAliases(c, index, new Set(['branch', 'tag']));
  assert.equal(index.has('tag'), false, 'alias must not shadow an existing primary name');
});

test('registerCommandAliases keeps the FIRST claimant of a duplicate alias', () => {
  const index = new Map<string, CommandDefinition>();
  const first = cmd('a', ['shared']);
  const second = cmd('b', ['shared']);
  registerCommandAliases(first, index, new Set(['a', 'b']));
  registerCommandAliases(second, index, new Set(['a', 'b']));
  assert.equal(index.get('shared'), first, 'the first claimant of an alias wins');
});

test('registerCommandAliases ignores empty / self / non-string aliases', () => {
  const index = new Map<string, CommandDefinition>();
  const c = cmd('deploy', ['', '  ', 'deploy', 42 as unknown as string, 'ship']);
  registerCommandAliases(c, index, new Set(['deploy']));
  assert.deepEqual([...index.keys()], ['ship']);
});

test('registerCommandAliases is a no-op when a command declares no aliases', () => {
  const index = new Map<string, CommandDefinition>();
  const ret = registerCommandAliases(cmd('x'), index, new Set(['x']));
  assert.equal(ret, index);
  assert.equal(index.size, 0);
});

// --- parsing the gates out of command FILES (.toml + .md) -------------------

test('loadCommands parses enabled/hidden/sensitive knobs from a .toml command', () => {
  const dir = mkTmp();
  try {
    writeCmd(path.join(dir, 'commands'), 'login.toml', [
      'description = "log in"',
      'prompt = "do login $ARGUMENTS"',
      'enabled = false',
      'hidden = true',
      'sensitive = true',
    ].join('\n'));
    const cmds = loadCommands(path.join(dir, 'commands'));
    const c = cmds.find(x => x.name === 'login');
    assert.ok(c, 'login command loaded');
    assert.equal(c!.isEnabled, false);
    assert.equal(c!.isHidden, true);
    assert.equal(c!.isSensitive, true);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('loadCommands parses is-hidden / is-sensitive frontmatter from a .md command', () => {
  const dir = mkTmp();
  try {
    writeCmd(path.join(dir, 'commands'), 'secret.md', [
      '---',
      'description: secret cmd',
      'is-hidden: true',
      'is-sensitive: true',
      '---',
      'body prompt',
    ].join('\n'));
    const cmds = loadCommands(path.join(dir, 'commands'));
    const c = cmds.find(x => x.name === 'secret');
    assert.ok(c);
    assert.equal(c!.isHidden, true);
    assert.equal(c!.isSensitive, true);
    assert.equal(c!.isEnabled, undefined, 'unset knob stays undefined');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// --- scan diagnostics (logLabel + onScanError) ------------------------------

test('loadCommands reports an unreadable primary dir through onScanError with the default label', () => {
  const calls: Array<{ dir: string; label: string; message: string }> = [];
  const diag: CommandScanDiagnostics = {
    onScanError: ({ dir, label, message }) => calls.push({ dir, label, message }),
  };
  const missing = path.join(mkTmp(), 'does-not-exist');
  const cmds = loadCommands(missing, undefined, undefined, undefined, diag);
  assert.deepEqual(cmds, []);
  assert.equal(calls.length, 1);
  assert.equal(calls[0]!.dir, missing);
  assert.equal(calls[0]!.label, 'plugin');
  assert.match(calls[0]!.message, /Failed to scan plugin directory/);
});

test('loadCommands uses a caller-supplied logLabel in the scan-failure message', () => {
  const calls: string[] = [];
  const missing = path.join(mkTmp(), 'nope');
  loadCommands(missing, undefined, undefined, undefined, {
    logLabel: 'skill',
    onScanError: ({ message }) => calls.push(message),
  });
  assert.equal(calls.length, 1);
  assert.match(calls[0]!, /Failed to scan skill directory/);
});

test('loadCommands reports an unreadable extraPath through onScanError', () => {
  const dir = mkTmp();
  try {
    writeCmd(path.join(dir, 'commands'), 'ok.toml', 'description = "d"\nprompt = "p"');
    const missingExtra = path.join(dir, 'ghost-extra');
    const calls: string[] = [];
    const cmds = loadCommands(path.join(dir, 'commands'), undefined, [missingExtra], undefined, {
      onScanError: ({ dir: d }) => calls.push(d),
    });
    assert.equal(cmds.length, 1, 'the good primary command still loads');
    assert.deepEqual(calls, [missingExtra]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('loadCommands stays silent (no throw) on a missing dir when no diagnostics are wired', () => {
  const missing = path.join(mkTmp(), 'absent');
  assert.deepEqual(loadCommands(missing), []);
});
