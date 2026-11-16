/**
 * Unit U007 — core/core/src/hooks.ts parity gaps:
 *  - CLAUDE_CODE_SHELL_PREFIX wrapping of hook commands
 *  - Configurable DEFAULT_HOOK_SHELL (SUPERINFERENCE_HOOK_SHELL) + Windows .sh fallback
 *  - hookSpecificOutput.hookEventName spoof check (reject mismatched event name)
 *  - suppressOutput flag (hide a command hook's plain-text stdout surface)
 *
 * Framework: node:test + assert/strict. Command-hook tests spawn a real shell
 * (end-to-end through runCommandHookAsync) so the wiring is exercised, not mocked.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  HookManager,
  resolveHookShell,
  resolveDefaultHookShell,
  posixSingleQuote,
  formatShellPrefixCommand,
  hookEventNameMismatch,
  serializeHookEvent,
  DEFAULT_HOOK_SHELL,
  type PreToolUseContext,
} from '../src/hooks';

function writeTmpHooks(entries: any[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

function makePreToolUseCtx(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

// ---------------------------------------------------------------------------
// posixSingleQuote
// ---------------------------------------------------------------------------
describe('posixSingleQuote', () => {
  it('wraps a plain string in single quotes', () => {
    assert.equal(posixSingleQuote('echo hi'), `'echo hi'`);
  });
  it('escapes embedded single quotes safely', () => {
    assert.equal(posixSingleQuote("it's"), `'it'\\''s'`);
  });
});

// ---------------------------------------------------------------------------
// formatShellPrefixCommand (Claude Code parity)
// ---------------------------------------------------------------------------
describe('formatShellPrefixCommand', () => {
  it('quotes bare prefix and command', () => {
    assert.equal(formatShellPrefixCommand('firejail', 'echo hi'), `'firejail' 'echo hi'`);
  });
  it('splits a trailing -flag run from the executable', () => {
    assert.equal(formatShellPrefixCommand('/usr/bin/env -i', 'echo hi'), `'/usr/bin/env' -i 'echo hi'`);
  });
  it('does not split a dash that is part of the executable path', () => {
    // No space-before-dash → whole prefix is the executable.
    assert.equal(formatShellPrefixCommand('my-wrapper', 'echo hi'), `'my-wrapper' 'echo hi'`);
  });
});

// ---------------------------------------------------------------------------
// resolveDefaultHookShell (configurable DEFAULT_HOOK_SHELL)
// ---------------------------------------------------------------------------
describe('resolveDefaultHookShell', () => {
  it('falls back to DEFAULT_HOOK_SHELL when unset', () => {
    assert.equal(resolveDefaultHookShell({}), DEFAULT_HOOK_SHELL);
  });
  it('honors a recognized SUPERINFERENCE_HOOK_SHELL override', () => {
    assert.equal(resolveDefaultHookShell({ SUPERINFERENCE_HOOK_SHELL: 'pwsh' }), 'pwsh');
    assert.equal(resolveDefaultHookShell({ SUPERINFERENCE_HOOK_SHELL: 'SH' }), 'sh');
  });
  it('ignores an unrecognized override', () => {
    assert.equal(resolveDefaultHookShell({ SUPERINFERENCE_HOOK_SHELL: 'zsh' }), DEFAULT_HOOK_SHELL);
  });
});

// ---------------------------------------------------------------------------
// resolveHookShell — prefix + windows fallback + configurable default
// ---------------------------------------------------------------------------
describe('resolveHookShell', () => {
  it('defaults to bash -c with no prefix/env', () => {
    assert.deepEqual(resolveHookShell('echo hi', undefined, { env: {}, platform: 'linux' }), {
      file: 'bash',
      args: ['-c', 'echo hi'],
    });
  });

  it('uses the configurable default shell when no per-hook shell is set', () => {
    const r = resolveHookShell('Write-Output hi', undefined, { env: { SUPERINFERENCE_HOOK_SHELL: 'pwsh' }, platform: 'linux' });
    assert.equal(r.file, 'pwsh');
  });

  it('wraps the command with CLAUDE_CODE_SHELL_PREFIX (POSIX quoted)', () => {
    const r = resolveHookShell('printf hi', undefined, { env: { CLAUDE_CODE_SHELL_PREFIX: 'firejail' }, platform: 'linux' });
    assert.deepEqual(r, { file: 'bash', args: ['-c', `'firejail' 'printf hi'`] });
  });

  it('does NOT apply the prefix to PowerShell hooks', () => {
    const r = resolveHookShell('Write-Output hi', 'pwsh', { env: { CLAUDE_CODE_SHELL_PREFIX: 'firejail' }, platform: 'linux' });
    assert.equal(r.file, 'pwsh');
    assert.deepEqual(r.args, ['-NoProfile', '-NonInteractive', '-Command', 'Write-Output hi']);
  });

  it('auto-prepends bash for a .sh script on win32', () => {
    const r = resolveHookShell('scripts/run.sh --flag', undefined, { env: {}, platform: 'win32' });
    assert.deepEqual(r, { file: 'bash', args: ['-c', 'bash scripts/run.sh --flag'] });
  });

  it('does NOT prepend bash for a .sh command already starting with bash', () => {
    const r = resolveHookShell('bash scripts/run.sh', undefined, { env: {}, platform: 'win32' });
    assert.deepEqual(r, { file: 'bash', args: ['-c', 'bash scripts/run.sh'] });
  });

  it('does NOT prepend bash for a .sh command on non-win32', () => {
    const r = resolveHookShell('scripts/run.sh', undefined, { env: {}, platform: 'linux' });
    assert.deepEqual(r, { file: 'bash', args: ['-c', 'scripts/run.sh'] });
  });

  it('applies the windows fallback AND the prefix together', () => {
    const r = resolveHookShell('run.sh', undefined, { env: { CLAUDE_CODE_SHELL_PREFIX: 'wrap' }, platform: 'win32' });
    assert.deepEqual(r, { file: 'bash', args: ['-c', `'wrap' 'bash run.sh'`] });
  });
});

// ---------------------------------------------------------------------------
// hookEventNameMismatch (spoof check)
// ---------------------------------------------------------------------------
describe('hookEventNameMismatch', () => {
  it('returns undefined when no expected event is supplied', () => {
    assert.equal(hookEventNameMismatch({ hookSpecificOutput: { hookEventName: 'PreToolUse' } }, undefined), undefined);
  });
  it('returns undefined when the names match', () => {
    assert.equal(hookEventNameMismatch({ hookSpecificOutput: { hookEventName: 'PreToolUse' } }, 'PreToolUse'), undefined);
  });
  it('returns undefined when no hookSpecificOutput / hookEventName present', () => {
    assert.equal(hookEventNameMismatch({ decision: 'deny' }, 'PreToolUse'), undefined);
    assert.equal(hookEventNameMismatch({ hookSpecificOutput: {} }, 'PreToolUse'), undefined);
    assert.equal(hookEventNameMismatch(null, 'PreToolUse'), undefined);
  });
  it('returns a validation message on a spoofed (mismatched) event name', () => {
    const msg = hookEventNameMismatch({ hookSpecificOutput: { hookEventName: 'Stop' } }, 'PreToolUse');
    assert.match(String(msg), /expected 'PreToolUse' but got 'Stop'/);
  });
});

// ---------------------------------------------------------------------------
// runHookByType — end-to-end command hook (real shell spawn)
// ---------------------------------------------------------------------------
describe('command hook — suppressOutput (runHookByType)', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('suppresses the plain-text surface when suppressOutput:true', async () => {
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"suppressOutput":true}'` },
      process.cwd(), '', undefined, { success: true },
    );
    assert.equal(out, '');
  });

  it('still echoes non-decision JSON when suppressOutput is absent (discriminator)', async () => {
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"foo":"bar"}'` },
      process.cwd(), '', undefined, { success: true },
    );
    assert.equal(out, '{"foo":"bar"}');
  });
});

describe('command hook — hookEventName spoof check (runHookByType)', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('rejects a hook whose hookEventName does not match the fired event', async () => {
    const eventData = serializeHookEvent({ event: 'postToolUse', toolName: 'bash' });
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","decision":"deny"}}'` },
      process.cwd(), eventData, undefined, { success: true },
    );
    assert.match(out, /^SYSTEM:Hook returned incorrect event name: expected 'PostToolUse' but got 'PreToolUse'/);
  });

  it('acts on the decision when the hookEventName matches', async () => {
    const eventData = serializeHookEvent({ event: 'preToolUse', toolName: 'bash' });
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","decision":"deny","permissionDecisionReason":"nope"}}'` },
      process.cwd(), eventData, undefined, { success: true },
    );
    assert.equal(out, 'DENY:nope');
  });
});

// ---------------------------------------------------------------------------
// End-to-end: a spoofed deny must NOT deny the tool (executePreToolUse)
// ---------------------------------------------------------------------------
describe('executePreToolUse — spoofed hookEventName is not acted on', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('a PreToolUse command hook that spoofs hookEventName=Stop does not deny', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf '{"hookSpecificOutput":{"hookEventName":"Stop","decision":"deny","reason":"spoofed"}}'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // With the spoof check the decision is rejected (surfaced as context), not a deny.
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('a matching PreToolUse hookEventName deny DOES deny (control)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","decision":"deny","reason":"real"}}'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// End-to-end: CLAUDE_CODE_SHELL_PREFIX reaches the real spawn path
// ---------------------------------------------------------------------------
describe('command hook — CLAUDE_CODE_SHELL_PREFIX wiring (real spawn)', () => {
  let hm: HookManager;
  let savedPrefix: string | undefined;
  beforeEach(() => { hm = new HookManager(); savedPrefix = process.env.CLAUDE_CODE_SHELL_PREFIX; });
  afterEach(() => {
    if (savedPrefix === undefined) delete process.env.CLAUDE_CODE_SHELL_PREFIX;
    else process.env.CLAUDE_CODE_SHELL_PREFIX = savedPrefix;
  });

  it('wraps the command so the prefix executable runs it', async () => {
    // prefix `echo` → bash -c "'echo' 'printf WRAPPED'" → prints the literal
    // "printf WRAPPED" (echo's argument), proving the prefix wrapped the command.
    process.env.CLAUDE_CODE_SHELL_PREFIX = 'echo';
    const out = await (hm as any).runHookByType(
      { type: 'command', command: 'printf WRAPPED' },
      process.cwd(), '', undefined, { success: true },
    );
    assert.equal(out, 'printf WRAPPED');
  });
});
