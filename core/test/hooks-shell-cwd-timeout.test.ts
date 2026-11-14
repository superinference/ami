/**
 * Claude->AMI parity: per-hook shell selection, cwd validation, malformed-JSON
 * validation feedback, and per-callback (function-hook) timeouts.
 *
 * Covered gaps:
 *  - command hook `shell` selector knob (bash | sh | powershell/pwsh) + DEFAULT_HOOK_SHELL
 *  - cwd existence validation + fallback before spawning a hook
 *  - user-facing validation feedback for malformed hook JSON output
 *  - timeout boundary / per-callback timeout for in-process (function) hook callbacks
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  DEFAULT_HOOK_SHELL,
  resolveHookShell,
  resolveHookCwd,
  formatHookValidationError,
  HOOK_JSON_SCHEMA_HINT,
  runWithTimeout,
  DEFAULT_CALLBACK_HOOK_TIMEOUT_MS,
  type PreToolUseContext,
} from '../src/hooks';

function makePreToolCtx(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, ...overrides };
}

function writeTmpHooks(entries: any[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-sct-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

// ---------------------------------------------------------------------------
// resolveHookShell
// ---------------------------------------------------------------------------
describe('resolveHookShell', () => {
  it('defaults to DEFAULT_HOOK_SHELL (bash -c)', () => {
    assert.equal(DEFAULT_HOOK_SHELL, 'bash');
    assert.deepEqual(resolveHookShell('echo hi', undefined), { file: 'bash', args: ['-c', 'echo hi'] });
  });
  it('selects sh when requested', () => {
    assert.deepEqual(resolveHookShell('echo hi', 'sh'), { file: 'sh', args: ['-c', 'echo hi'] });
  });
  it('maps powershell/pwsh to a non-interactive pwsh -Command invocation', () => {
    const ps = resolveHookShell('Write-Output hi', 'powershell');
    assert.equal(ps.file, 'pwsh');
    assert.deepEqual(ps.args, ['-NoProfile', '-NonInteractive', '-Command', 'Write-Output hi']);
    assert.deepEqual(resolveHookShell('x', 'PWSH'), resolveHookShell('x', 'powershell'));
  });
  it('is case-insensitive and falls back to bash for unknown selectors', () => {
    assert.equal(resolveHookShell('x', 'BASH').file, 'bash');
    assert.equal(resolveHookShell('x', 'zsh').file, 'bash');
  });
});

// ---------------------------------------------------------------------------
// resolveHookCwd
// ---------------------------------------------------------------------------
describe('resolveHookCwd', () => {
  it('returns an existing directory unchanged', () => {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'hookcwd-'));
    assert.equal(resolveHookCwd(d), d);
    fs.rmSync(d, { recursive: true, force: true });
  });
  it('falls back to home dir when the cwd does not exist', () => {
    const gone = path.join(os.tmpdir(), 'definitely-not-here-' + Math.random().toString(36).slice(2));
    assert.equal(resolveHookCwd(gone), os.homedir());
  });
  it('falls back to home dir for an empty cwd', () => {
    assert.equal(resolveHookCwd(''), os.homedir());
  });
  it('falls back when the path is a file, not a directory', () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'hookcwdf-')), 'a.txt');
    fs.writeFileSync(f, 'x');
    assert.equal(resolveHookCwd(f), os.homedir());
    fs.rmSync(path.dirname(f), { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// formatHookValidationError
// ---------------------------------------------------------------------------
describe('formatHookValidationError', () => {
  it('includes the raw output, the error, and the schema hint', () => {
    const msg = formatHookValidationError('{bad', new Error('Unexpected token b'));
    assert.match(msg, /Hook JSON output validation failed: Unexpected token b/);
    assert.match(msg, /The hook's output was: \{bad/);
    assert.match(msg, /Expected schema:/);
    assert.ok(msg.includes(HOOK_JSON_SCHEMA_HINT));
  });
  it('stringifies a non-Error thrown value', () => {
    assert.match(formatHookValidationError('{', 'boom'), /validation failed: boom/);
  });
});

// ---------------------------------------------------------------------------
// runWithTimeout
// ---------------------------------------------------------------------------
describe('runWithTimeout', () => {
  it('resolves when the work settles before the deadline', async () => {
    assert.equal(await runWithTimeout(Promise.resolve(42), 1000, 'x'), 42);
  });
  it('rejects with a labelled timeout when the work exceeds the deadline', async () => {
    const never = new Promise<number>(() => {});
    await assert.rejects(
      () => runWithTimeout(never, 20, 'slow hook'),
      /slow hook timed out after 20ms/,
    );
  });
  it('propagates the work rejection', async () => {
    await assert.rejects(() => runWithTimeout(Promise.reject(new Error('inner')), 1000, 'x'), /inner/);
  });
});

// ---------------------------------------------------------------------------
// Integration: per-callback timeout in executeCallbacks
// ---------------------------------------------------------------------------
describe('executeCallbacks per-callback timeout', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('exposes a 5s default callback timeout', () => {
    assert.equal(DEFAULT_CALLBACK_HOOK_TIMEOUT_MS, 5_000);
  });

  it('abandons a callback that exceeds its timeout and moves on', async () => {
    const hung = () => new Promise<void>(() => {}); // never resolves
    hm.registerCallback('preToolUse', async () => { await hung(); }, 15);
    hm.registerCallback('preToolUse', async () => ({ decision: 'deny', reason: 'second ran' }), 1000);
    const result = await hm.executeCallbacks('preToolUse', {});
    // The hung callback timed out (logged, skipped); the second callback's
    // decision is still returned.
    assert.deepEqual(result, { decision: 'deny', reason: 'second ran' });
  });

  it('returns a fast callback decision without waiting the full default', async () => {
    hm.registerCallback('preToolUse', async () => ({ decision: 'approve' }));
    const start = Date.now();
    const result = await hm.executeCallbacks('preToolUse', {});
    assert.deepEqual(result, { decision: 'approve' });
    assert.ok(Date.now() - start < 1000);
  });
});

// ---------------------------------------------------------------------------
// Integration: malformed-JSON validation feedback through a command hook
// ---------------------------------------------------------------------------
describe('malformed command-hook JSON → validation feedback', () => {
  let hm: HookManager;
  let tmp: string | undefined;
  beforeEach(() => { hm = new HookManager(); });
  afterEach(() => { if (tmp) { fs.rmSync(tmp, { recursive: true, force: true }); tmp = undefined; } });

  it('surfaces a validation error (not silent plain text) when stdout is malformed JSON', async () => {
    tmp = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf '{not valid json'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmp);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.equal(decision.action, 'allow');
    assert.ok(decision.additionalContext, 'expected a validation error in additionalContext');
    assert.match(decision.additionalContext!, /Hook JSON output validation failed/);
    assert.match(decision.additionalContext!, /Expected schema:/);
  });

  it('still treats non-{ plain text output as opaque (no false validation error)', async () => {
    tmp = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf 'just some text'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmp);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.equal(decision.action, 'allow');
    assert.ok(!decision.additionalContext || !/validation failed/.test(decision.additionalContext));
  });
});

// ---------------------------------------------------------------------------
// disableAllHooks kill-switch (managed > non-managed precedence)
// ---------------------------------------------------------------------------
describe('disableAllHooks kill-switch', () => {
  let hm: HookManager;
  let tmp: string | undefined;
  beforeEach(() => { hm = new HookManager(); });
  afterEach(() => { if (tmp) { fs.rmSync(tmp, { recursive: true, force: true }); tmp = undefined; } });

  it('defaults to not-disabled', () => {
    assert.equal(hm.isAllHooksDisabled(), false);
  });

  it('a managed disableAllHooks skips ALL hook registration', () => {
    tmp = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ALLOW' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.setDisableAllHooks(true);          // managed by default
    assert.equal(hm.isAllHooksDisabled(), true);
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 0);
  });

  it('a non-managed disableAllHooks only disables non-managed hooks (= allowManagedHooksOnly)', () => {
    hm.setDisableAllHooks(true, false);   // non-managed
    assert.equal(hm.isAllHooksDisabled(), false, 'managed kill-switch stays off');
    assert.equal(hm.isAllowManagedHooksOnly(), true, 'collapses to managed-only');
  });

  it('a non-managed source cannot re-enable a managed disable', () => {
    hm.setDisableAllHooks(true, true);    // managed off-switch
    hm.setDisableAllHooks(false, false);  // non-managed attempt to re-enable
    assert.equal(hm.isAllHooksDisabled(), true, 'non-managed cannot override managed');
  });
});

// ---------------------------------------------------------------------------
// Integration: `shell` selector actually drives the interpreter
// ---------------------------------------------------------------------------
describe('command hook `shell` selector drives execution', () => {
  let hm: HookManager;
  let tmp: string | undefined;
  beforeEach(() => { hm = new HookManager(); });
  afterEach(() => { if (tmp) { fs.rmSync(tmp, { recursive: true, force: true }); tmp = undefined; } });

  it('runs a command hook under the selected POSIX shell (sh)', async () => {
    tmp = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf 'SYSTEM:ran-under-sh'`, shell: 'sh' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmp);
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.equal(decision.action, 'allow');
    assert.equal(decision.additionalContext, 'ran-under-sh');
  });

  it('a hook spawned from a removed cwd still runs (cwd fallback)', async () => {
    const dir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf 'SYSTEM:survived'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir); // reads hooks.json synchronously; exec cwd = this dir
    // Delete the directory the hook will spawn from; resolveHookCwd must fall
    // back to home so spawn does not ENOENT.
    fs.rmSync(dir, { recursive: true, force: true });
    const decision = await hm.executePreToolUse(makePreToolCtx());
    assert.equal(decision.additionalContext, 'survived');
  });
});
