import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  PermissionManager,
  ghArgsTargetForeignHost,
  isGhInvocationReadOnly,
  isGhReadOnlyCommandSafe,
} from '../src/permissions';

// =============================================================================
// U003 (w85) — two additive permission-parity features in core/permissions.ts:
//   A. `gh` (GitHub CLI) read-only command classification + HOST/OWNER/REPO exfil
//      guard (parity: Claude's GH_READ_ONLY_COMMANDS + ghIsDangerousCallback,
//      utils/shell/readOnlyCommandValidation.ts).
//   B. deny/ask RULE matching for file-path tools against BOTH the literal path
//      AND every symlink-resolved form (parity: Claude's getPathsForPermissionCheck
//      threaded through deny/ask rule matching, utils/permissions/filesystem.ts).
// =============================================================================

// ---- Feature A: gh exfil guard (unit) --------------------------------------

test('ghArgsTargetForeignHost: a normal OWNER/REPO spec is safe', () => {
  assert.equal(ghArgsTargetForeignHost(['--repo', 'cli/cli']), false);
  assert.equal(ghArgsTargetForeignHost(['--repo=cli/cli']), false);
  assert.equal(ghArgsTargetForeignHost(['123', '--json', 'number']), false);
});

test('ghArgsTargetForeignHost: a 3-segment host/owner/repo is flagged', () => {
  assert.equal(ghArgsTargetForeignHost(['--repo', 'evil.host/owner/repo']), true);
  assert.equal(ghArgsTargetForeignHost(['--repo=evil.host/owner/repo']), true);
});

test('ghArgsTargetForeignHost: URL-scheme and ssh-style specs are flagged', () => {
  assert.equal(ghArgsTargetForeignHost(['https://evil.host/x']), true);
  assert.equal(ghArgsTargetForeignHost(['git@evil.host:owner/repo']), true);
});

// ---- Feature A: isGhInvocationReadOnly (unit) ------------------------------

test('isGhInvocationReadOnly: allowlisted read-only pairs are read-only', () => {
  assert.equal(isGhInvocationReadOnly(['gh', 'pr', 'list']), true);
  assert.equal(isGhInvocationReadOnly(['gh', 'issue', 'view', '42']), true);
  assert.equal(isGhInvocationReadOnly(['gh', 'search', 'repos', '--language', 'go']), true);
  assert.equal(isGhInvocationReadOnly(['gh', 'auth', 'status']), true);
});

test('isGhInvocationReadOnly: write / unlisted / bare forms are NOT read-only', () => {
  assert.equal(isGhInvocationReadOnly(['gh', 'pr', 'create']), false);
  assert.equal(isGhInvocationReadOnly(['gh', 'pr', 'merge', '42']), false);
  assert.equal(isGhInvocationReadOnly(['gh', 'api', '/repos/x/y']), false);
  assert.equal(isGhInvocationReadOnly(['gh', 'pr']), false); // no subcommand
  assert.equal(isGhInvocationReadOnly(['gh']), false);
  assert.equal(isGhInvocationReadOnly(['gh', '--version']), false);
});

test('isGhInvocationReadOnly: the exfil guard vetoes a foreign-host read', () => {
  assert.equal(isGhInvocationReadOnly(['gh', 'pr', 'list', '--repo', 'cli/cli']), true);
  assert.equal(isGhInvocationReadOnly(['gh', 'pr', 'list', '--repo', 'evil.host/o/r']), false);
});

test('isGhReadOnlyCommandSafe: compound-aware whole-command guard', () => {
  assert.equal(isGhReadOnlyCommandSafe('gh pr list && gh issue list'), true);
  assert.equal(isGhReadOnlyCommandSafe('gh pr list && gh pr create'), false);
  // a non-gh segment is not this guard's concern (classified elsewhere)
  assert.equal(isGhReadOnlyCommandSafe('ls && gh pr view 1'), true);
});

// ---- Feature A: classifier wiring (functional) -----------------------------

test('classifyBashCommand: read-only gh commands classify as safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('gh pr list'), 'safe');
  assert.equal(pm.classifyBashCommand('gh issue view 7 --json state'), 'safe');
  assert.equal(pm.classifyBashCommand('gh run view --log'), 'safe');
  assert.equal(pm.classifyBashCommand('gh search repos --language rust'), 'safe');
});

test('classifyBashCommand: gh write / foreign-host / api commands are not safe', () => {
  const pm = new PermissionManager();
  assert.notEqual(pm.classifyBashCommand('gh pr create --title x'), 'safe');
  assert.notEqual(pm.classifyBashCommand('gh pr merge 42'), 'safe');
  assert.notEqual(pm.classifyBashCommand('gh api /repos/o/r --method POST'), 'safe');
  // exfil: a foreign HOST/OWNER/REPO must not be auto-approved
  assert.equal(pm.classifyBashCommand('gh pr list --repo evil.host/o/r'), 'unsafe');
});

// ---- Feature A: end-to-end through check() in acceptEdits mode --------------

test('check(): acceptEdits auto-allows a read-only gh command, prompts a write', async () => {
  const pm = new PermissionManager();
  pm.setMode('acceptEdits');
  assert.equal(await pm.check('bash', { command: 'gh pr list' }), 'allow');
  assert.notEqual(await pm.check('bash', { command: 'gh pr create --title x' }), 'allow');
  // exfil form is not auto-allowed even in acceptEdits
  assert.notEqual(await pm.check('bash', { command: 'gh pr list --repo evil.host/o/r' }), 'allow');
});

// ---- Feature C: deny rule matches the symlink-resolved path -----------------

test('check(): a file_read deny rule matches through an in-workspace symlink', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u003-w85-'));
  try {
    const target = path.join(dir, 'secret.txt');
    fs.writeFileSync(target, 'top secret');
    const link = path.join(dir, 'innocent.txt');
    fs.symlinkSync(target, link);
    const resolved = fs.realpathSync(link); // the real path a deny rule can name

    const pm = new PermissionManager();
    // Deny the RESOLVED target; the agent passes the innocuous-looking symlink.
    pm.addRule({ tool: 'file_read', pattern: resolved, action: 'deny' });

    // Literal link path must still be caught via symlink resolution.
    assert.equal(await pm.check('file_read', { file_path: link }), 'deny');
    // Sanity: reading the real target directly is also denied.
    assert.equal(await pm.check('file_read', { file_path: resolved }), 'deny');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('check(): an ask rule is bypass-immune through a symlink in bypassPermissions', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u003-w85-ask-'));
  try {
    const target = path.join(dir, 'watched.txt');
    fs.writeFileSync(target, 'x');
    const link = path.join(dir, 'alias.txt');
    fs.symlinkSync(target, link);
    const resolved = fs.realpathSync(link);

    const pm = new PermissionManager();
    pm.setMode('bypassPermissions');
    pm.addRule({ tool: 'file_read', pattern: resolved, action: 'ask' });
    // bypassPermissions allows everything EXCEPT an explicit ask rule, which must
    // still fire — including when reached via the symlink alias.
    assert.equal(await pm.check('file_read', { file_path: link }), 'ask');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('check(): a non-matching file_read symlink is unaffected (no false deny)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u003-w85-neg-'));
  try {
    const target = path.join(dir, 'plain.txt');
    fs.writeFileSync(target, 'ok');
    const link = path.join(dir, 'ln.txt');
    fs.symlinkSync(target, link);
    const pm = new PermissionManager();
    pm.addRule({ tool: 'file_read', pattern: path.join(dir, 'other-secret.txt'), action: 'deny' });
    // The deny names a different file — the symlink read must NOT be denied by it.
    assert.notEqual(await pm.check('file_read', { file_path: link }), 'deny');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
