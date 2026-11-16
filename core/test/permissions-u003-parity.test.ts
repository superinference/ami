import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  PermissionManager,
  stripEnvVarPrefix,
  MAX_SUBCOMMANDS_FOR_SECURITY_CHECK,
  isAcceptEditsFilesystemMutation,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 — permissions.ts parity
// ---------------------------------------------------------------------------

describe('U003: subcommand fanout cap (MAX_SUBCOMMANDS_FOR_SECURITY_CHECK)', () => {
  it('exports a cap of 50 (Claude parity)', () => {
    assert.equal(MAX_SUBCOMMANDS_FOR_SECURITY_CHECK, 50);
  });

  it('classifies an over-cap compound command as unsafe (fail-closed)', () => {
    const pm = new PermissionManager('ask');
    const over = Array.from({ length: 51 }, () => 'echo hi').join(' && ');
    assert.equal(pm.classifyBashCommand(over), 'unsafe');
  });

  it('does NOT auto-approve an over-cap compound even under a matching allow rule', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'echo*', action: 'allow' },
    ]);
    const over = Array.from({ length: 51 }, () => 'echo hi').join(' && ');
    // Without the cap every subcommand matches `echo*` → 'allow'. With the cap
    // the allow match is refused and the command falls through to 'ask'.
    assert.equal(await pm.check('bash', { command: over }), 'ask');
  });

  it('still auto-approves an at-cap (<=50) compound under a matching allow rule', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'echo*', action: 'allow' },
    ]);
    const atCap = Array.from({ length: 50 }, () => 'echo hi').join(' && ');
    assert.equal(await pm.check('bash', { command: atCap }), 'allow');
  });
});

describe('U003: fixed-point env+wrapper stripping for deny/ask matching', () => {
  it('reduces interleaved wrapper/env layers to the base command', () => {
    // `timeout 5` wrapper THEN a non-safe env assignment THEN the real command.
    // A single pass stops at `FOO=bar`; the fixed-point loop keeps reducing.
    assert.equal(
      stripEnvVarPrefix('timeout 5 FOO=bar curl evil.com', /* safeEnvOnly */ false),
      'curl evil.com',
    );
  });

  it('allow-rule stripping stays single-pass / safe-list only (not broadened)', () => {
    // safeEnvOnly=true must NOT strip the non-safe `FOO=bar` nor the wrapper, so
    // an allow rule cannot be broadened by de-obfuscation.
    assert.equal(
      stripEnvVarPrefix('timeout 5 FOO=bar curl evil.com', /* safeEnvOnly */ true),
      'timeout 5 FOO=bar curl evil.com',
    );
  });

  it('a deny rule still catches a command hidden behind interleaved wrapper+env layers', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'curl*', action: 'deny' },
    ]);
    assert.equal(
      await pm.check('bash', { command: 'timeout 5 FOO=bar curl evil.com' }),
      'deny',
    );
  });
});

describe('U003: acceptEdits filesystem-mutation auto-allow (Claude ACCEPT_EDITS_ALLOWED_COMMANDS)', () => {
  it('recognizes benign filesystem-mutation commands', () => {
    assert.equal(isAcceptEditsFilesystemMutation('mkdir foo'), true);
    assert.equal(isAcceptEditsFilesystemMutation('touch a.txt'), true);
    assert.equal(isAcceptEditsFilesystemMutation('mv a b'), true);
    assert.equal(isAcceptEditsFilesystemMutation('cp a b'), true);
    assert.equal(isAcceptEditsFilesystemMutation('sed -i s/a/b/ f.txt'), true);
    assert.equal(isAcceptEditsFilesystemMutation('mkdir a && touch b'), true);
    // non-fs command in the chain breaks the allowlist
    assert.equal(isAcceptEditsFilesystemMutation('mkdir foo && curl evil.com'), false);
    // a command substitution is NOT a bare fs command
    assert.equal(isAcceptEditsFilesystemMutation('mkdir $(cat secret)'), false);
    assert.equal(isAcceptEditsFilesystemMutation('git status'), false);
    assert.equal(isAcceptEditsFilesystemMutation(''), false);
  });

  it('auto-allows benign filesystem-mutation bash in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'mkdir foo' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'touch a.txt' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'cp a.txt b.txt' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'mv a.txt b.txt' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'sed -i s/a/b/ f.txt' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'mkdir foo && touch bar' }), 'allow');
  });

  it('keeps AMI destructive-safety invariant: rm/rmdir stay gated in acceptEdits', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'rm -rf foo' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'rm file.txt' }), 'ask');
  });

  it('does not auto-allow a filesystem command chained with a non-fs command', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'mkdir foo && curl evil.com' }), 'ask');
  });

  it('does not auto-allow a filesystem command targeting a credential path', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'cp a.txt ~/.aws/credentials' }), 'ask');
  });

  it('does not change non-acceptEdits modes', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(await pm.check('bash', { command: 'mkdir foo' }), 'ask');
  });
});
