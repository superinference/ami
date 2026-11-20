import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  permissionRuleExtractPrefix,
  validatePermissionRule,
} from '../src/permissions.ts';

// U003 / w118 — gap: AMI VALIDATES Claude's `cmd:*` ruleContent prefix convention
// (validatePermissionRule accepts `bash(git:*)` and even suggests it as the
// canonical example) and stores it verbatim, but the raw glob matcher treated the
// `:` literally so a `git:*` allow rule never matched `git status`. A rule the user
// is explicitly told is valid silently granted nothing. This wires
// `permissionRuleExtractPrefix` into bash matching (parity with the reference's
// shellPermissionHelpers.ts L76-84).

describe('permissionRuleExtractPrefix (Claude :* prefix convention)', () => {
  it('extracts the command prefix from a `:*` rule content', () => {
    assert.equal(permissionRuleExtractPrefix('git commit:*'), 'git commit');
    assert.equal(permissionRuleExtractPrefix('git:*'), 'git');
    assert.equal(permissionRuleExtractPrefix('npm run build:*'), 'npm run build');
    assert.equal(permissionRuleExtractPrefix('  ls :*'), 'ls');
  });

  it('returns null when the content is not a prefix rule', () => {
    assert.equal(permissionRuleExtractPrefix('ls *'), null);   // AMI glob form
    assert.equal(permissionRuleExtractPrefix('git status'), null); // exact
    assert.equal(permissionRuleExtractPrefix(':*'), null);     // empty prefix
    assert.equal(permissionRuleExtractPrefix(''), null);
  });

  it('is consistent with the validator that blesses `bash(<prefix>:*)`', () => {
    // The validator advertises `bash(git:*)` as a valid example...
    assert.equal(validatePermissionRule('bash(git:*)').valid, true);
    // ...and rejects an empty prefix — the same boundary the extractor enforces.
    assert.equal(validatePermissionRule('bash(:*)').valid, false);
    assert.equal(permissionRuleExtractPrefix('git:*'), 'git');
    assert.equal(permissionRuleExtractPrefix(':*'), null);
  });
});

describe('PermissionManager honors a bash `cmd:*` prefix allow rule', () => {
  it('a `ls:*` allow rule grants `ls` and `ls <args>` (RED before the fix: was ask)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls:*', action: 'allow' }]);
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
    // Still matches the equivalent AMI glob form, unchanged.
    const glob = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls *', action: 'allow' }]);
    assert.equal(await glob.check('bash', { command: 'ls -la' }), 'allow');
  });

  it('a `git log:*` allow rule grants `git log` and its subcommands/args', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git log:*', action: 'allow' }]);
    assert.equal(await pm.check('bash', { command: 'git log' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'git log --oneline -n 5' }), 'allow');
  });

  it('is anchored on a word boundary — `ls:*` does NOT match `lsof`', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls:*', action: 'allow' }]);
    // lsof is a different command; no allow rule matches → default ask.
    assert.equal(await pm.check('bash', { command: 'lsof -i' }), 'ask');
  });

  it('matches every segment of a compound command (allow requires all)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls:*', action: 'allow' }]);
    assert.equal(await pm.check('bash', { command: 'ls -la && ls -R' }), 'allow');
    // One unrelated segment breaks the all-segments allow.
    assert.equal(await pm.check('bash', { command: 'ls -la && cat x' }), 'ask');
  });
});

describe('`cmd:*` prefix rule — safety guards still win (regression lock)', () => {
  it('a destructive command under `rm:*` still downgrades to ask', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm:*', action: 'allow' }]);
    // The prefix now matches, but the destructive-command guard forces a prompt.
    assert.equal(await pm.check('bash', { command: 'rm -rf build' }), 'ask');
  });

  it('a dangerous code-exec prefix under `python:*` still downgrades to ask', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'python:*', action: 'allow' }]);
    assert.equal(await pm.check('bash', { command: 'python evil.py' }), 'ask');
  });

  it('a `curl:*` DENY rule blocks the command (prefix convention on the deny side)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'curl:*', action: 'deny' }]);
    assert.equal(await pm.check('bash', { command: 'curl http://example.com' }), 'deny');
  });
});
