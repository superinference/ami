import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PermissionManager,
  hasUnbalancedQuotes,
  type AutoModeSafetyVerdict,
} from '../src/permissions';

// =============================================================================
// U-perm1 / w123 — permissions.ts parity gaps
// =============================================================================

describe('U-perm1/w123 — env-prefix-scoped allow rules (Bash env assignment kept in prefix)', () => {
  it('matches a rule scoped WITH a leading env-var assignment (GOEXPERIMENT in SAFE_ENV_VARS)', async () => {
    // GOEXPERIMENT is a SAFE_ENV_VARS member, so AMI strips it from the command
    // before matching — which, before the fix, made a rule that NAMES the env
    // prefix unmatchable (decision 'ask'). RED on revert: this returns 'ask'.
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'GOEXPERIMENT=synctest go test:*', action: 'allow' },
    ]);
    assert.equal(await pm.check('bash', { command: 'GOEXPERIMENT=synctest go test ./...' }), 'allow');
  });

  it('matches an env-scoped rule for a non-safe env var too (PYTHONPATH)', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'PYTHONPATH=/tmp python3:*', action: 'allow' },
    ]);
    assert.equal(await pm.check('bash', { command: 'PYTHONPATH=/tmp python3 script.py' }), 'allow');
  });

  it('does NOT let a bare rule auto-allow an env-prefixed command of a different base (no broadening)', async () => {
    // The rule names no env prefix, so the raw-match arm never fires: an
    // unsafe-env-prefixed `curl` must not be auto-allowed by a bare `curl:*` rule
    // (the unsafe env prefix stays and breaks the match → prompt).
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'curl:*', action: 'allow' },
    ]);
    assert.notEqual(await pm.check('bash', { command: 'EVIL=x curl http://host' }), 'allow');
  });

  it('an env-scoped DENY rule blocks the matching env-prefixed command', async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'bash', pattern: 'LD_PRELOAD=/x./evil ./app:*', action: 'deny' },
    ]);
    assert.equal(await pm.check('bash', { command: 'LD_PRELOAD=/x./evil ./app --run' }), 'deny');
  });
});

describe('U-perm1/w123 — fail-closed on unparseable (unbalanced-quote) commands', () => {
  it('hasUnbalancedQuotes flags an open double quote', () => {
    assert.equal(hasUnbalancedQuotes('echo "unterminated'), true);
  });

  it('hasUnbalancedQuotes flags an open single quote', () => {
    assert.equal(hasUnbalancedQuotes("echo 'open"), true);
  });

  it('hasUnbalancedQuotes accepts balanced quotes', () => {
    assert.equal(hasUnbalancedQuotes('echo "a b" \'c d\''), false);
    assert.equal(hasUnbalancedQuotes('echo hi'), false);
  });

  it('hasUnbalancedQuotes treats a double quote inside single quotes as literal', () => {
    assert.equal(hasUnbalancedQuotes("echo 'he said \"hi\"'"), false);
  });

  it('hasUnbalancedQuotes honors a backslash-escaped quote outside single quotes', () => {
    assert.equal(hasUnbalancedQuotes('echo \\"'), false);
  });

  it('hasUnbalancedQuotes ignores a backslash inside single quotes (literal)', () => {
    // bash: `\` is literal inside single quotes, so this closes normally.
    assert.equal(hasUnbalancedQuotes("echo '\\'"), false);
  });

  it("classifies an unparseable command as 'unsafe' (fail-closed)", () => {
    const pm = new PermissionManager('ask', []);
    // RED on revert: naive splitting sees a bare `echo` → 'safe'.
    assert.equal(pm.classifyBashCommand('echo "unterminated'), 'unsafe');
  });

  it("still classifies a balanced safe command 'safe'", () => {
    const pm = new PermissionManager('ask', []);
    assert.equal(pm.classifyBashCommand('echo "all good"'), 'safe');
  });

  it('does not false-trip on a quote inside a heredoc body', () => {
    const pm = new PermissionManager('ask', []);
    // The `'` lives in the heredoc body (not shell syntax) — stripped before the
    // balance check, so the command is not forced unsafe by the quote guard.
    assert.notEqual(
      pm.classifyBashCommand("cat <<EOF\nit's fine\nEOF"),
      'unsafe',
    );
  });

  it('E2E through check(): an unparseable bash command prompts instead of auto-allowing', async () => {
    // acceptEdits auto-allows a bash command classified 'safe'. A balanced safe read
    // is waved through; the unparseable variant is now 'unsafe' and must prompt.
    // RED on revert: `echo "unterminated` classifies 'safe' and is auto-allowed.
    const pm = new PermissionManager('acceptEdits', []);
    assert.equal(await pm.check('bash', { command: 'echo hi' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'echo "unterminated' }), 'ask');
  });
});

describe('U-perm1/w123 — passthrough (abstain) safety-classifier verdict', () => {
  function pmWith(verdict: AutoModeSafetyVerdict | undefined): PermissionManager {
    const pm = new PermissionManager('auto-allow', []);
    pm.setAutoModeSafetyClassifier(() => verdict);
    return pm;
  }

  it("treats an explicit 'passthrough' verdict as abstain (deterministic allow stands)", async () => {
    const pm = pmWith({ decision: 'passthrough' });
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
    // Abstain records no denial.
    assert.equal(pm.getRecentDenials().length, 0);
  });

  it("'passthrough' behaves identically to returning undefined", async () => {
    const abstain = pmWith({ decision: 'passthrough' });
    const undef = pmWith(undefined);
    assert.equal(await abstain.check('bash', { command: 'ls' }), 'allow');
    assert.equal(await undef.check('bash', { command: 'ls' }), 'allow');
  });

  it("a 'deny' verdict still blocks and records auto-mode classifier provenance", async () => {
    const pm = pmWith({ decision: 'deny', reason: 'blocked by policy' });
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
    const denials = pm.getRecentDenials();
    assert.equal(denials.length, 1);
    assert.equal(denials[0]!.source, 'classifier');
    assert.equal(denials[0]!.classifier, 'auto-mode');
    assert.equal(denials[0]!.reason, 'blocked by policy');
  });

  it("an 'ask' verdict downgrades to a prompt without recording a denial", async () => {
    const pm = pmWith({ decision: 'ask', reason: 'needs review' });
    assert.equal(await pm.check('bash', { command: 'ls' }), 'ask');
    assert.equal(pm.getRecentDenials().length, 0);
  });
});
