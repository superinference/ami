import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getDestructiveCommandWarning,
  validateAllowedPromptRule,
  partitionAllowedPromptRules,
  MAX_ALLOWED_PROMPT_LENGTH,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w82): two additive, pure-core permission-parity capabilities, each
// wired through an EXISTING consumer (no new field, minimal cross-layer churn):
//
//  1. Extended getDestructiveCommandWarning coverage — several commands that
//     classifyBashCommand already escalates to a `destructive` prompt
//     (shred/truncate/rmdir/kill/pkill/killall) had NO per-pattern warning and
//     fell back to a generic "is destructive" line. Add specific, human-readable
//     notes, distinguishing CERTAINTY ("will" for a deterministic effect like
//     `shred`, "may" for conditional ones) — parity with Claude's
//     destructiveCommandWarning.ts will/may wording. Purely informational;
//     surfaced through the existing `destructive` safetyCheck decision reason.
//
//  2. validateAllowedPromptRule / partitionAllowedPromptRules — a guard for the
//     MODEL-supplied plan-mode `allowedPrompts` turned into `allow` rules on
//     plan-mode exit. Without it a crafted/injected `*` / `` / `**` entry becomes
//     a blanket `bash(*)` allow-everything rule (privilege escalation). Parity
//     with Claude's ExitPlanModeV2Tool treating allowedPrompts as scoped action
//     descriptions, never a match-all glob.
// ---------------------------------------------------------------------------

// --- Feature 1: extended destructive-command warnings ----------------------

test('getDestructiveCommandWarning: secure-delete shred carries a deterministic "will" note', () => {
  assert.equal(
    getDestructiveCommandWarning('shred report.txt'),
    'Note: will permanently overwrite and delete the target files',
  );
});

test('getDestructiveCommandWarning: truncate carries a conditional "may" note', () => {
  assert.equal(
    getDestructiveCommandWarning('truncate -s 0 app.log'),
    'Note: may shrink or empty the target file',
  );
});

test('getDestructiveCommandWarning: rmdir removes directories', () => {
  assert.equal(getDestructiveCommandWarning('rmdir build'), 'Note: may remove directories');
});

test('getDestructiveCommandWarning: process-control commands', () => {
  assert.equal(getDestructiveCommandWarning('kill 1234'), 'Note: may terminate a running process');
  assert.equal(getDestructiveCommandWarning('kill -9 1234'), 'Note: may forcibly terminate a process');
  assert.equal(getDestructiveCommandWarning('pkill node'), 'Note: may terminate matching processes by name');
  assert.equal(getDestructiveCommandWarning('killall node'), 'Note: may terminate matching processes by name');
});

test('getDestructiveCommandWarning: new patterns are statement-anchored (no in-word false positive)', () => {
  // `skill` must not trigger the kill pattern; `autotruncate` must not trigger truncate.
  assert.equal(getDestructiveCommandWarning('run-skill start'), null);
  assert.equal(getDestructiveCommandWarning('echo autotruncate'), null);
});

test('getDestructiveCommandWarning: still returns the pre-existing rm/git warnings (no regression)', () => {
  assert.equal(getDestructiveCommandWarning('rm -rf build'), 'Note: may recursively force-remove files');
  assert.equal(getDestructiveCommandWarning('git reset --hard HEAD~1'), 'Note: may discard uncommitted changes');
});

test('checkWithReason surfaces the specific shred warning in the destructive safetyCheck', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'shred report.txt' });
  assert.equal(r.decision, 'ask');
  assert.ok(r.reason && r.reason.type === 'safetyCheck' && r.reason.check === 'destructive');
  assert.match(r.message ?? '', /will permanently overwrite and delete the target files/);
});

// --- Feature 2: allowedPrompts rule validation -----------------------------

test('validateAllowedPromptRule: rejects a non-string', () => {
  assert.deepEqual(validateAllowedPromptRule(123), { safe: false, reason: 'prompt must be a string' });
  assert.deepEqual(validateAllowedPromptRule(undefined), { safe: false, reason: 'prompt must be a string' });
});

test('validateAllowedPromptRule: rejects empty / whitespace-only', () => {
  assert.equal(validateAllowedPromptRule('').safe, false);
  assert.equal(validateAllowedPromptRule('   ').safe, false);
});

test('validateAllowedPromptRule: rejects match-all glob patterns', () => {
  for (const p of ['*', '**', '***', '.*', '*:*', ':*', '*.*', '? *', ' * ', '*  ?']) {
    const r = validateAllowedPromptRule(p);
    assert.equal(r.safe, false, `expected ${JSON.stringify(p)} to be rejected`);
    assert.match(r.reason ?? '', /too broad/);
  }
});

test('validateAllowedPromptRule: rejects an over-long pattern', () => {
  const long = 'a'.repeat(MAX_ALLOWED_PROMPT_LENGTH + 1);
  const r = validateAllowedPromptRule(long);
  assert.equal(r.safe, false);
  assert.match(r.reason ?? '', /exceeds/);
});

test('validateAllowedPromptRule: accepts a normal scoped pattern (incl. a narrow trailing wildcard)', () => {
  assert.deepEqual(validateAllowedPromptRule('npm test'), { safe: true });
  assert.deepEqual(validateAllowedPromptRule('ls*'), { safe: true });
  assert.deepEqual(validateAllowedPromptRule('git log --oneline'), { safe: true });
});

test('partitionAllowedPromptRules: splits valid from rejected and keeps the entry shape', () => {
  const entries = [
    { tool: 'bash', prompt: 'npm test' },
    { tool: 'bash', prompt: '*' },
    { tool: 'file_read', prompt: 'src/**' },
    { prompt: '' },
    { tool: 'bash', prompt: 'ls*' },
  ];
  const { valid, rejected } = partitionAllowedPromptRules(entries);
  assert.deepEqual(valid, [
    { tool: 'bash', prompt: 'npm test' },
    { tool: 'file_read', prompt: 'src/**' },
    { tool: 'bash', prompt: 'ls*' },
  ]);
  assert.equal(rejected.length, 2);
  assert.equal(rejected[0]!.entry.prompt, '*');
  assert.match(rejected[0]!.reason, /too broad/);
  assert.equal(rejected[1]!.entry.prompt, '');
});

test('partitionAllowedPromptRules: empty input yields empty partitions', () => {
  assert.deepEqual(partitionAllowedPromptRules([]), { valid: [], rejected: [] });
});
