import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w96 — G6 + G19: per-subcommand (per-pipe-segment) independent
// permission evaluation. checkWithReason() now retains a `subcommandResults`
// map (segment → allow|deny|ask) for a COMPOUND bash command so a UI/executor
// can show WHICH specific segment forced the ask/deny instead of collapsing
// the whole chain to a single verdict. Deny-wins aggregation is preserved: the
// overall decision stays authoritative, the map is additive diagnostics.
// Mirrors Claude's BashPermissionRequest.subcommandResults
// (Map<string, PermissionResult>) + per-segment deny-wins ordering
// (tools/BashTool/bashCommandHelpers.ts L84-156).
// ---------------------------------------------------------------------------

test('G6/G19: a compound bash command with one denied segment retains a per-segment verdict map', async () => {
  const pm = new PermissionManager('acceptEdits', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  const verdict = await pm.checkWithReason('bash', { command: 'ls -la && rm foo.txt' });
  assert.equal(verdict.decision, 'deny', 'deny-wins: the denied segment forces an overall deny');
  assert.ok(verdict.subcommandResults, 'a compound command retains a subcommand verdict map');
  const results = verdict.subcommandResults!;
  assert.equal(results['ls -la'], 'allow', 'the read-only segment evaluates to allow');
  assert.equal(results['rm foo.txt'], 'deny', 'the denied segment evaluates to deny and is identifiable');
});

test('G6/G19: a pipe-separated compound is split per segment', async () => {
  const pm = new PermissionManager('acceptEdits', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  const verdict = await pm.checkWithReason('bash', { command: 'cat x | rm y' });
  assert.equal(verdict.decision, 'deny');
  assert.ok(verdict.subcommandResults);
  assert.equal(verdict.subcommandResults!['rm y'], 'deny');
  assert.equal(verdict.subcommandResults!['cat x'], 'allow');
});

test('G6/G19: a single (non-compound) command returns no subcommand map', async () => {
  const pm = new PermissionManager('acceptEdits', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  const verdict = await pm.checkWithReason('bash', { command: 'rm foo.txt' });
  assert.equal(verdict.decision, 'deny');
  assert.equal(verdict.subcommandResults, undefined, 'no map for a single command — nothing to disambiguate');
});

test('G6/G19: an allowed compound command carries no map (only ask/deny needs disambiguation)', async () => {
  const pm = new PermissionManager('acceptEdits');
  const verdict = await pm.checkWithReason('bash', { command: 'ls && pwd' });
  // Both segments are read-only/safe → overall allow, which early-returns without a map.
  assert.equal(verdict.decision, 'allow');
  assert.equal(verdict.subcommandResults, undefined);
});

test('G6/G19: deny-wins — a deny segment outranks an ask segment in the aggregate', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
    { tool: 'bash', pattern: 'curl *', action: 'ask' },
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  const verdict = await pm.checkWithReason('bash', { command: 'curl example.com && rm x' });
  assert.equal(verdict.decision, 'deny', 'deny outranks ask');
  assert.ok(verdict.subcommandResults);
  assert.equal(verdict.subcommandResults!['curl example.com'], 'ask');
  assert.equal(verdict.subcommandResults!['rm x'], 'deny');
});

test('G6/G19: a non-bash tool never produces a subcommand map', async () => {
  const pm = new PermissionManager('ask');
  const verdict = await pm.checkWithReason('file_read', { file_path: '/etc/passwd && rm x' });
  assert.equal(verdict.subcommandResults, undefined, 'only bash commands decompose into segments');
});
