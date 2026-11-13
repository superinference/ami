/**
 * POV: core/core/src/permissions.ts — behavioral matrix.
 *
 * Every expected value in this file was computed by actually executing the code
 * (not by reading source and `.includes`-ing a string). We drive the REAL
 * PermissionManager + the REAL exported helpers and assert on their verdicts.
 *
 * Covers: bash classification (safe/unsafe/destructive), allow/deny rule
 * matching + ordering (deny unconditionally wins), permission modes
 * (ask/auto-allow/deny-all/plan/acceptEdits/bypassPermissions), hardline
 * override, dangerouslyDisableSandbox, credential/destructive downgrades,
 * verdictRank + holdsOver + mostRestrictiveVerdict ordering,
 * containsUnquotedExpansion / containsShellSubstitution, isBuildConfigFile,
 * unwrapShellControlStructures, path classification, denial tracking + circuit
 * breaker, rule management, persistence, normalizePermissionMode, PolicyMemo.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  PermissionManager,
  PolicyMemo,
  PERMISSION_MODES,
  BASH_CLASSIFICATIONS,
  normalizePermissionMode,
  verdictRank,
  holdsOver,
  mostRestrictiveVerdict,
  containsUnquotedExpansion,
  containsShellSubstitution,
  isBuildConfigFile,
  unwrapShellControlStructures,
  normalizeLineContinuations,
  stripEnvVarPrefix,
  stripSafeEnvVars,
  detectCommandChaining,
  detectHardlineCommand,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// classifyBashCommand — safe / unsafe / destructive matrix
// ---------------------------------------------------------------------------

test('classifyBashCommand: safe read-only commands', () => {
  const pm = new PermissionManager();
  for (const cmd of ['ls -la', 'cat file.txt', 'grep foo bar', 'echo hi', 'pwd', 'git status', 'make build', 'ls | grep f', 'sed s/a/b/ f', 'find . -name x', 'docker ps', '', '# just a comment']) {
    assert.equal(pm.classifyBashCommand(cmd), 'safe', `${JSON.stringify(cmd)} should be safe`);
  }
});

test('classifyBashCommand: unsafe mutating commands', () => {
  const pm = new PermissionManager();
  for (const cmd of ['git push', 'git commit -m x', 'npm install', 'mv a b', 'cp a b', 'mkdir d', 'touch f', 'curl http://x', 'sed -i s/a/b/ f', 'docker run x', 'make clean', 'unknowncmd', 'cat ~/.ssh/id_rsa']) {
    assert.equal(pm.classifyBashCommand(cmd), 'unsafe', `${JSON.stringify(cmd)} should be unsafe`);
  }
});

test('classifyBashCommand: destructive commands', () => {
  const pm = new PermissionManager();
  for (const cmd of ['rm foo', 'rm -rf dir', 'kill 1', 'git reset --hard', 'git clean', 'dd if=/dev/zero of=f', 'find . -delete', 'DROP TABLE users', 'DELETE FROM t', 'rmdir d']) {
    assert.equal(pm.classifyBashCommand(cmd), 'destructive', `${JSON.stringify(cmd)} should be destructive`);
  }
});

test('classifyBashCommand: credential read is elevated from safe to unsafe', () => {
  const pm = new PermissionManager();
  // `cat` alone is safe, but a credential target must never auto-classify safe.
  assert.equal(pm.classifyBashCommand('cat somefile.txt'), 'safe');
  assert.equal(pm.classifyBashCommand('cat ~/.ssh/id_rsa'), 'unsafe');
});

test('classifyBashCommand: compound chains take the most severe verdict', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('ls && rm x'), 'destructive');
  assert.equal(pm.classifyBashCommand('git status && npm install'), 'unsafe');
  assert.equal(pm.classifyBashCommand('ls && cat x'), 'safe');
});

test('BASH_CLASSIFICATIONS enumerates the three verdicts', () => {
  assert.deepEqual([...BASH_CLASSIFICATIONS], ['safe', 'unsafe', 'destructive']);
});

// ---------------------------------------------------------------------------
// check() — permission modes
// ---------------------------------------------------------------------------

test('check: ask mode defaults to ask when no rule matches', async () => {
  const pm = new PermissionManager('ask');
  assert.equal(await pm.check('file_read', {}), 'ask');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'ask');
});

test('check: auto-allow allows everything but deny rules + hardline still fire', async () => {
  const pm = new PermissionManager('auto-allow');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'allow');
  // hardline overrides even auto-allow
  assert.equal(await pm.check('bash', { command: 'rm -rf /' }), 'deny');
  // dangerouslyDisableSandbox is NOT auto-allowed — it always confirms
  assert.equal(await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: true }), 'ask');
});

test('check: auto-allow honors an explicit deny rule regardless of order', async () => {
  const pm = new PermissionManager('auto-allow', [
    { tool: 'bash', pattern: '*', action: 'allow' },
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  assert.equal(await pm.check('bash', { command: 'rm foo' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
});

test('check: deny-all denies every tool', async () => {
  const pm = new PermissionManager('deny-all');
  assert.equal(await pm.check('file_read', {}), 'deny');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  // hardline + dangerouslyDisableSandbox both resolve to deny too
  assert.equal(await pm.check('bash', { command: 'rm -rf /' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: true }), 'deny');
});

test('check: plan mode allows read-only tools, denies the rest', async () => {
  const pm = new PermissionManager('plan');
  for (const t of ['file_read', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search']) {
    assert.equal(await pm.check(t, {}), 'allow', `${t} should be allowed in plan mode`);
  }
  assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'deny');
});

test('check: acceptEdits auto-allows edits + safe bash, confirms build-config + unsafe bash', async () => {
  const pm = new PermissionManager('acceptEdits');
  assert.equal(await pm.check('file_edit', { file_path: 'src/x.ts' }), 'allow');
  assert.equal(await pm.check('file_write', { file_path: 'src/y.ts' }), 'allow');
  // build-config file must confirm even under acceptEdits
  assert.equal(await pm.check('file_edit', { file_path: 'package.json' }), 'ask');
  // safe bash auto-allowed; unsafe bash falls through to default ask
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  assert.equal(await pm.check('bash', { command: 'npm install' }), 'ask');
});

test('check: bypassPermissions allows generally but confirms safety-sensitive paths', async () => {
  const pm = new PermissionManager('bypassPermissions');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  assert.equal(await pm.check('file_edit', { file_path: 'src/x.ts' }), 'allow');
  assert.equal(await pm.check('bash', { command: 'cat .ssh/known_hosts' }), 'ask');
  assert.equal(await pm.check('file_edit', { file_path: 'project/.git/config' }), 'ask');
});

// ---------------------------------------------------------------------------
// check() — rule matching, ordering, downgrades
// ---------------------------------------------------------------------------

test('check: wildcard allow is downgraded to ask for destructive bash', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: '*', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  assert.equal(await pm.check('bash', { command: 'rm foo' }), 'ask');
});

test('check: a deny rule unconditionally wins over an earlier allow', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: '*', action: 'allow' },
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  assert.equal(await pm.check('bash', { command: 'rm foo' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
});

test('check: credential-targeting bash confirms even under a wildcard allow', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: '*', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'ask');
});

test('check: glob rule pattern matches bash prefix', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git *', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  assert.equal(await pm.check('bash', { command: 'ls' }), 'ask');
});

test('check: tool-name glob in a rule scopes by namespace', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'github:*', action: 'allow' }]);
  assert.equal(await pm.check('github:create_issue', {}), 'allow');
  assert.equal(await pm.check('gitlab:create_issue', {}), 'ask');
});

test('check: a tracked denial of the exact invocation auto-denies', async () => {
  const pm = new PermissionManager('ask');
  const input = { command: 'some-weird-cmd' };
  pm.trackDenial('bash', input);
  assert.equal(await pm.check('bash', input), 'deny');
});

// ---------------------------------------------------------------------------
// verdict ranking & hold-over
// ---------------------------------------------------------------------------

test('verdictRank orders deny > ask = modify > allow > unknown', () => {
  assert.equal(verdictRank('deny'), 3);
  assert.equal(verdictRank('ask'), 2);
  assert.equal(verdictRank('modify'), 2);
  assert.equal(verdictRank('allow'), 1);
  assert.equal(verdictRank('nonsense'), 0);
});

test('holdsOver: stronger held verdict holds; weaker does not', () => {
  assert.equal(holdsOver({ decision: 'deny' }, { decision: 'allow' }), true);
  assert.equal(holdsOver({ decision: 'allow' }, { decision: 'deny' }), false);
});

test('holdsOver: at equal rank, an attributed held verdict beats an unattributed answer', () => {
  assert.equal(holdsOver({ decision: 'ask', rule: 'r1' }, { decision: 'ask' }), true);
  assert.equal(holdsOver({ decision: 'ask' }, { decision: 'ask' }), false);
  assert.equal(holdsOver({ decision: 'ask', rule: 'a' }, { decision: 'ask', source: 'b' }), false);
});

test('mostRestrictiveVerdict: empty -> allow; picks the strongest', () => {
  assert.deepEqual(mostRestrictiveVerdict([]), { decision: 'allow' });
  assert.equal(mostRestrictiveVerdict([{ decision: 'allow' }, { decision: 'deny' }]).decision, 'deny');
  assert.equal(mostRestrictiveVerdict([{ decision: 'allow' }, { decision: 'ask' }]).decision, 'ask');
  assert.equal(mostRestrictiveVerdict([{ decision: 'allow' }]).decision, 'allow');
});

// ---------------------------------------------------------------------------
// expansion / substitution detectors
// ---------------------------------------------------------------------------

test('containsUnquotedExpansion: flags $VAR, globs, brackets; tolerates quoted / plain', () => {
  assert.equal(containsUnquotedExpansion('echo $HOME'), true);
  assert.equal(containsUnquotedExpansion('ls *.ts'), true);
  assert.equal(containsUnquotedExpansion('cat file?.txt'), true);
  assert.equal(containsUnquotedExpansion('ls [abc]'), true);
  assert.equal(containsUnquotedExpansion('cmd $(date)'), true);
  assert.equal(containsUnquotedExpansion('echo "$HOME"'), false);
  assert.equal(containsUnquotedExpansion("echo '$HOME'"), false);
  assert.equal(containsUnquotedExpansion('echo hello'), false);
});

test('containsShellSubstitution: flags $VAR / $(cmd), tolerates globs + quoted', () => {
  assert.equal(containsShellSubstitution('echo $HOME'), true);
  assert.equal(containsShellSubstitution('cmd $(date)'), true);
  assert.equal(containsShellSubstitution('ls *.ts'), false);
  assert.equal(containsShellSubstitution('ls [abc]'), false);
  assert.equal(containsShellSubstitution('echo "$HOME"'), false);
  assert.equal(containsShellSubstitution('echo hello'), false);
});

// ---------------------------------------------------------------------------
// build-config detection
// ---------------------------------------------------------------------------

test('isBuildConfigFile: basenames, path patterns, and negatives', () => {
  for (const f of ['package.json', 'Makefile', 'Dockerfile', '.env', 'pyproject.toml', 'cargo.toml', 'go.mod']) {
    assert.equal(isBuildConfigFile(f), true, `${f} should be a build-config file`);
  }
  for (const f of ['/home/u/.github/workflows/ci.yml', '/home/u/.ssh/config', '/home/u/.git/hooks/pre-commit', '/home/u/.aws/credentials']) {
    assert.equal(isBuildConfigFile(f), true, `${f} should be a build-config path`);
  }
  for (const f of ['README.md', 'src/index.ts', 'notes.txt']) {
    assert.equal(isBuildConfigFile(f), false, `${f} should NOT be a build-config file`);
  }
});

// ---------------------------------------------------------------------------
// shell control-structure unwrapping + misc string helpers
// ---------------------------------------------------------------------------

test('unwrapShellControlStructures: extracts loop/if bodies, leaves plain commands', () => {
  const loop = unwrapShellControlStructures('for f in *.ts; do cat "$f"; done');
  assert.ok(loop.includes('cat "$f"'));
  assert.ok(!loop.includes('done') && !loop.includes('for '));

  const cond = unwrapShellControlStructures('if test -f x; then cat x; else echo no; fi');
  assert.ok(cond.includes('cat x') && cond.includes('echo no'));
  assert.ok(!cond.includes('fi') && !cond.includes('then'));

  assert.equal(unwrapShellControlStructures('cat x'), 'cat x');
});

test('normalizeLineContinuations joins backslash-newline', () => {
  assert.equal(normalizeLineContinuations('a \\\nb'), 'a b');
  assert.equal(normalizeLineContinuations('plain'), 'plain');
});

test('stripEnvVarPrefix strips assignments and shell wrappers', () => {
  assert.equal(stripEnvVarPrefix('FOO=1 bash -c ls'), 'ls');
  assert.equal(stripEnvVarPrefix('ls -la'), 'ls -la');
});

test('stripSafeEnvVars removes safe env assignments but keeps unsafe ones', () => {
  assert.equal(stripSafeEnvVars('NODE_ENV=prod FOO=bar ls'), 'FOO=bar ls');
  assert.equal(stripSafeEnvVars('NODE_ENV=prod ls'), 'ls');
});

test('detectCommandChaining reports operators and segment count', () => {
  const r = detectCommandChaining('a && b || c; d');
  assert.equal(r.chained, true);
  assert.deepEqual(r.operators.sort(), ['&&', ';', '||']);
  assert.equal(r.count, 4);
  const single = detectCommandChaining('ls -la');
  assert.equal(single.chained, false);
  assert.equal(single.count, 1);
});

test('detectHardlineCommand blocks catastrophic commands, passes benign ones', () => {
  assert.equal(detectHardlineCommand('rm -rf /').blocked, true);
  assert.equal(detectHardlineCommand('mkfs.ext4 /dev/sda1').blocked, true);
  assert.equal(detectHardlineCommand('shutdown now').blocked, true);
  assert.equal(detectHardlineCommand('ls -la').blocked, false);
  assert.equal(detectHardlineCommand('ls -la').description, null);
});

// ---------------------------------------------------------------------------
// checkDenyRules
// ---------------------------------------------------------------------------

test('checkDenyRules: pattern match, no-pattern blanket deny, reason passthrough', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'rm *', action: 'deny', reason: 'no removes' },
  ]);
  const hit = pm.checkDenyRules('bash', 'rm foo');
  assert.equal(hit.denied, true);
  assert.equal(hit.reason, 'no removes');
  assert.equal(pm.checkDenyRules('bash', 'ls').denied, false);

  const blanket = new PermissionManager('ask', [{ tool: 'web_fetch', action: 'deny' }]);
  assert.equal(blanket.checkDenyRules('web_fetch', 'http://x').denied, true);
  assert.equal(blanket.checkDenyRules('bash', 'ls').denied, false);
});

// ---------------------------------------------------------------------------
// denial tracking + circuit breaker
// ---------------------------------------------------------------------------

test('denial tracking: track / isDenied / clear', () => {
  const pm = new PermissionManager();
  const input = { command: 'foo' };
  assert.equal(pm.isDenied('bash', input), false);
  pm.trackDenial('bash', input);
  assert.equal(pm.isDenied('bash', input), true);
  pm.clearDenials();
  assert.equal(pm.isDenied('bash', input), false);
});

test('circuit breaker trips on consecutive denials and resets on success', () => {
  const pm = new PermissionManager();
  assert.equal(pm.shouldAbort(), false);
  pm.recordDenial();
  pm.recordDenial();
  assert.equal(pm.shouldAbort(), false);
  pm.recordDenial();
  assert.equal(pm.shouldAbort(), true, '3 consecutive denials should trip the breaker');
  pm.recordSuccess();
  assert.equal(pm.shouldAbort(), false, 'success resets consecutive counter');
});

test('circuit breaker trips on total denials and fully resets', () => {
  const pm = new PermissionManager();
  for (let i = 0; i < 20; i++) {
    pm.recordDenial();
    pm.recordSuccess(); // keep consecutive low so only the TOTAL gate can trip
  }
  assert.equal(pm.shouldAbort(), true, '20 total denials should trip the breaker');
  pm.resetCircuitBreaker();
  assert.equal(pm.shouldAbort(), false);
});

// ---------------------------------------------------------------------------
// mode + rule management
// ---------------------------------------------------------------------------

test('getMode / setMode and the bypass guard', () => {
  const pm = new PermissionManager();
  assert.equal(pm.getMode(), 'ask');
  assert.equal(pm.setMode('acceptEdits'), true);
  assert.equal(pm.getMode(), 'acceptEdits');

  pm.setDisableBypass(true);
  assert.equal(pm.isDisableBypass(), true);
  assert.equal(pm.setMode('bypassPermissions'), false, 'bypass blocked by policy');
  assert.equal(pm.setMode('auto-allow'), false, 'auto-allow blocked by policy');
  assert.equal(pm.getMode(), 'acceptEdits', 'mode unchanged after a blocked setMode');
  assert.equal(pm.setMode('deny-all'), true, 'non-bypass modes still allowed');
});

test('addRule prepends (later rules win) and getRules returns a copy', () => {
  const pm = new PermissionManager();
  pm.addRule({ tool: 'bash', pattern: 'a', action: 'allow' });
  pm.addRule({ tool: 'bash', pattern: 'b', action: 'deny' });
  const rules = pm.getRules();
  assert.equal(rules[0].pattern, 'b');
  rules.push({ tool: 'x', action: 'allow' }); // mutating the copy must not leak
  assert.equal(pm.getRules().length, 2);
});

test('removeRule removes matches and returns the count', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'x', action: 'allow', reason: 'scoped' },
    { tool: 'grep', action: 'allow' },
    { tool: 'bash', pattern: 'y', action: 'allow', reason: 'scoped' },
  ]);
  const removed = pm.removeRule(r => r.reason === 'scoped');
  assert.equal(removed, 2);
  assert.equal(pm.getRules().length, 1);
});

test('detectShadowedRules flags a wildcard allow hidden behind an earlier deny', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'deny' },
    { tool: 'bash', pattern: 'ls', action: 'allow' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  assert.equal(shadows[0].rule.pattern, 'ls');
  assert.equal(shadows[0].shadowedBy.action, 'deny');
});

// ---------------------------------------------------------------------------
// normalizePermissionMode + PERMISSION_MODES
// ---------------------------------------------------------------------------

test('normalizePermissionMode: valid, legacy dontAsk, trimming, and rejection', () => {
  for (const m of PERMISSION_MODES) {
    assert.equal(normalizePermissionMode(m), m);
  }
  assert.equal(normalizePermissionMode('dontAsk'), 'deny-all');
  assert.equal(normalizePermissionMode('  ask '), 'ask');
  assert.equal(normalizePermissionMode('bogus'), null);
  assert.equal(normalizePermissionMode(''), null);
});

// ---------------------------------------------------------------------------
// path classification
// ---------------------------------------------------------------------------

test('classifyReadAccess / isPathAllowed with real temp dirs', () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-cwd-'));
  const sibling = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-sib-'));
  try {
    const inside = path.join(cwd, 'f.txt');
    const outside = path.join(sibling, 'g.txt');
    fs.writeFileSync(inside, 'x');
    fs.writeFileSync(outside, 'y');

    const pm = new PermissionManager();
    assert.equal(pm.classifyReadAccess(inside, cwd), 'allow');
    assert.equal(pm.classifyReadAccess(outside, cwd), 'prompt');
    assert.equal(pm.classifyReadAccess('/etc/passwd', cwd), 'deny');

    assert.equal(pm.isPathAllowed(inside, cwd), true);
    assert.equal(pm.isPathAllowed('/etc/passwd', cwd), false);
    assert.equal(pm.isPathAllowed(outside, cwd), false);

    // Granting the sibling root flips prompt -> allow.
    pm.registerAllowedRoot(sibling);
    assert.ok(pm.getAllowedReadRoots().some(r => r.includes('perm-sib-')));
    assert.equal(pm.classifyReadAccess(outside, cwd), 'allow');
    assert.equal(pm.isPathAllowed(outside, cwd), true);
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(sibling, { recursive: true, force: true });
  }
});

test('registerAllowedRoot ignores empty / non-string input', () => {
  const pm = new PermissionManager();
  pm.registerAllowedRoot('');
  // @ts-expect-error — intentional bad input
  pm.registerAllowedRoot(undefined);
  assert.equal(pm.getAllowedReadRoots().length, 0);
});

// ---------------------------------------------------------------------------
// persistence: loadRules / saveRules round-trip
// ---------------------------------------------------------------------------

test('saveRules then loadRules round-trips mode and rules', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-save-'));
  try {
    const src = new PermissionManager('acceptEdits', [
      { tool: 'bash', pattern: 'git *', action: 'allow' },
      { tool: 'web_fetch', action: 'deny', reason: 'offline' },
    ]);
    src.saveRules(dir);
    assert.ok(fs.existsSync(path.join(dir, '.ami', 'permissions.json')));

    const dst = new PermissionManager();
    dst.loadRules(dir);
    assert.equal(dst.getMode(), 'acceptEdits');
    const rules = dst.getRules();
    assert.ok(rules.some(r => r.tool === 'bash' && r.pattern === 'git *' && r.action === 'allow'));
    assert.ok(rules.some(r => r.tool === 'web_fetch' && r.action === 'deny' && r.reason === 'offline'));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('loadRules: missing file is a no-op; malformed file is ignored', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-load-'));
  try {
    const pm = new PermissionManager('ask');
    pm.loadRules(dir); // no .ami/permissions.json -> no throw, no change
    assert.equal(pm.getMode(), 'ask');
    assert.equal(pm.getRules().length, 0);

    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'), '{ not valid json', 'utf-8');
    pm.loadRules(dir); // malformed -> swallowed
    assert.equal(pm.getRules().length, 0);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('loadRules: a paramPattern with a non-string value is rejected (rule skipped)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-param-'));
  try {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.ami', 'permissions.json'),
      JSON.stringify({
        mode: 'ask',
        rules: [
          { tool: 'model_tool', action: 'allow', paramPattern: { model: 123 } }, // malformed -> skipped
          { tool: 'model_tool', action: 'allow', paramPattern: { model: 'opus*' } }, // valid -> kept
        ],
      }),
      'utf-8',
    );
    const pm = new PermissionManager();
    pm.loadRules(dir);
    const kept = pm.getRules();
    assert.equal(kept.length, 1);
    assert.deepEqual(kept[0].paramPattern, { model: 'opus*' });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// paramPattern matching through check()
// ---------------------------------------------------------------------------

test('check: paramPattern rule matches only when all named params match', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'model_tool', action: 'allow', paramPattern: { model: 'opus*' } },
  ]);
  assert.equal(await pm.check('model_tool', { model: 'opus-4' }), 'allow');
  assert.equal(await pm.check('model_tool', { model: 'sonnet-4' }), 'ask');
  assert.equal(await pm.check('model_tool', {}), 'ask');
});

// ---------------------------------------------------------------------------
// hasExplicitDeny
// ---------------------------------------------------------------------------

test('hasExplicitDeny: honors tracked denials and first-matching deny rule', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
    { tool: 'bash', pattern: '*', action: 'allow' },
  ]);
  assert.equal(pm.hasExplicitDeny('bash', { command: 'rm foo' }), true);
  assert.equal(pm.hasExplicitDeny('bash', { command: 'ls' }), false);
  const tracked = { command: 'weird' };
  pm.trackDenial('bash', tracked);
  assert.equal(pm.hasExplicitDeny('bash', tracked), true);
});

// ---------------------------------------------------------------------------
// policy memo: used by check() + standalone PolicyMemo
// ---------------------------------------------------------------------------

test('enableMemo caches check() decisions; disableMemo clears it', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git *', action: 'allow' }]);
  pm.enableMemo(10_000);
  assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  const memo = pm.getMemo();
  assert.ok(memo && memo.size() >= 1);
  pm.disableMemo();
  assert.equal(pm.getMemo(), null);
});

test('PolicyMemo: set/get, size, clear', () => {
  const m = new PolicyMemo(10_000);
  assert.equal(m.get('bash', 'h1'), null);
  m.set('bash', 'h1', 'allow');
  assert.equal(m.get('bash', 'h1'), 'allow');
  assert.equal(m.size(), 1);
  m.clear();
  assert.equal(m.size(), 0);
  assert.equal(m.get('bash', 'h1'), null);
});

test('PolicyMemo: entries expire after the TTL and prune removes them', () => {
  const m = new PolicyMemo(-1); // already-expired on insert
  m.set('bash', 'h', 'deny');
  assert.equal(m.get('bash', 'h'), null, 'expired entry reads as null');

  const m2 = new PolicyMemo(-1);
  m2.set('a', 'x', 'allow');
  m2.set('b', 'y', 'ask');
  const removed = m2.prune();
  assert.equal(removed, 2);
  assert.equal(m2.size(), 0);
});
