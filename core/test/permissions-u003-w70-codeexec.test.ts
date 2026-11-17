import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';
import {
  isDangerousCodeExecPrefix,
  CROSS_PLATFORM_CODE_EXEC,
  DANGEROUS_BASH_PATTERNS,
} from '../src/dangerous-code-exec';

// ---------------------------------------------------------------------------
// U003-w70 — code-execution allow-rule downgrade.
//
// An allow rule like Bash(python:*) / Bash(npx:*) / Bash(bash:*) grants the
// model arbitrary code execution through an interpreter / package-runner /
// shell, silently bypassing the command safety classifier (interpreters
// classify as merely 'unsafe', not 'destructive', so the existing
// isDestructiveBash downgrade never caught them). check() now downgrades such a
// MATCHED allow rule to an explicit prompt in the rule-evaluated modes
// ('ask' and 'acceptEdits'). Mirrors Claude's dangerousPatterns.ts +
// isDangerousBashPermission / stripDangerousPermissionsForAutoMode.
// ---------------------------------------------------------------------------

test('DANGEROUS lists enumerate interpreters + package runners + shells', () => {
  for (const s of ['python', 'python3', 'python2', 'node', 'deno', 'tsx', 'ruby',
    'perl', 'php', 'lua', 'npx', 'bunx', 'npm run', 'yarn run', 'pnpm run',
    'bun run', 'bash', 'sh', 'ssh']) {
    assert.ok(CROSS_PLATFORM_CODE_EXEC.includes(s), `expected ${s} in CROSS_PLATFORM_CODE_EXEC`);
    assert.ok(DANGEROUS_BASH_PATTERNS.includes(s), `expected ${s} in DANGEROUS_BASH_PATTERNS`);
  }
  for (const s of ['zsh', 'fish', 'eval', 'exec', 'env', 'xargs', 'sudo']) {
    assert.ok(DANGEROUS_BASH_PATTERNS.includes(s), `expected ${s} in DANGEROUS_BASH_PATTERNS`);
    assert.ok(!CROSS_PLATFORM_CODE_EXEC.includes(s), `${s} is bash-only, not cross-platform`);
  }
});

test('isDangerousCodeExecPrefix flags over-broad code-exec rule shapes', () => {
  // Every shape variant for a named prefix.
  for (const c of ['python', 'python:*', 'python*', 'python *', 'python -c*',
    'PYTHON:*', '  node:* ', 'npx:*', 'npm run:*', 'npm run *',
    'bash:*', 'sh:*', 'ssh:*', 'deno:*', 'tsx:*', 'lua:*', 'php:*', 'ruby:*',
    'perl:*', 'python2:*', 'python3:*', 'zsh:*', 'fish:*', 'eval:*', 'exec:*',
    'env:*', 'xargs:*', 'sudo:*', 'bunx:*', 'bun run:*', 'yarn run:*', 'pnpm run:*']) {
    assert.equal(isDangerousCodeExecPrefix(c), true, `expected ${c} dangerous`);
  }
});

test('isDangerousCodeExecPrefix does NOT flag narrow / tool-wide / unrelated rules', () => {
  for (const c of [undefined, '', '*', '   ', 'node server.js', 'python manage.py migrate',
    'nodemon *', 'npm:*', 'npm install', 'git:*', 'curl*', 'ls *', 'nodejs-tool *',
    'python-wrapper *']) {
    assert.equal(isDangerousCodeExecPrefix(c), false, `expected ${JSON.stringify(c)} safe`);
  }
});

test('ask mode: a Bash(python *) allow rule is downgraded to a prompt (RED-GREEN)', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'python *', action: 'allow' }]);
  // Without the downgrade this returned 'allow' (silent arbitrary code exec).
  assert.equal(await pm.check('bash', { command: 'python app.py --serve' }), 'ask');
});

test('ask mode: Bash(npx *) / Bash(bash *) allow rules are downgraded', async () => {
  const npx = new PermissionManager('ask', [{ tool: 'bash', pattern: 'npx *', action: 'allow' }]);
  assert.equal(await npx.check('bash', { command: 'npx cowsay pwned' }), 'ask');
  const shell = new PermissionManager('ask', [{ tool: 'bash', pattern: 'bash *', action: 'allow' }]);
  assert.equal(await shell.check('bash', { command: "bash -c 'rm -rf /tmp/x'" }), 'ask');
});

test('acceptEdits mode: code-exec allow rule is downgraded (gap: destructive guard was ask-only) (RED-GREEN)', async () => {
  const pm = new PermissionManager('acceptEdits', [{ tool: 'bash', pattern: 'node *', action: 'allow' }]);
  // acceptEdits auto-allows only 'safe' bash; `node app.js` classifies 'unsafe'
  // so it reaches the rule loop, where the code-exec allow rule is downgraded.
  assert.equal(await pm.check('bash', { command: 'node app.js --port 9999' }), 'ask');
});

test('a NARROW code-exec allow rule still auto-approves (no downgrade)', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'python manage.py migrate', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'python manage.py migrate' }), 'allow');
});

test('tool-wide Bash(*) allow is honored, NOT downgraded (AMI autonomy grant preserved)', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: '*', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  // Even a python one-liner stays allowed under an explicit full-bash grant.
  assert.equal(await pm.check('bash', { command: "python -c 'print(1)'" }), 'allow');
});

test('an explicit deny still wins over a code-exec allow rule', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'python *', action: 'deny' },
    { tool: 'bash', pattern: 'python *', action: 'allow' },
  ]);
  assert.equal(await pm.check('bash', { command: "python -c 'x'" }), 'deny');
});

test('auto-allow mode is NOT affected (blanket autonomy keeps allowing)', async () => {
  const pm = new PermissionManager('auto-allow', [{ tool: 'bash', pattern: 'python *', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: "python -c 'x'" }), 'allow');
});

test('checkWithReason attributes the downgrade to a codeExec safetyCheck', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'python *', action: 'allow' }]);
  const res = await pm.checkWithReason('bash', { command: "python -c 'x'" });
  assert.equal(res.decision, 'ask');
  assert.equal(res.reason?.type, 'safetyCheck');
  assert.equal(res.reason && res.reason.type === 'safetyCheck' ? res.reason.check : null, 'codeExec');
  assert.match(res.message ?? '', /arbitrary code execution/);
});

test('codeExec reason attribution skips deny + non-code-exec allow rules', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },      // skipped (not allow)
    { tool: 'bash', pattern: 'ls *', action: 'allow' },     // skipped (not code-exec)
    { tool: 'bash', pattern: 'python *', action: 'allow' }, // the dangerous match
  ]);
  const res = await pm.checkWithReason('bash', { command: 'python app.py' });
  assert.equal(res.decision, 'ask');
  assert.equal(res.reason && res.reason.type === 'safetyCheck' ? res.reason.check : null, 'codeExec');
});

test('codeExec reason attribution respects a non-matching paramPattern on a code-exec rule', async () => {
  const pm = new PermissionManager('ask', [
    // A scoped code-exec allow rule whose named-param gate does NOT match the input
    // is skipped; the plain code-exec rule below is the real cause.
    { tool: 'bash', pattern: 'python *', action: 'allow', paramPattern: { command: 'ruby *' } },
    { tool: 'bash', pattern: 'python *', action: 'allow' },
  ]);
  const res = await pm.checkWithReason('bash', { command: 'python app.py' });
  assert.equal(res.reason && res.reason.type === 'safetyCheck' ? res.reason.check : null, 'codeExec');
});

test('a non-matching code-exec allow rule falls back to the mode reason (no misattribution)', async () => {
  // `node *` is a code-exec prefix but does NOT match a python command, so the
  // default 'ask' is attributed to the mode, not to a code-exec downgrade.
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'node *', action: 'allow' }]);
  const res = await pm.checkWithReason('bash', { command: 'python app.py' });
  assert.equal(res.decision, 'ask');
  assert.equal(res.reason?.type, 'mode');
});

test('getDangerousCodeExecAllowRules lists only code-exec allow rules (introspection)', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'python *', action: 'allow' },
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
    { tool: 'bash', pattern: 'node *', action: 'deny' },
    { tool: 'bash', pattern: '*', action: 'allow' },
  ]);
  const flagged = pm.getDangerousCodeExecAllowRules();
  assert.equal(flagged.length, 1);
  assert.equal(flagged[0]!.pattern, 'python *');
});
