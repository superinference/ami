import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getSimpleCommandPrefix,
  getFirstWordPrefix,
  suggestAllowPrefix,
  BARE_SHELL_PREFIXES,
  SUBCOMMAND_AWARE_ROOTS,
  normalizeToolName,
  permissionRuleValueFromString,
  permissionRuleValueToString,
  escapeRuleContent,
  unescapeRuleContent,
  validatePermissionRule,
  PermissionManager,
} from '../src/permissions';

test('getSimpleCommandPrefix derives two-word subcommand prefixes', () => {
  assert.equal(getSimpleCommandPrefix('git commit -m "fix typo"'), 'git commit');
  assert.equal(getSimpleCommandPrefix('npm run build'), 'npm run');
  assert.equal(getSimpleCommandPrefix('NODE_ENV=prod npm run build'), 'npm run');
});

test('getSimpleCommandPrefix declines flags, numbers, paths, unsafe env', () => {
  assert.equal(getSimpleCommandPrefix('ls -la'), null);
  assert.equal(getSimpleCommandPrefix('chmod 755 file'), null);
  assert.equal(getSimpleCommandPrefix('cat file.txt'), null);
  assert.equal(getSimpleCommandPrefix('git /tmp'), null);
  assert.equal(getSimpleCommandPrefix('MY_SECRET=x npm run build'), null);
  assert.equal(getSimpleCommandPrefix('   '), null);
});

test('getFirstWordPrefix returns the first word unless shell/interp/wrapper', () => {
  assert.equal(getFirstWordPrefix('ls -la'), 'ls');
  assert.equal(getFirstWordPrefix('cargo build'), 'cargo');
  assert.equal(getFirstWordPrefix('bash -c "evil"'), null);
  assert.equal(getFirstWordPrefix('sudo rm -rf /x'), null);
  assert.equal(getFirstWordPrefix('node script.js'), null);
  assert.equal(getFirstWordPrefix('./script.sh'), null);
  assert.equal(getFirstWordPrefix('MY_SECRET=x ls'), null);
  assert.equal(getFirstWordPrefix(''), null);
});

test('BARE_SHELL_PREFIXES and SUBCOMMAND_AWARE_ROOTS hold the key denylist members', () => {
  for (const s of ['bash', 'env', 'sudo', 'node', 'python3', 'xargs']) {
    assert.ok(BARE_SHELL_PREFIXES.has(s), `expected ${s} in BARE_SHELL_PREFIXES`);
  }
  for (const s of ['git', 'docker', 'kubectl', 'npm']) {
    assert.ok(SUBCOMMAND_AWARE_ROOTS.has(s), `expected ${s} in SUBCOMMAND_AWARE_ROOTS`);
  }
});

test('suggestAllowPrefix prefers two-word, applies bare-root + NEVER_SUGGEST guards', () => {
  assert.equal(suggestAllowPrefix('git log --oneline'), 'git log');
  assert.equal(suggestAllowPrefix('npm install foo'), 'npm install');
  // bare subcommand-aware root → refuse (fall back to exact command)
  assert.equal(suggestAllowPrefix('git'), null);
  assert.equal(suggestAllowPrefix('docker'), null);
  // shell/interpreter → refuse
  assert.equal(suggestAllowPrefix('bash -c "evil"'), null);
  assert.equal(suggestAllowPrefix('node x.js'), null);
  // plain non-subcommand tool → first word is fine
  assert.equal(suggestAllowPrefix('ls -la'), 'ls');
});

test('normalizeToolName canonicalizes legacy/cross-framework names', () => {
  assert.equal(normalizeToolName('Bash'), 'bash');
  assert.equal(normalizeToolName('Edit'), 'file_edit');
  assert.equal(normalizeToolName('Write'), 'file_write');
  assert.equal(normalizeToolName('KillShell'), 'task_stop');
  assert.equal(normalizeToolName('bash'), 'bash'); // already canonical
  assert.equal(normalizeToolName('unknown_tool'), 'unknown_tool');
});

test('permissionRuleValueFromString / ToString round-trip with escaped parens', () => {
  assert.deepEqual(permissionRuleValueFromString('Bash'), { toolName: 'bash' });
  assert.deepEqual(permissionRuleValueFromString('Bash(npm install)'),
    { toolName: 'bash', ruleContent: 'npm install' });
  // Tool() and Tool(*) collapse to tool-wide
  assert.deepEqual(permissionRuleValueFromString('Bash()'), { toolName: 'bash' });
  assert.deepEqual(permissionRuleValueFromString('Bash(*)'), { toolName: 'bash' });
  // escaped parens in content
  const parsed = permissionRuleValueFromString('Bash(python -c "print\\(1\\)")');
  assert.equal(parsed.ruleContent, 'python -c "print(1)"');
  // round-trip
  assert.equal(permissionRuleValueToString({ toolName: 'bash', ruleContent: 'print(1)' }),
    'bash(print\\(1\\))');
  assert.equal(permissionRuleValueToString({ toolName: 'bash' }), 'bash');
  assert.equal(unescapeRuleContent(escapeRuleContent('a(b)c\\d')), 'a(b)c\\d');
});

test('permissionRuleValueFromString treats malformed/trailing as tool name', () => {
  assert.deepEqual(permissionRuleValueFromString('(foo)'), { toolName: '(foo)' });
  assert.deepEqual(permissionRuleValueFromString('Bash(x)y'), { toolName: 'Bash(x)y' });
});

test('validatePermissionRule reports actionable diagnostics', () => {
  assert.equal(validatePermissionRule('').valid, false);
  assert.equal(validatePermissionRule('   ').error, 'Permission rule cannot be empty');
  assert.equal(validatePermissionRule('Bash(ls').error, 'Mismatched parentheses');
  assert.equal(validatePermissionRule('Bash()').error, 'Empty parentheses');
  assert.equal(validatePermissionRule('()').error, 'Empty parentheses with no tool name');
  assert.equal(validatePermissionRule('bash(:*)').error, 'Prefix cannot be empty before :*');
  assert.equal(validatePermissionRule('bash(npm:* run)').error, 'The :* pattern must be at the end');
  assert.equal(validatePermissionRule('bash(npm run:*)').valid, true);
  assert.equal(validatePermissionRule('bash(git log)').valid, true);
  assert.equal(validatePermissionRule('file_edit(src/**)').valid, true);
});

test('toolNameMatches honours normalizeToolName so a Claude-named rule gates the AMI tool', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'Bash', pattern: '*', action: 'deny' }]);
  assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  const pm2 = new PermissionManager('ask', [{ tool: 'Edit', action: 'allow' }]);
  assert.equal(await pm2.check('file_edit', { file_path: '/tmp/x' }), 'allow');
});
