import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  PermissionManager,
  PERMISSION_RULE_SOURCES,
  coercePermissionRuleSource,
  isManagedPermissionSource,
  managedPermissionsPath,
  type PermissionRule,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// Test harness: point the managed config (and thus the managed permissions file)
// at an isolated temp dir via SUPERINFERENCE_MANAGED_CONFIG, so no real
// /etc/superinference file is read or written.
// ---------------------------------------------------------------------------
function withManagedDir(
  managed: Record<string, unknown> | null,
  fn: (dir: string) => void,
): void {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-managed-'));
  const prev = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  // Managed config lives at <dir>/config.json; managed permissions sit beside it.
  process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(dir, 'config.json');
  try {
    if (managed !== null) {
      fs.writeFileSync(managedPermissionsPath(), JSON.stringify(managed), 'utf-8');
    }
    fn(dir);
  } finally {
    if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

// ======================= Gap A: tiered rule source ==========================

test('PERMISSION_RULE_SOURCES is the wider superset including the 3 read-only sources', () => {
  for (const s of ['userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
    'policySettings', 'flagSettings', 'command']) {
    assert.ok((PERMISSION_RULE_SOURCES as readonly string[]).includes(s), `missing ${s}`);
  }
  assert.equal(PERMISSION_RULE_SOURCES.length, 8);
});

test('coercePermissionRuleSource validates against the known sources', () => {
  assert.equal(coercePermissionRuleSource('policySettings'), 'policySettings');
  assert.equal(coercePermissionRuleSource('command'), 'command');
  assert.equal(coercePermissionRuleSource('flagSettings'), 'flagSettings');
  assert.equal(coercePermissionRuleSource('nope'), undefined);
  assert.equal(coercePermissionRuleSource(42), undefined);
  assert.equal(coercePermissionRuleSource(undefined), undefined);
});

test('isManagedPermissionSource is true only for policySettings', () => {
  assert.equal(isManagedPermissionSource('policySettings'), true);
  assert.equal(isManagedPermissionSource('userSettings'), false);
  assert.equal(isManagedPermissionSource(undefined), false);
});

test('a loaded rule may carry its own source tag (falling back to the loader tier)', () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-'));
  try {
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(cwd, '.ami', 'permissions.json'),
      JSON.stringify({ rules: [
        { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'command' },
        { tool: 'bash', pattern: 'git status', action: 'allow' },
      ] }),
      'utf-8',
    );
    const pm = new PermissionManager();
    pm.loadRules(cwd);
    const rules = pm.getRules();
    assert.equal(rules.find(r => r.pattern === 'ls *')?.source, 'command');
    // No in-file tag → falls back to the loader's default tier (projectSettings).
    assert.equal(rules.find(r => r.pattern === 'git status')?.source, 'projectSettings');
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
  }
});

// ============== Gap B: allowManagedPermissionRulesOnly lockdown ==============

test('managedPermissionsPath resolves beside the managed config file', () => {
  const prev = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  process.env.SUPERINFERENCE_MANAGED_CONFIG = '/opt/co/config.json';
  try {
    assert.equal(managedPermissionsPath(), path.join('/opt/co', 'permissions.json'));
  } finally {
    if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
  }
});

test('loadManagedPermissions loads policy rules tagged policySettings at highest precedence', () => {
  withManagedDir({ rules: [{ tool: 'bash', pattern: 'rm *', action: 'deny' }] }, () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'cliArg' },
    ]);
    const warnings = pm.loadManagedPermissions();
    assert.deepEqual(warnings, []);
    assert.equal(pm.isManagedRulesOnly(), false); // no lockdown flag set
    const rules = pm.getRules();
    // Managed rule prepended → first.
    assert.equal(rules[0].tool, 'bash');
    assert.equal(rules[0].pattern, 'rm *');
    assert.equal(rules[0].source, 'policySettings');
    // Pre-existing cliArg rule retained (no lockdown).
    assert.ok(rules.some(r => r.pattern === 'ls *'));
  });
});

test('lockdown clears non-managed rules and skips all other tiers', () => {
  withManagedDir({
    allowManagedPermissionRulesOnly: true,
    rules: [{ tool: 'bash', pattern: 'git *', action: 'allow' }],
  }, (dir) => {
    // A project-tier rule that MUST be ignored under lockdown.
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.ami', 'permissions.json'),
      JSON.stringify({ rules: [{ tool: 'bash', pattern: 'curl *', action: 'allow' }] }),
      'utf-8',
    );
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'cliArg' },
    ]);
    const warnings = pm.loadTieredRules(dir);
    assert.deepEqual(warnings, []);
    assert.equal(pm.isManagedRulesOnly(), true);
    const rules = pm.getRules();
    // Only the managed rule survives; cliArg + projectSettings are gone.
    assert.equal(rules.length, 1);
    assert.equal(rules[0].pattern, 'git *');
    assert.equal(rules[0].source, 'policySettings');
  });
});

test('under lockdown addRule refuses non-managed grants but accepts policy rules', () => {
  withManagedDir({ allowManagedPermissionRulesOnly: true, rules: [] }, (dir) => {
    const pm = new PermissionManager();
    pm.loadTieredRules(dir);
    assert.equal(pm.isManagedRulesOnly(), true);
    pm.addRule({ tool: 'bash', pattern: 'npm *', action: 'allow', source: 'session' });
    assert.equal(pm.getRules().length, 0, 'session grant must be refused under lockdown');
    pm.addRule({ tool: 'bash', pattern: 'yarn *', action: 'allow', source: 'policySettings' });
    assert.equal(pm.getRules().length, 1, 'policy rule is allowed');
  });
});

test('under lockdown saveRules and saveRulesToDestination refuse to persist', () => {
  withManagedDir({ allowManagedPermissionRulesOnly: true, rules: [] }, (dir) => {
    const pm = new PermissionManager();
    pm.loadTieredRules(dir);
    const r1 = pm.saveRules(dir);
    assert.equal(r1.ok, false);
    assert.match(r1.error ?? '', /lockdown/);
    const r2 = pm.saveRulesToDestination(dir, 'projectSettings');
    assert.equal(r2.ok, false);
    assert.match(r2.error ?? '', /lockdown/);
    assert.ok(!fs.existsSync(path.join(dir, '.ami', 'permissions.json')));
  });
});

test('shouldShowAlwaysAllowOptions is hidden only under lockdown', () => {
  withManagedDir({ allowManagedPermissionRulesOnly: true, rules: [] }, (dir) => {
    const locked = new PermissionManager();
    locked.loadTieredRules(dir);
    assert.equal(locked.shouldShowAlwaysAllowOptions(), false);
  });
  const open = new PermissionManager();
  assert.equal(open.shouldShowAlwaysAllowOptions(), true);
});

test('a broken managed permissions file is surfaced as a warning, not a crash', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-managed-'));
  const prev = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(dir, 'config.json');
  try {
    fs.writeFileSync(managedPermissionsPath(), '{ not json', 'utf-8');
    const pm = new PermissionManager();
    const warnings = pm.loadManagedPermissions();
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /could not be parsed/);
    assert.equal(pm.isManagedRulesOnly(), false);
  } finally {
    if (prev === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a malformed rule inside the managed file is skipped with a policySettings warning', () => {
  withManagedDir({ rules: [{ tool: 'bash', action: 'nonsense' }] }, () => {
    const pm = new PermissionManager();
    const warnings = pm.loadManagedPermissions();
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /^policySettings:/);
    assert.equal(pm.getRules().length, 0);
  });
});

// ============ Integration: managed deny is honored end-to-end ================

test('E2E: a managed deny rule wins over auto-allow mode through check()', async () => {
  withManagedDir({ rules: [{ tool: 'bash', pattern: 'rm -rf *', action: 'deny' }] }, (dir) => {
    // auto-allow mode normally allows everything except explicit denies.
    const pm = new PermissionManager('auto-allow');
    pm.loadTieredRules(dir);
    return (async () => {
      assert.equal(await pm.check('bash', { command: 'rm -rf /' }), 'deny');
      assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
    })();
  });
});

test('E2E: a managed allow rule wins first-match over a conflicting project ask rule', async () => {
  await new Promise<void>((resolve, reject) => {
    withManagedDir({ rules: [{ tool: 'bash', pattern: 'deploy *', action: 'allow' }] }, (dir) => {
      fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
      fs.writeFileSync(
        path.join(dir, '.ami', 'permissions.json'),
        JSON.stringify({ rules: [{ tool: 'bash', pattern: 'deploy *', action: 'ask' }] }),
        'utf-8',
      );
      const pm = new PermissionManager('ask');
      pm.loadTieredRules(dir);
      pm.check('bash', { command: 'deploy prod' })
        .then((d) => { assert.equal(d, 'allow'); resolve(); })
        .catch(reject);
    });
  });
});

// Keep a reference to the type import so it is not flagged unused.
const _typeProbe: PermissionRule = { tool: '*', action: 'allow', source: 'policySettings' };
assert.equal(_typeProbe.source, 'policySettings');
