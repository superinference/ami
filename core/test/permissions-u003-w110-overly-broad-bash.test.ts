import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w110 — G(4,5): detect + strip OVERLY-BROAD bash `allow` rules.
//
// A pattern-less bash allow (`{tool:'bash', action:'allow'}`), a `Bash(*)`
// allow, or an all-tools (`*`) allow auto-approves EVERY bash command — the
// YOLO / bypass-permissions equivalent — because check() matches a pattern-less
// rule against every command (`rule.pattern ? … : true`). A committed/untrusted
// project settings file must never silently grant the whole team that breadth.
//
// Parity: the reference's isOverlyBroadBashAllowRule / findOverlyBroadBashPermissions
// (utils/permissions/permissionSetup.ts) + removeDangerousPermissions re-run on
// every settings sync (utils/settings/applySettingsChange.ts L50-59).
// ---------------------------------------------------------------------------

function withTempProject(rules: unknown[], fn: (cwd: string) => void): void {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w110-'));
  try {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'), JSON.stringify({ rules }));
    fn(dir);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('G4: findOverlyBroadBashAllowRules flags pattern-less / `*` / all-tools bash allows only', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'allow' },                  // pattern-less → broad
    { tool: 'bash', pattern: '*', action: 'allow' },    // `*` → broad
    { tool: '*', action: 'allow' },                     // all tools → broad (incl. bash)
    { tool: 'bash', pattern: 'git status', action: 'allow' }, // narrow → NOT broad
    { tool: 'bash', action: 'deny' },                   // deny → NOT broad
    { tool: 'file_edit', action: 'allow' },             // non-bash tool → NOT broad
    { tool: 'bash', paramPattern: { command: 'ls*' }, action: 'allow' }, // param-scoped → NOT broad
  ]);
  const flagged = pm.findOverlyBroadBashAllowRules();
  assert.equal(flagged.length, 3);
  // Narrow / deny / non-bash / param-scoped rules are not flagged.
  assert.ok(!flagged.some(r => r.pattern === 'git status'));
  assert.ok(!flagged.some(r => r.action === 'deny'));
  assert.ok(!flagged.some(r => r.tool === 'file_edit'));
  assert.ok(!flagged.some(r => r.paramPattern !== undefined));
});

test('G5: loadRules strips a project-sourced overly-broad bash allow and records it', () => {
  withTempProject(
    [
      { tool: 'bash', action: 'allow' },                        // pattern-less → stripped
      { tool: 'bash', pattern: 'npm run build', action: 'allow' }, // narrow → kept
    ],
    (cwd) => {
      const pm = new PermissionManager('ask');
      const warnings = pm.loadRules(cwd);
      const patterns = pm.getRules().map(r => r.pattern);
      assert.ok(patterns.includes('npm run build'), 'narrow rule kept');
      // The pattern-less (broad) rule must be gone.
      assert.ok(!pm.getRules().some(r => r.tool === 'bash' && r.pattern === undefined),
        'overly-broad pattern-less bash allow must be stripped');
      const stripped = pm.getStrippedOverlyBroadBashRules();
      assert.equal(stripped.length, 1);
      assert.equal(stripped[0]!.source, 'projectSettings');
      assert.ok(warnings.some(w => /overly-broad bash allow/i.test(w)),
        'a warning must report the stripped rule');
    },
  );
});

test('G5: a policy (managed) overly-broad bash allow is trusted and NOT stripped', () => {
  withTempProject(
    [{ tool: 'bash', action: 'allow', source: 'policySettings' }],
    (cwd) => {
      const pm = new PermissionManager('ask');
      pm.loadRules(cwd);
      assert.ok(pm.getRules().some(r => r.tool === 'bash' && r.pattern === undefined),
        'enterprise-managed broad rule is trusted and kept');
      assert.equal(pm.getStrippedOverlyBroadBashRules().length, 0);
    },
  );
});

test('G5: a PERSONAL/untagged overly-broad bash allow (constructor) is NOT stripped', () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', action: 'allow' }]);
  // The user's own explicit grant survives — only shared untrusted tiers are screened.
  assert.equal(pm.findOverlyBroadBashAllowRules().length, 1);
  assert.equal(pm.getStrippedOverlyBroadBashRules().length, 0);
});

test('G5: syncRulesFromDisk strips an overly-broad bash allow from a tiered project file', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w110-sync-'));
  try {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    // projectSettings tier is .ami/permissions.json (committed). Write a blanket bash allow.
    fs.writeFileSync(
      path.join(dir, '.ami', 'permissions.json'),
      JSON.stringify({ rules: [{ tool: 'bash', pattern: '*', action: 'allow' }] }),
    );
    const pm = new PermissionManager('ask');
    const warnings = pm.syncRulesFromDisk(dir);
    assert.ok(!pm.getRules().some(r => r.tool === 'bash' && (r.pattern === '*' || r.pattern === undefined)),
      'blanket bash allow must be stripped on settings re-sync');
    assert.ok(pm.getStrippedOverlyBroadBashRules().length >= 1);
    assert.ok(warnings.some(w => /overly-broad bash allow/i.test(w)));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// RED-GREEN security reproducer: WITHOUT the strip, a committed project
// pattern-less bash allow auto-approves an arbitrary (non-safe) bash command in
// `ask` mode; WITH the strip it falls through to a prompt. Reverting the strip
// wiring flips this assertion (check() returns 'allow').
test('G4/G5 E2E: a project blanket bash allow does NOT auto-approve an arbitrary command', async () => {
  await new Promise<void>((resolve, reject) => {
    withTempProject([{ tool: 'bash', action: 'allow' }], async (cwd) => {
      try {
        const pm = new PermissionManager('ask');
        pm.loadRules(cwd);
        // `make build` is not a SAFE_COMMAND; with the broad allow stripped it must
        // require a prompt rather than being silently auto-allowed.
        const verdict = await pm.check('bash', { command: 'make build' });
        assert.equal(verdict, 'ask',
          'stripped blanket allow must leave an arbitrary command needing approval');
        resolve();
      } catch (e) {
        reject(e as Error);
      }
    });
  });
});
