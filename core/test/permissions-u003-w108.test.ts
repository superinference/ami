import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  PermissionManager,
  isPermissivePermissionMode,
  dynamicAllowPatternToken,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w108 — three additive permission-layer parity improvements:
//   G6  — strip dangerous code-exec `allow` rules on escalation INTO a permissive
//         mode (parity: the reference's transitionPermissionMode cleanup).
//   G13/G14/G15 — source-tracked additional read roots + persistence to
//         `permissions.additionalDirectories` (parity: the reference's
//         additionalWorkingDirectories Map + persistPermissionUpdate dedupe).
//   G20 — reject a shell-DYNAMIC token in a persistent bash `allow` pattern
//         (parity: the reference's staticPrefix.ts dynamic-arg sanitization).
// ---------------------------------------------------------------------------

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w108-'));
}

// --- G6: isPermissivePermissionMode predicate --------------------------------

test('isPermissivePermissionMode flags acceptEdits/auto-allow/bypass only', () => {
  assert.equal(isPermissivePermissionMode('acceptEdits'), true);
  assert.equal(isPermissivePermissionMode('auto-allow'), true);
  assert.equal(isPermissivePermissionMode('bypassPermissions'), true);
  // Neutral/screened/restrictive modes are NOT permissive.
  assert.equal(isPermissivePermissionMode('ask'), false);
  assert.equal(isPermissivePermissionMode('auto'), false);
  assert.equal(isPermissivePermissionMode('deny-all'), false);
  assert.equal(isPermissivePermissionMode('plan'), false);
  assert.equal(isPermissivePermissionMode(undefined), false);
});

// --- G6: escalation strips a dangerous code-exec allow rule ------------------

test('escalating into acceptEdits strips a dangerous code-exec allow rule', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'python:*', action: 'allow' });
  pm.addRule({ tool: 'bash', pattern: 'git status', action: 'allow' });
  assert.equal(pm.getRules().length, 2);

  const changed = pm.setMode('acceptEdits');
  assert.equal(changed, true);

  const remaining = pm.getRules();
  // The interpreter grant is gone; the benign `git status` grant survives.
  assert.ok(!remaining.some(r => r.pattern === 'python:*'),
    'dangerous python:* allow should be stripped on escalation');
  assert.ok(remaining.some(r => r.pattern === 'git status'),
    'benign allow rule must be preserved');
  // Stripped rule is recorded for introspection.
  assert.ok(pm.getStrippedDangerousRules().some(r => r.pattern === 'python:*'));
});

test('a steady-state (non-escalating) mode change leaves rules untouched', () => {
  // Start already in a permissive mode: entering it at init must NOT strip, and a
  // transition to ANOTHER non-escalating mode (acceptEdits → ask) must not strip.
  const pm = new PermissionManager('acceptEdits');
  pm.addRule({ tool: 'bash', pattern: 'node:*', action: 'allow' });
  // init did not strip (reason==='init' is exempt).
  assert.ok(pm.getRules().some(r => r.pattern === 'node:*'));
  // De-escalating to ask keeps the rule (cleanup only fires on escalation IN).
  pm.setMode('ask');
  assert.ok(pm.getRules().some(r => r.pattern === 'node:*'),
    'de-escalation must not strip rules');
});

test('escalation cleanup fires only on the first crossing into a permissive tier', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'python:*', action: 'allow' });
  pm.setMode('acceptEdits'); // strips python:*
  assert.ok(!pm.getRules().some(r => r.pattern === 'python:*'));
  // Add another dangerous rule while permissive, then cross to a DIFFERENT
  // permissive tier (acceptEdits → bypassPermissions) — still an escalation
  // between two permissive modes; the predicate requires the PREVIOUS mode to be
  // non-permissive, so this crossing does NOT re-strip.
  pm.addRule({ tool: 'bash', pattern: 'node:*', action: 'allow' });
  pm.setMode('bypassPermissions');
  assert.ok(pm.getRules().some(r => r.pattern === 'node:*'),
    'permissive→permissive crossing is not a fresh escalation');
});

// --- G20: dynamicAllowPatternToken classifier --------------------------------

test('dynamicAllowPatternToken detects substitution / variable / backslash', () => {
  assert.equal(dynamicAllowPatternToken('deploy $(cat target)'), 'substitution');
  assert.equal(dynamicAllowPatternToken('echo `whoami`'), 'substitution');
  assert.equal(dynamicAllowPatternToken('echo ${HOME}'), 'variable');
  assert.equal(dynamicAllowPatternToken('echo $HOME'), 'variable');
  assert.equal(dynamicAllowPatternToken('run $@'), 'variable');
  assert.equal(dynamicAllowPatternToken('type C:\\\\tool'), 'backslash');
  // Safe static prefixes / globs are tolerated.
  assert.equal(dynamicAllowPatternToken('git *'), null);
  assert.equal(dynamicAllowPatternToken('git status'), null);
  assert.equal(dynamicAllowPatternToken('ls -la ?'), null);
  assert.equal(dynamicAllowPatternToken('cat file[0-9]'), null);
  assert.equal(dynamicAllowPatternToken('price is 5$'), null); // trailing bare $
  assert.equal(dynamicAllowPatternToken(undefined), null);
});

// --- G20: addRule refuses to persist a dynamic bash allow rule ---------------

test('addRule rejects a bash allow rule with a dynamic token but keeps deny/ask', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'deploy $(cat prod)', action: 'allow' });
  assert.equal(pm.getRules().length, 0, 'dynamic allow must not be persisted');

  // A deny rule with the same dynamic token IS allowed — it only widens what is
  // blocked, which is not an escalation hazard.
  pm.addRule({ tool: 'bash', pattern: 'rm $(cat list)', action: 'deny' });
  assert.equal(pm.getRules().length, 1);
  assert.equal(pm.getRules()[0]!.action, 'deny');

  // A static bash allow rule is unaffected.
  pm.addRule({ tool: 'bash', pattern: 'git push', action: 'allow' });
  assert.ok(pm.getRules().some(r => r.pattern === 'git push' && r.action === 'allow'));
});

// --- G13/G14/G15: source-tracked roots -------------------------------------

test('registerAllowedRoot tracks the granting source; session vs project', () => {
  const pm = new PermissionManager('ask');
  const a = path.join(os.tmpdir(), 'ami-root-a');
  const b = path.join(os.tmpdir(), 'ami-root-b');
  pm.registerAllowedRoot(a); // defaults to session
  pm.registerAllowedRoot(b, 'projectSettings');
  const entries = pm.getAllowedReadRootEntries();
  const byPath = new Map(entries.map(e => [e.path, e.source]));
  assert.equal(byPath.get(path.normalize(a)), 'session');
  assert.equal(byPath.get(path.normalize(b)), 'projectSettings');
  // getAllowedReadRoots keeps its string[] shape for existing callers.
  assert.deepEqual(new Set(pm.getAllowedReadRoots()), new Set([path.normalize(a), path.normalize(b)]));
});

test('a re-grant upgrades a session root to a persistable source', () => {
  const pm = new PermissionManager('ask');
  const a = path.join(os.tmpdir(), 'ami-root-upgrade');
  pm.registerAllowedRoot(a);
  assert.equal(pm.getAllowedReadRootEntries()[0]!.source, 'session');
  pm.registerAllowedRoot(a, 'projectSettings');
  assert.equal(pm.getAllowedReadRootEntries().length, 1, 'still de-duped by path');
  assert.equal(pm.getAllowedReadRootEntries()[0]!.source, 'projectSettings');
});

// --- G13/G15: persistence (only persistable tiers written) ------------------

test('saveRules persists only non-session roots to additionalDirectories', () => {
  const cwd = mkTmp();
  try {
    const pm = new PermissionManager('ask');
    const sess = path.join(cwd, 'session-dir');
    const proj = path.join(cwd, 'project-dir');
    pm.registerAllowedRoot(sess); // session — in-memory only
    pm.registerAllowedRoot(proj, 'projectSettings'); // persisted
    const res = pm.saveRules(cwd);
    assert.equal(res.ok, true);

    const raw = JSON.parse(fs.readFileSync(path.join(cwd, '.ami', 'permissions.json'), 'utf-8'));
    assert.deepEqual(raw.additionalDirectories, [path.normalize(proj)]);
    assert.ok(!raw.additionalDirectories.includes(path.normalize(sess)),
      'ephemeral session grant must not leak into the committed file');
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
  }
});

test('loadRules reloads additionalDirectories as real, source-tagged read grants', () => {
  const cwd = mkTmp();
  const granted = mkTmp(); // an out-of-workspace dir
  try {
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(cwd, '.ami', 'permissions.json'),
      JSON.stringify({ mode: 'ask', rules: [], additionalDirectories: [granted, '', 42] }),
    );
    const pm = new PermissionManager();
    const warnings = pm.loadRules(cwd);

    // The valid dir is granted + tagged projectSettings; invalid entries warn.
    const entry = pm.getAllowedReadRootEntries().find(e => e.path === path.normalize(granted));
    assert.ok(entry, 'valid additionalDirectories entry should be granted');
    assert.equal(entry!.source, 'projectSettings');
    assert.ok(warnings.some(w => /additionalDirectories/.test(w)),
      'invalid entries must produce a warning');

    // FUNCTIONAL: a file under the granted dir now reads as allowed from a
    // DIFFERENT cwd — proving the persisted grant actually widened read access.
    assert.equal(pm.classifyReadAccess(path.join(granted, 'notes.txt'), cwd), 'allow');
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(granted, { recursive: true, force: true });
  }
});

test('additionalDirectories round-trips and respects removal', () => {
  const cwd = mkTmp();
  try {
    const d1 = path.join(cwd, 'd1');
    const d2 = path.join(cwd, 'd2');
    const pm1 = new PermissionManager('ask');
    pm1.registerAllowedRoot(d1, 'projectSettings');
    pm1.registerAllowedRoot(d2, 'projectSettings');
    pm1.saveRules(cwd);

    // Fresh manager reloads both.
    const pm2 = new PermissionManager();
    pm2.loadRules(cwd);
    assert.deepEqual(new Set(pm2.getAllowedReadRoots()), new Set([path.normalize(d1), path.normalize(d2)]));

    // Remove one, re-save — the removed dir is filtered out on disk.
    assert.equal(pm2.removeAllowedRoot(d1), true);
    pm2.saveRules(cwd);
    const raw = JSON.parse(fs.readFileSync(path.join(cwd, '.ami', 'permissions.json'), 'utf-8'));
    assert.deepEqual(raw.additionalDirectories, [path.normalize(d2)]);
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
  }
});
