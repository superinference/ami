import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  createPermissionRetryMessage,
  type PermissionUpdate,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U003 — wave w66 parity gaps in core/core/src/permissions.ts
//
// Structured permission updates (PermissionUpdate discriminated union +
// applyUpdate/applyUpdates), grant-and-retry recovery for denied commands,
// and the model-facing retry message. Each block cites the Claude reference
// whose behavior AMI is matching.
// ===========================================================================

// ---------------------------------------------------------------------------
// PermissionUpdate: addRules
//   claudeRef utils/permissions/PermissionUpdate.ts:L55-95 (applyPermissionUpdate)
//            utils/permissions/PermissionUpdateSchema.ts (discriminated union)
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdate addRules', () => {
  it('adds allow rules in suggestion order and tags the destination as source', () => {
    const pm = new PermissionManager('ask');
    const update: PermissionUpdate = {
      type: 'addRules',
      behavior: 'allow',
      destination: 'session',
      rules: [{ toolName: 'bash', ruleContent: 'git status*' }, { toolName: 'bash', ruleContent: 'npm test*' }],
    };
    const { applied } = pm.applyUpdate(update);
    assert.equal(applied, 2);
    const rules = pm.getRules();
    // Suggestion order preserved (first rule is first in the list).
    assert.equal(rules[0]!.pattern, 'git status*');
    assert.equal(rules[1]!.pattern, 'npm test*');
    assert.equal(rules[0]!.action, 'allow');
    assert.equal(rules[0]!.source, 'session');
  });

  it('canonicalizes a Claude-Code tool name (Bash → bash)', () => {
    const pm = new PermissionManager('ask');
    pm.applyUpdate({ type: 'addRules', behavior: 'deny', rules: [{ toolName: 'Bash', ruleContent: 'rm *' }] });
    assert.equal(pm.getRules()[0]!.tool, 'bash');
    assert.equal(pm.getRules()[0]!.action, 'deny');
  });

  it('honors the managed-rules-only lockdown (a session addRules is refused)', () => {
    const pm = new PermissionManager('ask');
    // Force the lockdown by loading a managed-only policy via the private flag path:
    // a replaceRules with a non-managed source must be rejected by addRule.
    (pm as unknown as { _managedRulesOnly: boolean })._managedRulesOnly = true;
    const { applied } = pm.applyUpdate({ type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'ls*' }] });
    assert.equal(applied, 0);
    assert.equal(pm.getRules().length, 0);
  });
});

// ---------------------------------------------------------------------------
// PermissionUpdate: removeRules (normalized parse→serialize match)
//   claudeRef utils/permissions/PermissionUpdate.ts (removeRules + normalization)
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdate removeRules normalized', () => {
  it('removes a stored tool-wide rule via a Bash(*) suggestion (normalization)', () => {
    const pm = new PermissionManager('ask');
    // Stored as a tool-wide bash allow (no pattern).
    pm.addRule({ tool: 'bash', action: 'allow' });
    assert.equal(pm.getRules().length, 1);
    // Suggestion written as Bash(*) → normalizes to the same tool-wide rule.
    const { applied } = pm.applyUpdate({ type: 'removeRules', behavior: 'allow', rules: [{ toolName: 'Bash', ruleContent: '*' }] });
    assert.equal(applied, 1);
    assert.equal(pm.getRules().length, 0);
  });

  it('only removes rules of the matching behavior', () => {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'git push*', action: 'deny' });
    pm.addRule({ tool: 'bash', pattern: 'git push*', action: 'allow' });
    const { applied } = pm.applyUpdate({ type: 'removeRules', behavior: 'allow', rules: [{ toolName: 'bash', ruleContent: 'git push*' }] });
    assert.equal(applied, 1);
    const remaining = pm.getRules();
    assert.equal(remaining.length, 1);
    assert.equal(remaining[0]!.action, 'deny');
  });
});

// ---------------------------------------------------------------------------
// PermissionUpdate: replaceRules (atomic swap of a behavior+destination set)
//   claudeRef utils/permissions/PermissionUpdate.ts:L98-120
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdate replaceRules', () => {
  it('atomically replaces all allow rules of a destination', () => {
    const pm = new PermissionManager('ask');
    pm.applyUpdate({ type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'old1*' }, { toolName: 'bash', ruleContent: 'old2*' }] });
    pm.applyUpdate({ type: 'replaceRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'new*' }] });
    const rules = pm.getRules().filter(r => r.action === 'allow' && r.source === 'session');
    assert.equal(rules.length, 1);
    assert.equal(rules[0]!.pattern, 'new*');
  });

  it('does not touch rules of a different behavior or destination', () => {
    const pm = new PermissionManager('ask');
    pm.applyUpdate({ type: 'addRules', behavior: 'deny', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'keep-deny*' }] });
    pm.applyUpdate({ type: 'addRules', behavior: 'allow', destination: 'userSettings', rules: [{ toolName: 'bash', ruleContent: 'keep-user*' }] });
    pm.applyUpdate({ type: 'replaceRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'new*' }] });
    const patterns = pm.getRules().map(r => r.pattern);
    assert.ok(patterns.includes('keep-deny*'));
    assert.ok(patterns.includes('keep-user*'));
    assert.ok(patterns.includes('new*'));
  });
});

// ---------------------------------------------------------------------------
// PermissionUpdate: addDirectories / removeDirectories (read-access boundary)
//   claudeRef utils/permissions/PermissionUpdate.ts (addDirectories/removeDirectories)
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdate directories', () => {
  it('adds and removes workspace read roots', () => {
    const pm = new PermissionManager('ask');
    const add = pm.applyUpdate({ type: 'addDirectories', directories: ['/tmp/alpha', '/tmp/beta'] });
    assert.equal(add.applied, 2);
    assert.ok(pm.getAllowedReadRoots().some(r => r.includes('alpha')));
    const rm = pm.applyUpdate({ type: 'removeDirectories', directories: ['/tmp/alpha'] });
    assert.equal(rm.applied, 1);
    assert.ok(!pm.getAllowedReadRoots().some(r => r.includes('alpha')));
    assert.ok(pm.getAllowedReadRoots().some(r => r.includes('beta')));
  });
});

// ---------------------------------------------------------------------------
// PermissionUpdate: setMode
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdate setMode', () => {
  it('changes the mode', () => {
    const pm = new PermissionManager('ask');
    const { applied } = pm.applyUpdate({ type: 'setMode', mode: 'acceptEdits' });
    assert.equal(applied, 1);
    assert.equal(pm.getMode(), 'acceptEdits');
  });

  it('reports applied:0 when policy blocks the mode change', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true);
    const { applied } = pm.applyUpdate({ type: 'setMode', mode: 'bypassPermissions' });
    assert.equal(applied, 0);
    assert.notEqual(pm.getMode(), 'bypassPermissions');
  });
});

// ---------------------------------------------------------------------------
// applyUpdates: batch (one-click "accept all suggestions")
//   claudeRef utils/permissions/PermissionUpdate.ts:L196 (applyPermissionUpdates)
//            components/permissions/shellPermissionHelpers.tsx (batched suggestions)
// ---------------------------------------------------------------------------
describe('U003/w66: applyUpdates batch', () => {
  it('applies a batch in order and totals the affected count', () => {
    const pm = new PermissionManager('ask');
    const { applied } = pm.applyUpdates([
      { type: 'addRules', behavior: 'allow', rules: [{ toolName: 'bash', ruleContent: 'git status*' }] },
      { type: 'addDirectories', directories: ['/tmp/gamma'] },
      { type: 'setMode', mode: 'acceptEdits' },
    ]);
    assert.equal(applied, 3);
    assert.equal(pm.getMode(), 'acceptEdits');
    assert.ok(pm.getAllowedReadRoots().some(r => r.includes('gamma')));
    assert.equal(pm.getRules()[0]!.pattern, 'git status*');
  });
});

// ---------------------------------------------------------------------------
// createPermissionRetryMessage
//   claudeRef utils/messages.ts:L4354-4367
// ---------------------------------------------------------------------------
describe('U003/w66: createPermissionRetryMessage', () => {
  it('joins commands into an "Allowed …" notice', () => {
    assert.equal(createPermissionRetryMessage(['git push', 'npm run build']), 'Allowed git push, npm run build');
  });
  it('returns a valid notice for an empty list', () => {
    assert.equal(createPermissionRetryMessage([]), 'No denied commands to retry');
  });
});

// ---------------------------------------------------------------------------
// grantRetryForDenials: grant-and-retry recovery
//   claudeRef commands/permissions/permissions.tsx:L5-9 (onRetryDenials)
//            components/permissions/rules/PermissionRuleList.tsx (grant+retry)
// ---------------------------------------------------------------------------
describe('U003/w66: grantRetryForDenials', () => {
  it('grants scoped allow rules for all denied commands and clears them', () => {
    const pm = new PermissionManager('ask');
    pm.recordDenial('git status --short', 'rule');
    pm.recordDenial('npm run build', 'mode');
    const result = pm.grantRetryForDenials();
    assert.deepEqual([...result.commands].sort(), ['git status --short', 'npm run build'].sort());
    assert.equal(result.message, `Allowed ${result.commands.join(', ')}`);
    // Scoped allow rules were seeded (e.g. `git status*`, `npm run*`).
    const patterns = pm.getRules().filter(r => r.action === 'allow').map(r => r.pattern);
    assert.ok(patterns.includes('git status*'));
    assert.ok(patterns.includes('npm run*'));
    // The retried denials are dropped from the review log.
    assert.equal(pm.getRecentDenials().length, 0);
  });

  it('only retries the selected subset, leaving others in the log', () => {
    const pm = new PermissionManager('ask');
    pm.recordDenial('git status', 'rule');
    pm.recordDenial('rm -rf /', 'hardline');
    const result = pm.grantRetryForDenials(['git status']);
    assert.deepEqual(result.commands, ['git status']);
    const remaining = pm.getRecentDenials().map(d => d.display);
    assert.deepEqual(remaining, ['rm -rf /']);
  });

  it('retries after approve even when no safe scoped rule can be derived', () => {
    const pm = new PermissionManager('ask');
    // A bare subcommand-aware root yields no safe prefix (suggestAllowPrefix → null),
    // so no rule is seeded but the command is still acknowledged as retried.
    pm.recordDenial('git', 'rule');
    const result = pm.grantRetryForDenials();
    assert.deepEqual(result.commands, ['git']);
    assert.equal(pm.getRules().length, 0);
    assert.equal(pm.getRecentDenials().length, 0);
  });
});
