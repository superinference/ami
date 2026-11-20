import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  validatePermissionUpdate,
  PermissionManager,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003/w90 — schema-validated permission-update vocabulary at the trust boundary.
//
// Parity: the reference models every permission mutation as a strict Zod
// `discriminatedUnion` over `type` with an enum `behavior`, an enum `destination`,
// and typed rule/directory arrays, so a malformed update (unknown type, missing
// behavior, non-array rules, bad destination, bogus mode, non-string directory)
// is rejected WHOLE at the entry boundary before anything is applied
// (references/claude/claude-code-main/src/utils/permissions/PermissionUpdateSchema.ts
// L27-L77). AMI previously applied `PermissionUpdate`s straight through
// `PermissionManager.applyUpdate` with NO runtime validation, even though they
// arrive from untrusted surfaces (a delegated permission-prompt tool's JSON reply
// — core/src/permission-prompt-tool.ts only array-checks `updatedPermissions` — and
// a hook process's `updatedPermissions`). This wires `validatePermissionUpdate` as
// a single acceptance boundary inside `applyUpdate`, dropping bad updates
// fail-closed (grant nothing) instead of partially applying them or crashing.
// ---------------------------------------------------------------------------

describe('validatePermissionUpdate — well-formed updates', () => {
  const valid: PermissionUpdate[] = [
    { type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'npm test' }] },
    { type: 'addRules', behavior: 'deny', rules: [{ toolName: 'bash' }] }, // no ruleContent, no destination
    { type: 'replaceRules', behavior: 'ask', destination: 'projectSettings', rules: [] },
    { type: 'removeRules', behavior: 'allow', destination: 'localSettings', rules: [{ toolName: 'web_fetch', ruleContent: 'example.com' }] },
    { type: 'setMode', mode: 'acceptEdits' },
    { type: 'setMode', mode: 'plan', destination: 'session' },
    { type: 'addDirectories', directories: ['/tmp/a', '/tmp/b'] },
    { type: 'removeDirectories', directories: ['/tmp/a'], destination: 'userSettings' },
  ];
  for (const u of valid) {
    it(`accepts ${u.type} (${JSON.stringify(u).slice(0, 48)}…)`, () => {
      assert.deepEqual(validatePermissionUpdate(u), { valid: true });
    });
  }
});

describe('validatePermissionUpdate — rejects malformed updates', () => {
  const cases: Array<{ name: string; value: unknown; errIncludes: string }> = [
    { name: 'null', value: null, errIncludes: 'must be an object' },
    { name: 'array', value: [{ type: 'setMode', mode: 'ask' }], errIncludes: 'must be an object' },
    { name: 'string', value: 'addRules', errIncludes: 'must be an object' },
    { name: 'unknown type', value: { type: 'grantEverything' }, errIncludes: 'unknown permission update type' },
    { name: 'missing type', value: { behavior: 'allow', rules: [] }, errIncludes: 'unknown permission update type' },
    { name: 'addRules bad behavior', value: { type: 'addRules', behavior: 'yes', rules: [] }, errIncludes: 'behavior' },
    { name: 'addRules missing behavior', value: { type: 'addRules', rules: [] }, errIncludes: 'behavior' },
    { name: 'addRules non-array rules', value: { type: 'addRules', behavior: 'allow', rules: 'bash' }, errIncludes: 'rules must be an array' },
    { name: 'addRules rule not object', value: { type: 'addRules', behavior: 'allow', rules: ['bash'] }, errIncludes: 'must be objects' },
    { name: 'addRules empty toolName', value: { type: 'addRules', behavior: 'allow', rules: [{ toolName: '' }] }, errIncludes: 'non-empty string toolName' },
    { name: 'addRules missing toolName', value: { type: 'addRules', behavior: 'allow', rules: [{ ruleContent: 'x' }] }, errIncludes: 'non-empty string toolName' },
    { name: 'addRules non-string ruleContent', value: { type: 'addRules', behavior: 'allow', rules: [{ toolName: 'bash', ruleContent: 42 }] }, errIncludes: 'ruleContent must be a string' },
    { name: 'bad destination', value: { type: 'addRules', behavior: 'allow', destination: 'policySettings', rules: [] }, errIncludes: 'invalid destination' },
    { name: 'destination non-string', value: { type: 'setMode', mode: 'ask', destination: 7 }, errIncludes: 'invalid destination' },
    { name: 'setMode bogus mode', value: { type: 'setMode', mode: 'totally-bogus' }, errIncludes: 'valid mode' },
    { name: 'setMode non-string mode', value: { type: 'setMode', mode: null }, errIncludes: 'valid mode' },
    { name: 'addDirectories non-array', value: { type: 'addDirectories', directories: '/tmp' }, errIncludes: 'must be an array' },
    { name: 'addDirectories empty string', value: { type: 'addDirectories', directories: [''] }, errIncludes: 'non-empty strings' },
    { name: 'addDirectories non-string', value: { type: 'removeDirectories', directories: [123] }, errIncludes: 'non-empty strings' },
  ];
  for (const c of cases) {
    it(`rejects ${c.name}`, () => {
      const res = validatePermissionUpdate(c.value);
      assert.equal(res.valid, false, `expected ${c.name} to be invalid`);
      assert.ok(
        res.error && res.error.includes(c.errIncludes),
        `expected error for ${c.name} to include "${c.errIncludes}", got "${res.error}"`,
      );
    });
  }

  it('rejects policySettings/flagSettings/command as write destinations but accepts all writable tiers', () => {
    for (const d of ['userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg']) {
      assert.equal(validatePermissionUpdate({ type: 'setMode', mode: 'ask', destination: d }).valid, true, d);
    }
    for (const d of ['policySettings', 'flagSettings', 'command']) {
      assert.equal(validatePermissionUpdate({ type: 'setMode', mode: 'ask', destination: d }).valid, false, d);
    }
  });
});

describe('PermissionManager.applyUpdate — enforces validation at the boundary', () => {
  it('applies a well-formed addRules update (regression lock on the happy path)', async () => {
    const pm = new PermissionManager('ask');
    const before = pm.getRules().length;
    const { applied } = pm.applyUpdate({
      type: 'addRules', behavior: 'allow', destination: 'session',
      rules: [{ toolName: 'bash', ruleContent: 'npm test' }],
    });
    assert.equal(applied, 1);
    assert.equal(pm.getRules().length, before + 1);
    // And the rule actually takes effect.
    assert.equal(await pm.check('bash', { command: 'npm test' }), 'allow');
  });

  it('drops a malformed update fail-closed: no mutation, no throw', () => {
    const pm = new PermissionManager('ask');
    const before = pm.getRules().length;
    // An addRules whose only "rule" has a non-string ruleContent must not be
    // installed — otherwise a garbage pattern would enter the live rule set.
    const bad = { type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: { evil: true } }] } as unknown as PermissionUpdate;
    const { applied } = pm.applyUpdate(bad);
    assert.equal(applied, 0);
    assert.equal(pm.getRules().length, before, 'no rule may be added from a malformed update');
  });

  it('a malformed setMode does NOT change the mode', () => {
    const pm = new PermissionManager('ask');
    const bogus = { type: 'setMode', mode: 'bypass-everything' } as unknown as PermissionUpdate;
    const { applied } = pm.applyUpdate(bogus);
    assert.equal(applied, 0);
    assert.equal(pm.getMode(), 'ask', 'mode must stay unchanged after a rejected setMode');
  });

  it('RED-GREEN: an unknown update type is dropped instead of crashing applyUpdates', async () => {
    const pm = new PermissionManager('ask');
    // Reproduces the pre-fix defect: an unknown `type` fell through the switch,
    // returned `undefined`, and `applyUpdates` dereferenced `.applied` → TypeError.
    // With the boundary guard, the batch completes and the valid update still
    // applies while the malformed one contributes 0.
    const updates = [
      { type: 'wreck-havoc', rules: [] } as unknown as PermissionUpdate,
      { type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'ls' }] } as PermissionUpdate,
    ];
    let result!: { applied: number };
    assert.doesNotThrow(() => { result = pm.applyUpdates(updates); });
    assert.equal(result.applied, 1, 'only the valid update is applied');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });

  it('simulates the untrusted permission-prompt-tool / hook boundary: a bad update in the batch is skipped, good ones apply', async () => {
    const pm = new PermissionManager('ask');
    // Mirrors common/tool-executor applying `decision.updatedPermissions` /
    // `hookResult.updatedPermissions` one-by-one through applyUpdate.
    const fromUntrustedReply = [
      { type: 'addRules', behavior: 'allow', destination: 'session', rules: [{ toolName: 'bash', ruleContent: 'git status' }] },
      { type: 'addDirectories', directories: [null] }, // malformed — must not widen the boundary
      { type: 'addRules', behavior: 'deny', destination: 'session', rules: [{ toolName: 'web_fetch', ruleContent: 'evil.test' }] },
    ] as unknown as PermissionUpdate[];
    let total = 0;
    for (const u of fromUntrustedReply) total += pm.applyUpdate(u).applied;
    assert.equal(total, 2, 'both valid rules apply; the malformed directory grant is skipped');
    assert.equal(pm.getAllowedReadRoots().some(r => r.includes('null')), false);
    assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  });
});
