import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions.ts';

// ===========================================================================
// UNIT U015 — compound cwd-desync TOCTOU guard wired into the permission
// decision (detectCdSensitiveDesync → bashToctouEscalation).
//
// A compound that changes directory into a credential/sensitive-system dir and
// then reads/writes a RELATIVE operand must require explicit approval even under
// a wildcard/tool-wide allow rule: the auto-allow path only sees the literal
// `id_rsa`/`shadow` token (which matches no credential/system rule), while bash
// resolves it against the just-entered secret directory at runtime. Parity with
// Claude's compoundCommandHasCd cwd-desync guard.
//
// RED/GREEN discriminator: WITHOUT the guard these auto-allow under the tool-wide
// rule; the guard forces `ask`. Non-desync commands under the same rule still
// auto-allow, so the test fails if the guard over-reaches.
// ===========================================================================

function toolWideAllow(mode: 'ask' | 'acceptEdits' = 'ask'): PermissionManager {
  return new PermissionManager(mode, [{ tool: 'bash', action: 'allow' }]);
}

describe('U015 compound cwd-desync permission guard', () => {
  it('forces ask for cd-into-credential-dir + relative read under a tool-wide allow rule', async () => {
    const pm = toolWideAllow();
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh && cat id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh; cat id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cd .ssh && cat id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cd /etc && cat shadow' }), 'ask');
  });

  it('still auto-allows non-desync commands under the same rule (no over-reach)', async () => {
    const pm = toolWideAllow();
    assert.equal(await pm.check('bash', { command: 'cd /tmp && cat a' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'cd build && cat log.txt' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh && ls' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh && echo done' }), 'allow');
  });

  it('also forces ask in acceptEdits mode under a tool-wide allow rule', async () => {
    const pm = toolWideAllow('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh && cat id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cd /etc && cat shadow' }), 'ask');
  });

  it('lets an explicit deny rule still win over the cwd-desync ask', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'cd*', action: 'deny' }]);
    assert.equal(await pm.check('bash', { command: 'cd ~/.ssh && cat id_rsa' }), 'deny');
  });

  it('surfaces a structured toctou safetyCheck reason for the UI', async () => {
    const pm = toolWideAllow();
    const r = await pm.checkWithReason('bash', { command: 'cd ~/.ssh && cat id_rsa' });
    assert.equal(r.decision, 'ask');
    assert.equal(r.reason?.type, 'safetyCheck');
    assert.equal((r.reason as { check: string }).check, 'toctou');
    assert.match(r.message!, /cwd-desync TOCTOU/);
  });
});
