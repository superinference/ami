import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions.ts';

// ===========================================================================
// UNIT U015 — compound-command TOCTOU guard wired into the permission decision.
//
// An archive-extraction-before-git compound and a symlink-creating compound must
// require explicit approval (an `ask`) even under a wildcard/tool-wide allow rule,
// because the auto-allow paths cannot see the runtime repo/path redirection those
// shapes introduce (the archive is unpacked, or the link created, AFTER the
// permission decision and BEFORE the later command runs). Parity with the reference's
// GIT_SAFETY_ARCHIVE_EXTRACTORS + hasSymlinkCreate gates.
//
// RED/GREEN discriminator: with a tool-wide `allow` rule, these commands would
// auto-allow WITHOUT the guard; the guard forces `ask`. Non-TOCTOU commands under
// the same rule still auto-allow, so the test fails if the guard over-reaches.
// ===========================================================================

function toolWideAllow(): PermissionManager {
  return new PermissionManager('ask', [{ tool: 'bash', action: 'allow' }]);
}

describe('U015 compound-command TOCTOU permission guard', () => {
  it('forces ask for archive-extraction-before-git under a tool-wide allow rule', async () => {
    const pm = toolWideAllow();
    assert.equal(await pm.check('bash', { command: 'tar -xf payload.tar && git status' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'unzip pkg.zip && git log' }), 'ask');
    assert.equal(await pm.check('bash', { command: '7z x a.7z; git diff' }), 'ask');
  });

  it('forces ask for a symlink-creating compound under a tool-wide allow rule', async () => {
    const pm = toolWideAllow();
    assert.equal(await pm.check('bash', { command: 'ln -s /etc/passwd link && cat link' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'mkdir d && ln -s ../secret d/x' }), 'ask');
  });

  it('forces ask for a cp/link coreutil link-creating compound under a tool-wide allow rule (w92)', async () => {
    const pm = toolWideAllow();
    // cp -s / --symbolic-link / -l / --link and the `link` coreutil create a link
    // just like `ln`, so the same read-through-a-just-created-link TOCTOU applies.
    assert.equal(await pm.check('bash', { command: 'cp -s a b && cat b' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cp --symbolic-link a b; cat b' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cp -l a b && cat b' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'link a b && cat b' }), 'ask');
  });

  it('still auto-allows non-TOCTOU commands under the same rule (guard does not over-reach)', async () => {
    const pm = toolWideAllow();
    assert.equal(await pm.check('bash', { command: 'tar -cf out.tar dir' }), 'allow'); // archive, no git
    assert.equal(await pm.check('bash', { command: 'ln -s a b' }), 'allow');           // single segment
    assert.equal(await pm.check('bash', { command: 'cp a b && cat a' }), 'allow');     // plain copy, not a link
    assert.equal(await pm.check('bash', { command: 'cp -r a b && cat a' }), 'allow');  // recursive copy, no link flag
    assert.equal(await pm.check('bash', { command: 'echo hi && git status' }), 'allow'); // git, no archive/symlink
  });

  it('also forces ask in acceptEdits mode under a tool-wide allow rule', async () => {
    const pm = new PermissionManager('acceptEdits', [{ tool: 'bash', action: 'allow' }]);
    assert.equal(await pm.check('bash', { command: 'tar -xf payload.tar && git status' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'ln -s /etc/passwd link && cat link' }), 'ask');
  });

  it('lets an explicit deny rule still win over the TOCTOU ask', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'tar*', action: 'deny' }]);
    assert.equal(await pm.check('bash', { command: 'tar -xf x && git status' }), 'deny');
  });

  it('surfaces a structured toctou safetyCheck reason for the UI', async () => {
    const pm = toolWideAllow();
    const r = await pm.checkWithReason('bash', { command: 'tar -xf payload.tar && git status' });
    assert.equal(r.decision, 'ask');
    assert.equal(r.reason?.type, 'safetyCheck');
    assert.equal((r.reason as { check: string }).check, 'toctou');
    assert.match(r.message!, /archive/);

    // Use a non-credential link target so the toctou reason is not shadowed by the
    // (earlier, also-valid) credential-path safetyCheck.
    const r2 = await pm.checkWithReason('bash', { command: 'mkdir d && ln -s a d/x' });
    assert.equal((r2.reason as { check: string }).check, 'toctou');
    assert.match(r2.message!, /link/);
  });
});
