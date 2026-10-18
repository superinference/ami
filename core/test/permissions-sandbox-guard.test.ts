import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

describe('dangerouslyDisableSandbox permission guard', () => {
  const sandboxInput = { command: 'ls -la', dangerouslyDisableSandbox: true };
  const normalInput = { command: 'ls -la' };

  it('returns ask in auto-allow mode', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', sandboxInput), 'ask');
  });

  it('returns ask in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', sandboxInput), 'ask');
  });

  it('returns ask in bypassPermissions mode', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('bash', sandboxInput), 'ask');
  });

  it('returns ask in regular ask mode', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(await pm.check('bash', sandboxInput), 'ask');
  });

  it('returns deny in deny-all mode', async () => {
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check('bash', sandboxInput), 'deny');
  });

  it('returns deny in dontAsk mode', async () => {
    const pm = new PermissionManager('dontAsk');
    assert.equal(await pm.check('bash', sandboxInput), 'deny');
  });

  it('auto-allow still allows normal bash without dangerouslyDisableSandbox', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', normalInput), 'allow');
  });

  it('does not trigger when dangerouslyDisableSandbox is false', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(
      await pm.check('bash', { command: 'ls', dangerouslyDisableSandbox: false }),
      'allow',
    );
  });

  it('does not trigger when dangerouslyDisableSandbox is undefined', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
  });

  it('hardline deny still wins over dangerouslyDisableSandbox', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(
      await pm.check('bash', { command: 'rm -rf /', dangerouslyDisableSandbox: true }),
      'deny',
    );
  });

  it('does not affect non-bash tools', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(
      await pm.check('file_edit', { dangerouslyDisableSandbox: true, file_path: 'foo.ts', old_string: 'a', new_string: 'b' }),
      'allow',
    );
  });

  it('returns ask even with an allow-all rule present', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', effect: 'allow' as const },
    ]);
    assert.equal(await pm.check('bash', sandboxInput), 'ask');
  });
});
