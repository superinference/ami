import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { PermissionManager } from '../src/permissions';
import type { SandboxPolicy } from '../src/types';

const CONTAINER_AUTO: SandboxPolicy = { mode: 'container', autoAllowBashIfSandboxed: true };
const CONTAINER_NOAUTO: SandboxPolicy = { mode: 'container' };
const OFF_AUTO: SandboxPolicy = { mode: 'off', autoAllowBashIfSandboxed: true };

describe('PermissionManager — autoAllowBashIfSandboxed (U006)', () => {
  it('setSandboxPolicy(undefined) clears a previously-set policy (prompt restored)', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'npm run build' }), 'allow');
    pm.setSandboxPolicy(undefined);
    assert.equal(await pm.check('bash', { command: 'npm run build' }), 'ask');
  });

  it('auto-allows bash in ask mode when container-sandboxed and knob opted in', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    // A command that would normally prompt in ask mode.
    assert.equal(await pm.check('bash', { command: 'npm install left-pad' }), 'allow');
  });

  it('auto-allows bash in acceptEdits mode too', async () => {
    const pm = new PermissionManager('acceptEdits');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'npm run build' }), 'allow');
  });

  it('is OPT-IN: no auto-allow when the knob is unset (default behavior preserved)', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_NOAUTO);
    assert.equal(await pm.check('bash', { command: 'npm install left-pad' }), 'ask');
  });

  it('does not auto-allow when there is no sandbox policy at all', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(await pm.check('bash', { command: 'npm install left-pad' }), 'ask');
  });

  it('only applies in container mode (mode:off never auto-allows)', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    assert.equal(await pm.check('bash', { command: 'npm install left-pad' }), 'ask');
  });

  it('only applies to the bash tool', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    // file_write is unaffected by the bash sandbox knob.
    assert.equal(await pm.check('file_write', { file_path: '/tmp/x', content: 'hi' }), 'ask');
  });

  it('hardline blocks still win over the sandbox auto-allow', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'rm -rf /' }), 'deny');
  });

  it('explicit deny rules still win over the sandbox auto-allow', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'npm install*', action: 'deny' }]);
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'npm install left-pad' }), 'deny');
    // A non-matching command is still auto-allowed.
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  });

  it('dangerouslyDisableSandbox still prompts (escape hatch never silently allowed)', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'make', dangerouslyDisableSandbox: true }), 'ask');
  });

  it('deny-all mode stays deny even with container auto-allow set', async () => {
    const pm = new PermissionManager('deny-all');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'deny');
  });

  it('plan mode stays deny for bash even with container auto-allow set', async () => {
    const pm = new PermissionManager('plan');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'deny');
  });
});
