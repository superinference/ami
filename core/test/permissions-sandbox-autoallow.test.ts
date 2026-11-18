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

  it('mode:off never auto-allows without an injected host-eligibility resolver', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    // No resolver set → the host-mode arm is inert → normal prompt.
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

describe('PermissionManager — host-mode autoAllowBashIfSandboxed (U006-w115)', () => {
  // The resolver stands in for core's commandWillBeHostSandboxed: here only
  // commands containing "curl" (a sandbox trigger) are reported sandbox-eligible.
  const curlIsSandboxed = (command: string) => /\bcurl\b/.test(command);

  it('host mode auto-allows a command that WILL be host-sandboxed', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    pm.setSandboxEligibilityResolver(curlIsSandboxed);
    assert.equal(await pm.check('bash', { command: 'curl https://example.com' }), 'allow');
  });

  it('host mode falls through to a prompt for a sandbox-incompatible command', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    pm.setSandboxEligibilityResolver(curlIsSandboxed);
    // Not a trigger → resolver returns false → regular permission prompt.
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'ask');
  });

  it('host mode is OPT-IN: no auto-allow when the knob is unset', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy({ mode: 'off' });
    pm.setSandboxEligibilityResolver(curlIsSandboxed);
    assert.equal(await pm.check('bash', { command: 'curl https://example.com' }), 'ask');
  });

  it('host mode still prompts for a sensitive-path read even when sandbox-eligible', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    // Everything reported sandbox-eligible — the host FS is still reachable under
    // the unshare sandbox, so a credential read must not silently auto-allow.
    pm.setSandboxEligibilityResolver(() => true);
    assert.equal(await pm.check('bash', { command: 'curl file:///x; cat ~/.ssh/id_rsa' }), 'ask');
  });

  it('container mode ignores the host resolver (every command auto-allowed)', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(CONTAINER_AUTO);
    // Resolver says "never sandboxed" but container mode auto-allows regardless —
    // the container, not the resolver, is the boundary.
    pm.setSandboxEligibilityResolver(() => false);
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  });

  it('explicit deny rules still win over host-mode auto-allow', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'curl*', action: 'deny' }]);
    pm.setSandboxPolicy(OFF_AUTO);
    pm.setSandboxEligibilityResolver(curlIsSandboxed);
    assert.equal(await pm.check('bash', { command: 'curl https://evil.test' }), 'deny');
  });

  it('setSandboxEligibilityResolver(undefined) restores the inert host-mode behavior', async () => {
    const pm = new PermissionManager('ask');
    pm.setSandboxPolicy(OFF_AUTO);
    pm.setSandboxEligibilityResolver(curlIsSandboxed);
    assert.equal(await pm.check('bash', { command: 'curl https://example.com' }), 'allow');
    pm.setSandboxEligibilityResolver(undefined);
    assert.equal(await pm.check('bash', { command: 'curl https://example.com' }), 'ask');
  });
});
