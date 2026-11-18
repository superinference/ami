import { test } from 'node:test';
import assert from 'node:assert/strict';
import { commandWillBeHostSandboxed } from '../src/tools/bash-sandbox';

// Deterministic env: never let the host-machine env flip the host-sandbox gates.
const CLEAN: NodeJS.ProcessEnv = {};

test('commandWillBeHostSandboxed: a trigger command on an enabled platform WILL be sandboxed', () => {
  assert.equal(
    commandWillBeHostSandboxed('curl https://example.com', { env: CLEAN, platform: 'linux' }),
    true,
  );
});

test('commandWillBeHostSandboxed: a non-trigger command is NOT host-sandboxed (trigger allowlist)', () => {
  assert.equal(
    commandWillBeHostSandboxed('ls -la', { env: CLEAN, platform: 'linux' }),
    false,
  );
  assert.equal(
    commandWillBeHostSandboxed('echo hello', { env: CLEAN, platform: 'linux' }),
    false,
  );
});

test('commandWillBeHostSandboxed: default-deny env sandboxes a non-trigger command', () => {
  assert.equal(
    commandWillBeHostSandboxed('./evil', { env: { AMI_SANDBOX_DEFAULT_DENY: '1' }, platform: 'linux' }),
    true,
  );
});

test('commandWillBeHostSandboxed: excludedCommands carve-out suppresses the sandbox under default-deny', () => {
  assert.equal(
    commandWillBeHostSandboxed('mytool run', {
      env: { AMI_SANDBOX_DEFAULT_DENY: '1' },
      platform: 'linux',
      excludedCommands: ['mytool'],
    }),
    false,
  );
});

test('commandWillBeHostSandboxed: enabled===false is the host off-switch', () => {
  assert.equal(
    commandWillBeHostSandboxed('curl https://example.com', { env: CLEAN, platform: 'linux', enabled: false }),
    false,
  );
});

test('commandWillBeHostSandboxed: the AMI_DISABLE_SANDBOX env knob disables host sandboxing', () => {
  assert.equal(
    commandWillBeHostSandboxed('curl https://example.com', { env: { AMI_DISABLE_SANDBOX: '1' }, platform: 'linux' }),
    false,
  );
});

test('commandWillBeHostSandboxed: a disabled platform returns false', () => {
  assert.equal(
    commandWillBeHostSandboxed('curl https://example.com', { env: CLEAN, platform: 'win32', enabledPlatforms: ['linux'] }),
    false,
  );
  // undefined enabledPlatforms → all platforms host-sandbox.
  assert.equal(
    commandWillBeHostSandboxed('curl https://example.com', { env: CLEAN, platform: 'win32' }),
    true,
  );
});

test('commandWillBeHostSandboxed: honors an injected shouldUseSandbox impl (bash test seam)', () => {
  let seen = '';
  const out = commandWillBeHostSandboxed('anything at all', {
    env: CLEAN,
    platform: 'linux',
    shouldUseSandboxImpl: (cmd) => { seen = cmd; return true; },
  });
  assert.equal(out, true);
  assert.equal(seen, 'anything at all');
});
