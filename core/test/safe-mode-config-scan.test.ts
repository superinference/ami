import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  detectConfigEscalationVectors,
  isTrustedByAncestor,
  shouldEnableSafeMode,
} from '../src/safe-mode';

test('detectConfigEscalationVectors flags credential/command helpers', () => {
  assert.deepEqual(detectConfigEscalationVectors({ apiKeyHelper: '/bin/get-key' }), ['apiKeyHelper']);
  assert.ok(detectConfigEscalationVectors({ awsCredentialExport: 'x' }).includes('awsCredentialExport'));
  assert.ok(detectConfigEscalationVectors({ otelHeadersHelper: 'x' }).includes('otelHeadersHelper'));
  assert.deepEqual(detectConfigEscalationVectors({ apiKeyHelper: '' }), []); // empty = not set
  assert.deepEqual(detectConfigEscalationVectors(null), []);
  assert.deepEqual(detectConfigEscalationVectors({ model: 'opus' }), []);
});

test('detectConfigEscalationVectors flags non-allowlisted env vars but not safe ones', () => {
  assert.ok(detectConfigEscalationVectors({ env: { PATH: '/evil:/bin' } }).includes('env.PATH'));
  assert.ok(detectConfigEscalationVectors({ env: { NODE_OPTIONS: '--require x' } }).includes('env.NODE_OPTIONS'));
  assert.ok(detectConfigEscalationVectors({ env: { DOCKER_HOST: 'tcp://evil' } }).includes('env.DOCKER_HOST'));
  assert.deepEqual(detectConfigEscalationVectors({ env: { NODE_ENV: 'production', TERM: 'xterm' } }), []);
});

test('isTrustedByAncestor: cwd inside a trusted root (git-root inheritance)', () => {
  assert.equal(isTrustedByAncestor('/home/u/proj/src', ['/home/u/proj']), true);
  assert.equal(isTrustedByAncestor('/home/u/proj', ['/home/u/proj']), true);
  assert.equal(isTrustedByAncestor('/home/u/other', ['/home/u/proj']), false);
  assert.equal(isTrustedByAncestor('/home/u/proj-evil', ['/home/u/proj']), false); // prefix but not a subdir
  assert.equal(isTrustedByAncestor('', ['/home/u/proj']), false);
  assert.equal(isTrustedByAncestor('/x', []), false);
});

test('shouldEnableSafeMode: config escalation forces safe mode even inside a trusted root', () => {
  assert.equal(shouldEnableSafeMode('/p', { hasConfigEscalation: true }), true);
  assert.equal(shouldEnableSafeMode('/p/sub', {
    untrustedProject: true, hasConfigEscalation: true, trustedRoots: ['/p'],
  }), true);
});

test('shouldEnableSafeMode: trusted-ancestor inheritance suppresses untrusted-project flag', () => {
  // without inheritance, untrustedProject engages safe mode
  assert.equal(shouldEnableSafeMode('/p/sub', { untrustedProject: true }), true);
  // with the cwd inside a trusted root, the untrusted-project flag is inherited-away
  assert.equal(shouldEnableSafeMode('/p/sub', { untrustedProject: true, trustedRoots: ['/p'] }), false);
  // clonedFromUnknown similarly suppressed under a trusted root
  assert.equal(shouldEnableSafeMode('/p/sub', { clonedFromUnknown: true, trustedRoots: ['/p'] }), false);
  // but a non-inherited cwd still engages
  assert.equal(shouldEnableSafeMode('/elsewhere', { untrustedProject: true, trustedRoots: ['/p'] }), true);
});

test('shouldEnableSafeMode: existing prompt-override+MCP path still engages', () => {
  assert.equal(shouldEnableSafeMode('/p', { hasPromptOverride: true, hasUntrustedMcp: true }), true);
  assert.equal(shouldEnableSafeMode('/p', { hasPromptOverride: true }), false);
});
