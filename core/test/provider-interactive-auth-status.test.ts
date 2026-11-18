/**
 * InteractiveAuthStatus schema (parity: Claude Code's AwsAuthStatus — the LIVE
 * isAuthenticating/error/output half of AwsAuthStatusManager). Verifies the neutral
 * factory and the three-field shape AMI uses to represent an in-flight provider login.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { idleInteractiveAuthStatus, type InteractiveAuthStatus } from '../src/provider';

describe('idleInteractiveAuthStatus', () => {
  it('returns a neutral status: not authenticating, no error, empty output', () => {
    const s = idleInteractiveAuthStatus();
    assert.equal(s.isAuthenticating, false);
    assert.equal(s.error, undefined);
    assert.deepEqual(s.output, []);
  });

  it('returns a fresh object each call (no shared mutable output array)', () => {
    const a = idleInteractiveAuthStatus();
    const b = idleInteractiveAuthStatus();
    a.output.push('leak');
    assert.deepEqual(b.output, [], 'a second idle status must not see the first mutation');
  });

  it('types the three AwsAuthStatus-parity fields', () => {
    const s: InteractiveAuthStatus = { isAuthenticating: true, error: 'boom', output: ['line1', 'line2'] };
    assert.equal(s.isAuthenticating, true);
    assert.equal(s.error, 'boom');
    assert.equal(s.output.length, 2);
  });
});
