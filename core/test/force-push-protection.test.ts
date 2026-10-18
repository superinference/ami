import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectForcePushToProtected } from '../src/tools/bash';

describe('detectForcePushToProtected', () => {
  it('blocks git push --force main', () => {
    const result = detectForcePushToProtected('git push --force origin main');
    assert.ok(result);
    assert.match(result!, /main/);
  });

  it('blocks git push -f master', () => {
    const result = detectForcePushToProtected('git push -f origin master');
    assert.ok(result);
    assert.match(result!, /master/);
  });

  it('blocks git push --force production', () => {
    const result = detectForcePushToProtected('git push --force origin production');
    assert.ok(result);
    assert.match(result!, /production/);
  });

  it('blocks git push --force-with-lease main', () => {
    const result = detectForcePushToProtected('git push --force-with-lease origin main');
    assert.ok(result);
    assert.match(result!, /main/);
  });

  it('allows git push --force to feature branch', () => {
    const result = detectForcePushToProtected('git push --force origin feature/my-branch');
    assert.equal(result, null);
  });

  it('allows regular git push to main', () => {
    const result = detectForcePushToProtected('git push origin main');
    assert.equal(result, null);
  });

  it('allows regular git push', () => {
    const result = detectForcePushToProtected('git push');
    assert.equal(result, null);
  });

  it('ignores non-git commands', () => {
    const result = detectForcePushToProtected('echo git push --force main');
    assert.equal(result, null);
  });

  it('blocks git push --force release', () => {
    const result = detectForcePushToProtected('git push --force origin release');
    assert.ok(result);
  });

  it('blocks git push --force develop', () => {
    const result = detectForcePushToProtected('git push --force origin develop');
    assert.ok(result);
  });
});
