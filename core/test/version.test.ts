import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { AMI_VERSION, AMI_CLIENT_NAME, AMI_CLIENT_INFO } from '../src/version';

describe('version — single source of truth', () => {
  it('AMI_VERSION is a semver string', () => {
    assert.match(AMI_VERSION, /^\d+\.\d+\.\d+$/);
  });

  it('AMI_CLIENT_NAME is "ami"', () => {
    assert.equal(AMI_CLIENT_NAME, 'ami');
  });

  it('AMI_CLIENT_INFO combines the name and the real version (sourced, not hardcoded)', () => {
    assert.deepEqual(AMI_CLIENT_INFO, { name: AMI_CLIENT_NAME, version: AMI_VERSION });
  });
});
