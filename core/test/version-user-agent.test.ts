import { test } from 'node:test';
import assert from 'node:assert/strict';
import { amiUserAgent, AMI_VERSION, AMI_CLIENT_NAME } from '../src/version';

test('amiUserAgent is version-stamped with the single-source AMI version', () => {
  assert.equal(amiUserAgent(), `${AMI_CLIENT_NAME}/${AMI_VERSION}`);
  assert.match(amiUserAgent(), /^ami\/\d+\.\d+\.\d+/);
});

test('amiUserAgent appends an optional component suffix', () => {
  assert.equal(amiUserAgent('skill-discovery'), `ami/${AMI_VERSION} (skill-discovery)`);
});

test('amiUserAgent reflects the version constant (not a hardcoded string)', () => {
  assert.ok(amiUserAgent('x').includes(AMI_VERSION));
});
