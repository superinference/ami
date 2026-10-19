import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateDanglingOperator, validateBashSecurity } from '../src/tools/bash-security';

describe('validateDanglingOperator', () => {
  it('blocks command ending with &&', () => {
    const result = validateDanglingOperator('echo hello &&');
    assert.equal(result.safe, false);
    assert.ok(result.message.includes('dangling'));
  });

  it('blocks command ending with ||', () => {
    const result = validateDanglingOperator('echo hello ||');
    assert.equal(result.safe, false);
  });

  it('blocks command ending with ;', () => {
    const result = validateDanglingOperator('echo hello ;');
    assert.equal(result.safe, false);
  });

  it('blocks command ending with |', () => {
    const result = validateDanglingOperator('echo hello |');
    assert.equal(result.safe, false);
  });

  it('blocks command ending with && and trailing whitespace', () => {
    const result = validateDanglingOperator('echo hello &&   ');
    assert.equal(result.safe, false);
  });

  it('allows complete compound command', () => {
    const result = validateDanglingOperator('echo hello && echo world');
    assert.equal(result.safe, true);
  });

  it('allows piped command', () => {
    const result = validateDanglingOperator('cat file.txt | grep pattern');
    assert.equal(result.safe, true);
  });

  it('allows simple command', () => {
    const result = validateDanglingOperator('echo hello');
    assert.equal(result.safe, true);
  });

  it('allows empty command', () => {
    const result = validateDanglingOperator('');
    assert.equal(result.safe, true);
  });

  it('integration: validateBashSecurity blocks dangling &&', () => {
    const result = validateBashSecurity('ls -la &&');
    assert.equal(result.safe, false);
    assert.ok(result.message.includes('dangling'));
  });

  it('integration: validateBashSecurity allows complete &&', () => {
    const result = validateBashSecurity('ls -la && echo done');
    assert.equal(result.safe, true);
  });
});
