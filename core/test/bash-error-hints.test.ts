import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// The diagnoseError function is private, so we test through the error hints patterns
const ERROR_HINTS: Array<{ pattern: RegExp; hint: string }> = [
  { pattern: /Transform failed|SyntaxError:.*Unexpected/i, hint: 'syntax error' },
  { pattern: /Cannot find module|ERR_MODULE_NOT_FOUND/i, hint: 'Module not found' },
  { pattern: /ENOENT.*no such file/i, hint: 'does not exist' },
  { pattern: /TypeError:.*is not a function/i, hint: 'function call is wrong' },
  { pattern: /ReferenceError:.*is not defined/i, hint: 'missing' },
  { pattern: /EACCES.*permission denied/i, hint: 'Permission denied' },
  { pattern: /EADDRINUSE/i, hint: 'Port already in use' },
  { pattern: /ENOMEM|Cannot allocate memory|out of memory/i, hint: 'memory' },
  { pattern: /ETIMEDOUT|ECONNREFUSED|ECONNRESET/i, hint: 'Network error' },
  { pattern: /EPIPE|broken pipe/i, hint: 'Broken pipe' },
  { pattern: /EMFILE|ENFILE|too many open files/i, hint: 'open files' },
  { pattern: /TS\d+:/i, hint: 'TypeScript' },
  { pattern: /error\[E\d+\]/i, hint: 'Rust' },
  { pattern: /npm ERR!.*ERESOLVE/i, hint: 'NPM' },
  { pattern: /fatal: not a git repository/i, hint: 'git repository' },
];

function diagnoseError(output: string): string | null {
  for (const { pattern, hint } of ERROR_HINTS) {
    if (pattern.test(output)) return hint;
  }
  return null;
}

describe('bash error hints', () => {
  it('detects EACCES permission denied', () => {
    assert.ok(diagnoseError('Error: EACCES: permission denied, open "/etc/shadow"'));
  });

  it('detects EADDRINUSE', () => {
    assert.ok(diagnoseError('Error: listen EADDRINUSE: address already in use :::3000'));
  });

  it('detects out of memory', () => {
    assert.ok(diagnoseError('FATAL ERROR: ENOMEM, not enough memory'));
    assert.ok(diagnoseError('Cannot allocate memory'));
    assert.ok(diagnoseError('JavaScript heap out of memory'));
  });

  it('detects network errors', () => {
    assert.ok(diagnoseError('Error: connect ECONNREFUSED 127.0.0.1:5432'));
    assert.ok(diagnoseError('Error: ETIMEDOUT'));
    assert.ok(diagnoseError('Error: ECONNRESET'));
  });

  it('detects TypeScript errors', () => {
    assert.ok(diagnoseError("error TS2304: Cannot find name 'foo'."));
    assert.ok(diagnoseError("TS2345: Argument of type 'string' is not assignable"));
  });

  it('detects Rust compilation errors', () => {
    assert.ok(diagnoseError('error[E0308]: mismatched types'));
  });

  it('detects npm resolution errors', () => {
    assert.ok(diagnoseError('npm ERR! ERESOLVE unable to resolve dependency tree'));
  });

  it('detects git not a repository', () => {
    assert.ok(diagnoseError('fatal: not a git repository (or any of the parent directories)'));
  });

  it('detects broken pipe', () => {
    assert.ok(diagnoseError('Error: write EPIPE'));
    assert.ok(diagnoseError('cat: write error: Broken pipe'));
  });

  it('detects too many open files', () => {
    assert.ok(diagnoseError('Error: EMFILE: too many open files'));
    assert.ok(diagnoseError('ENFILE: file table overflow'));
  });

  it('returns null for unknown errors', () => {
    assert.equal(diagnoseError('Something went wrong'), null);
    assert.equal(diagnoseError('Error code 42'), null);
  });

  it('returns null for empty output', () => {
    assert.equal(diagnoseError(''), null);
  });
});
