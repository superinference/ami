/**
 * Tests for bash security relaxation in detached mode.
 *
 * Three validators are skipped in detached mode because they produce
 * false positives on legitimate SWE-bench commands:
 *   - validateNewlines (multi-line compound commands)
 *   - validateRedirections (file output redirections)
 *   - validateShellMetacharsInCommands (2>&1 | grep patterns)
 *
 * All other security validators remain active in detached mode.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { validateBashSecurity } from '../src/tools/bash-security';

describe('validateBashSecurity — detached mode relaxation', () => {
  // -----------------------------------------------------------------------
  // Commands that should be ALLOWED in detached mode (blocked in interactive)
  // -----------------------------------------------------------------------

  it('allows grep with 2>&1 pipe in detached mode', () => {
    const result = validateBashSecurity(
      'go test ./... 2>&1 | grep -v "no test"',
      { detachedMode: true },
    );
    assert.equal(result.safe, true, `Should be safe in detached mode, got: ${result.message}`);
  });

  it('allows file output redirection in detached mode', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/test-output.txt',
      { detachedMode: true },
    );
    assert.equal(result.safe, true, `Should be safe in detached mode, got: ${result.message}`);
  });

  it('allows stderr redirection to file in detached mode', () => {
    const result = validateBashSecurity(
      'make build 2>/tmp/build-errors.log',
      { detachedMode: true },
    );
    assert.equal(result.safe, true, `Should be safe in detached mode, got: ${result.message}`);
  });

  it('allows multi-line compound command in detached mode', () => {
    const result = validateBashSecurity(
      'cd /repo && make build\n./run_tests.sh',
      { detachedMode: true },
    );
    assert.equal(result.safe, true, `Should be safe in detached mode, got: ${result.message}`);
  });

  it('allows find with pipe and xargs in detached mode', () => {
    const result = validateBashSecurity(
      'find . -name "*.go" -type f | xargs grep "func Test"',
      { detachedMode: true },
    );
    assert.equal(result.safe, true, `Should be safe in detached mode, got: ${result.message}`);
  });

  // -----------------------------------------------------------------------
  // Same commands BLOCKED in interactive mode (default)
  // -----------------------------------------------------------------------

  it('blocks grep with shell metachar in interactive mode', () => {
    const result = validateBashSecurity(
      'go test ./... 2>&1 | grep -v "no test"',
    );
    assert.equal(result.safe, false);
  });

  it('blocks file output redirection in interactive mode', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/test-output.txt',
    );
    assert.equal(result.safe, false);
  });

  it('blocks multi-line compound command in interactive mode', () => {
    const result = validateBashSecurity(
      'cd /repo && make build\n./run_tests.sh',
    );
    assert.equal(result.safe, false);
  });

  // -----------------------------------------------------------------------
  // Dangerous patterns STILL BLOCKED even in detached mode
  // -----------------------------------------------------------------------

  it('still blocks IFS injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo $IFS',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks control characters in detached mode', () => {
    const result = validateBashSecurity(
      'echo hello\x00world',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks proc environ access in detached mode', () => {
    const result = validateBashSecurity(
      'cat /proc/self/environ',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks command substitution injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo $(rm -rf /)',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks backtick injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo `rm -rf /`',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  // -----------------------------------------------------------------------
  // Signature backward-compatibility: no options arg = interactive mode
  // -----------------------------------------------------------------------

  it('works without options parameter (backward compatible)', () => {
    const result = validateBashSecurity('echo hello');
    assert.equal(result.safe, true);
  });

  it('works with empty options object', () => {
    const result = validateBashSecurity('echo hello', {});
    assert.equal(result.safe, true);
  });

  it('works with detachedMode: false (same as interactive)', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/out.txt',
      { detachedMode: false },
    );
    assert.equal(result.safe, false);
  });
});
