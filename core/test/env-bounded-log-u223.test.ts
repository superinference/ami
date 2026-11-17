import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateBoundedEnvInt,
  collectEnvLimitDiagnostics,
} from '../src/utils/env';

/**
 * U223 — parity with Claude Code's `validateBoundedIntEnvVar`, which calls
 * `logForDebugging(`${name} ${message}`)` whenever a numeric env override is
 * rejected (invalid) or clamped (capped). AMI already returns the structured
 * `{ effective, status, message }`; this adds the debug-log emission so a
 * misconfigured value is surfaced to the debug log, not only to the return value.
 *
 * The side effect is injected (an optional `log` callback) so the env module
 * stays dependency-free; the real `/doctor` consumer wires the core debug logger.
 */
describe('validateBoundedEnvInt debug-log emission (U223 parity)', () => {
  it('emits the diagnostic message via the log callback when the value is invalid', () => {
    const lines: string[] = [];
    const r = validateBoundedEnvInt('X', 'abc', 10, 100, { log: (l) => lines.push(l) });
    assert.equal(r.status, 'invalid');
    assert.equal(lines.length, 1);
    assert.ok(r.message);
    assert.equal(lines[0], r.message);
  });

  it('emits the diagnostic message via the log callback when the value is capped', () => {
    const lines: string[] = [];
    const r = validateBoundedEnvInt('X', '5000', 10, 1000, { log: (l) => lines.push(l) });
    assert.equal(r.status, 'capped');
    assert.equal(lines.length, 1);
    assert.equal(lines[0], r.message);
  });

  it('rejects below the lower bound and logs it', () => {
    const lines: string[] = [];
    const r = validateBoundedEnvInt('X', '0', 10, 1000, { log: (l) => lines.push(l) });
    assert.equal(r.status, 'invalid');
    assert.equal(lines.length, 1);
  });

  it('does NOT invoke the log callback for a valid value', () => {
    const lines: string[] = [];
    const r = validateBoundedEnvInt('X', '42', 10, 100, { log: (l) => lines.push(l) });
    assert.equal(r.status, 'valid');
    assert.equal(lines.length, 0);
  });

  it('does NOT invoke the log callback for an unset/empty value', () => {
    const lines: string[] = [];
    validateBoundedEnvInt('X', undefined, 10, 100, { log: (l) => lines.push(l) });
    validateBoundedEnvInt('X', '', 10, 100, { log: (l) => lines.push(l) });
    assert.equal(lines.length, 0);
  });

  it('is side-effect-free when no log callback is provided (pure parity path)', () => {
    // No throw, structured result unchanged when the optional seam is omitted.
    assert.equal(validateBoundedEnvInt('X', 'abc', 10, 100).status, 'invalid');
    assert.equal(validateBoundedEnvInt('X', '5000', 10, 1000).status, 'capped');
  });
});

describe('collectEnvLimitDiagnostics forwards the log callback (U223)', () => {
  it('logs one line per non-valid finding and none for valid overrides', () => {
    const lines: string[] = [];
    const diags = collectEnvLimitDiagnostics(
      [
        { name: 'A', defaultValue: 10, upperLimit: 100 }, // capped by 500
        { name: 'B', defaultValue: 5, upperLimit: 50 }, // valid 20
        { name: 'C', defaultValue: 7, upperLimit: 70 }, // invalid -1
      ],
      { A: '500', B: '20', C: '-1' },
      { log: (l) => lines.push(l) },
    );
    assert.equal(diags.length, 2);
    assert.equal(lines.length, 2);
    // Each logged line equals the corresponding diagnostic message.
    for (const d of diags) {
      assert.ok(lines.includes(d.message));
    }
  });

  it('does not log when every override is within bounds', () => {
    const lines: string[] = [];
    const diags = collectEnvLimitDiagnostics(
      [{ name: 'A', defaultValue: 10, upperLimit: 100 }],
      { A: '20' },
      { log: (l) => lines.push(l) },
    );
    assert.equal(diags.length, 0);
    assert.equal(lines.length, 0);
  });

  it('remains usable without a log callback (backward-shim-free optional seam)', () => {
    const diags = collectEnvLimitDiagnostics(
      [{ name: 'A', defaultValue: 10, upperLimit: 100 }],
      { A: '500' },
    );
    assert.equal(diags.length, 1);
    assert.equal(diags[0].status, 'capped');
  });
});
