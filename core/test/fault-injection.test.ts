// ===========================================================================
// fault-injection.test.ts — runtime fault-injection seam (U073)
//
// Covers parseFaultInjection, the live queue (consume/inject/reset/active),
// makeInjectedError + its classification by the REAL classifyError, and
// maybeThrowFault's env-gated throw. ESM-coverage-safe: no source-text asserts,
// temp env vars only, env state reset in afterEach.
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import {
  FAULT_INJECTION_ENV,
  FAULT_DEFAULT_STATUS,
  parseFaultInjection,
  resetFaultInjection,
  faultInjectionActive,
  injectFault,
  consumeFault,
  makeInjectedError,
  maybeThrowFault,
} from '../src/fault-injection';
import { classifyError } from '../src/error-classifier';

afterEach(() => {
  delete process.env[FAULT_INJECTION_ENV];
  resetFaultInjection();
});

describe('parseFaultInjection', () => {
  it('returns [] for an unset / blank value', () => {
    assert.deepEqual(parseFaultInjection(undefined), []);
    assert.deepEqual(parseFaultInjection(''), []);
    assert.deepEqual(parseFaultInjection('   '), []);
  });

  it('parses a single entry with per-kind default status and count 1', () => {
    assert.deepEqual(parseFaultInjection('provider:transient'), [
      { target: 'provider', kind: 'transient', status: 503, count: 1 },
    ]);
    assert.deepEqual(parseFaultInjection('provider:fatal'), [
      { target: 'provider', kind: 'fatal', status: 403, count: 1 },
    ]);
  });

  it('parses explicit status and count, and multiple comma-separated entries', () => {
    assert.deepEqual(parseFaultInjection('provider:fatal:401:2, mcp:transient:500:3'), [
      { target: 'provider', kind: 'fatal', status: 401, count: 2 },
      { target: 'mcp', kind: 'transient', status: 500, count: 3 },
    ]);
  });

  it('treats an empty status/count field as "use default"', () => {
    assert.deepEqual(parseFaultInjection('provider:transient::4'), [
      { target: 'provider', kind: 'transient', status: 503, count: 4 },
    ]);
  });

  it('skips malformed entries fail-safe (missing target, unknown kind, bad numbers)', () => {
    assert.deepEqual(parseFaultInjection(':transient'), []);
    assert.deepEqual(parseFaultInjection('provider:bogus'), []);
    assert.deepEqual(parseFaultInjection('provider'), []);
    assert.deepEqual(parseFaultInjection('provider:transient:notnum'), []);
    assert.deepEqual(parseFaultInjection('provider:transient:503:0'), []);
    assert.deepEqual(parseFaultInjection('provider:transient:503:-1'), []);
    assert.deepEqual(parseFaultInjection('provider:transient:1.5'), []);
    // trailing comma / blank token is skipped, the good one survives
    assert.deepEqual(parseFaultInjection('provider:transient,'), [
      { target: 'provider', kind: 'transient', status: 503, count: 1 },
    ]);
  });

  it('exposes the per-kind default status map', () => {
    assert.equal(FAULT_DEFAULT_STATUS.transient, 503);
    assert.equal(FAULT_DEFAULT_STATUS.fatal, 403);
  });
});

describe('live queue — consume / inject / reset / active', () => {
  it('is inactive and consumes nothing when the env is unset', () => {
    assert.equal(faultInjectionActive(), false);
    assert.equal(consumeFault('provider'), null);
  });

  it('seeds the queue lazily from the env on first use', () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:transient:503:2';
    resetFaultInjection();
    assert.equal(faultInjectionActive(), true);
    const first = consumeFault('provider');
    assert.deepEqual(first, { target: 'provider', kind: 'transient', status: 503, count: 1 });
    // still queued (count was 2 → 1)
    assert.equal(faultInjectionActive(), true);
    const second = consumeFault('provider');
    assert.equal(second!.count, 0);
    // drained and removed
    assert.equal(faultInjectionActive(), false);
    assert.equal(consumeFault('provider'), null);
  });

  it('only consumes faults for the matching target', () => {
    process.env[FAULT_INJECTION_ENV] = 'mcp:fatal';
    resetFaultInjection();
    assert.equal(consumeFault('provider'), null);
    assert.equal(consumeFault('mcp')!.kind, 'fatal');
  });

  it('supports programmatic injection, even with the env unset', () => {
    assert.equal(faultInjectionActive(), false);
    injectFault({ target: 'provider', kind: 'fatal', status: 403, count: 1 });
    assert.equal(faultInjectionActive(), true);
    const f = consumeFault('provider');
    assert.equal(f!.kind, 'fatal');
    assert.equal(faultInjectionActive(), false);
  });

  it('resetFaultInjection re-reads the env', () => {
    injectFault({ target: 'provider', kind: 'fatal', status: 403, count: 1 });
    assert.equal(faultInjectionActive(), true);
    resetFaultInjection();
    assert.equal(faultInjectionActive(), false);
  });
});

describe('makeInjectedError — classified by the REAL classifyError', () => {
  it('a transient fault is RETRYABLE (parity: transient → retry/backoff)', () => {
    const err = makeInjectedError({ target: 'provider', kind: 'transient', status: 503, count: 1 });
    assert.match(err.message, /HTTP 503/);
    assert.match(err.message, /injected transient fault/);
    const classified = classifyError(err);
    assert.equal(classified.retryable, true, 'transient 503 must be retryable');
    // 503 is a capacity overload → a retryable server/overload category.
    assert.ok(
      ['server_error', 'model_unavailable', 'network_error', 'rate_limited'].includes(classified.category),
      `unexpected retryable category ${classified.category}`,
    );
  });

  it('a fatal fault is NON-retryable (parity: fatal → teardown)', () => {
    const err = makeInjectedError({ target: 'provider', kind: 'fatal', status: 403, count: 1 });
    assert.match(err.message, /HTTP 403/);
    const classified = classifyError(err);
    assert.equal(classified.retryable, false, 'fatal 403 must NOT be retryable');
    assert.equal(classified.category, 'auth_error');
  });

  it('folds an optional errorType label into the message', () => {
    const err = makeInjectedError({ target: 'provider', kind: 'transient', status: 503, count: 1, errorType: 'overloaded_error' });
    assert.match(err.message, /overloaded_error/);
  });
});

describe('maybeThrowFault', () => {
  it('is a no-op when nothing is queued', () => {
    assert.doesNotThrow(() => maybeThrowFault('provider'));
  });

  it('throws a classifiable error for a queued transient fault, then drains', () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:transient';
    resetFaultInjection();
    assert.throws(() => maybeThrowFault('provider'), /HTTP 503/);
    // one-shot: the next call is clean
    assert.doesNotThrow(() => maybeThrowFault('provider'));
  });

  it('throws a non-retryable error for a queued fatal fault', () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:fatal';
    resetFaultInjection();
    let thrown: unknown;
    assert.throws(() => {
      try {
        maybeThrowFault('provider');
      } catch (e) {
        thrown = e;
        throw e;
      }
    });
    assert.equal(classifyError(thrown).retryable, false);
  });

  it('does not throw for a non-matching target', () => {
    process.env[FAULT_INJECTION_ENV] = 'mcp:fatal';
    resetFaultInjection();
    assert.doesNotThrow(() => maybeThrowFault('provider'));
    // the mcp fault is still queued
    assert.throws(() => maybeThrowFault('mcp'));
  });
});
