import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  classifySafetyCheckFailure,
  type SafetyClassifierFailure,
} from '../src/error-classifier';
import {
  resolveAutoModeClassifierOutcome,
  makeAutoModeClassifierFailure,
  type AutoModeClassifierOutcome,
} from '../src/auto-mode';

// ===========================================================================
// U073 — fail-safe fallback semantics for a safety classifier that CAN FAIL.
//
// Parity with Claude Code's YoloClassifierResult (`unavailable`,
// `transcriptTooLong`, `durationMs`) + the documented decision that a classifier
// which cannot produce a verdict must FALL BACK to normal prompting — never
// retry a deterministic failure and never auto-approve/hard-deny a failed check.
// ===========================================================================

describe('classifySafetyCheckFailure — structured failure shape', () => {
  it('marks any classifier call that could not produce a verdict as unavailable', () => {
    const f = classifySafetyCheckFailure(new Error('HTTP 500 - internal server error'));
    assert.equal(f.unavailable, true);
    assert.equal(f.category, 'server_error');
    assert.ok(f.reason.length > 0);
  });

  it('flags a DETERMINISTIC transcript-too-long and forces retryable=false', () => {
    // classifyError() marks a context overflow retryable (the MAIN model can
    // compact and retry), but for a CLASSIFIER call the same transcript always
    // overflows — retrying is futile, so the fail-safe primitive overrides it.
    const f = classifySafetyCheckFailure('prompt is too long: 250000 tokens > 200000 maximum');
    assert.equal(f.transcriptTooLong, true);
    assert.equal(f.retryable, false);
  });

  it('treats a broader context-overflow phrasing as transcriptTooLong too', () => {
    const f = classifySafetyCheckFailure(new Error('This model\'s maximum context length is exceeded: 413'));
    assert.equal(f.transcriptTooLong, true);
    assert.equal(f.retryable, false);
  });

  it('keeps a TRANSIENT failure retryable (retry before falling back may succeed)', () => {
    const rate = classifySafetyCheckFailure(new Error('429 rate limit exceeded'));
    assert.equal(rate.transcriptTooLong, false);
    assert.equal(rate.retryable, true);

    const net = classifySafetyCheckFailure(new Error('fetch failed: ECONNRESET'));
    assert.equal(net.transcriptTooLong, false);
    assert.equal(net.retryable, true);
  });

  it('marks a permanent auth failure non-retryable but still unavailable', () => {
    const f = classifySafetyCheckFailure(new Error('401 invalid api key'));
    assert.equal(f.unavailable, true);
    assert.equal(f.transcriptTooLong, false);
    assert.equal(f.retryable, false);
  });

  it('records durationMs timeout/latency telemetry when supplied', () => {
    const f = classifySafetyCheckFailure(new Error('network timeout'), { durationMs: 1234 });
    assert.equal(f.durationMs, 1234);
  });

  it('omits durationMs when not supplied', () => {
    const f = classifySafetyCheckFailure(new Error('boom'));
    assert.equal('durationMs' in f, false);
  });

  it('classifies a timeout/abort of the classifier call without throwing', () => {
    const abort = new Error('The operation was aborted');
    abort.name = 'AbortError';
    const f: SafetyClassifierFailure = classifySafetyCheckFailure(abort, { durationMs: 30000 });
    assert.equal(f.unavailable, true);
    assert.equal(f.category, 'abort');
    assert.equal(f.durationMs, 30000);
  });
});

describe('resolveAutoModeClassifierOutcome — fail-safe fallback to prompting', () => {
  it('auto-approves only when the classifier explicitly approved', () => {
    const outcome: AutoModeClassifierOutcome = { approved: true };
    assert.equal(resolveAutoModeClassifierOutcome(outcome), 'auto-approve');
  });

  it('prompts when the classifier explicitly declined', () => {
    assert.equal(resolveAutoModeClassifierOutcome({ approved: false }), 'prompt');
  });

  it('NEVER auto-approves a failed check — falls back to prompting (not fail-closed, not fail-open)', () => {
    const outcome = makeAutoModeClassifierFailure(
      'prompt is too long: 250000 tokens > 200000 maximum',
    );
    // The verdict could not be produced → the user is prompted, not silently
    // allowed or hard-blocked.
    assert.equal(resolveAutoModeClassifierOutcome(outcome), 'prompt');
    assert.equal(outcome.failure?.transcriptTooLong, true);
  });

  it('prompts on a transient classifier outage too (fail-safe, regardless of retryability)', () => {
    const outcome = makeAutoModeClassifierFailure(new Error('HTTP 503 overloaded'), {
      durationMs: 9000,
    });
    assert.equal(resolveAutoModeClassifierOutcome(outcome), 'prompt');
    assert.equal(outcome.failure?.durationMs, 9000);
    assert.equal(outcome.failure?.retryable, true);
  });

  it('prompts when the outcome is empty/indeterminate (no verdict, no failure)', () => {
    assert.equal(resolveAutoModeClassifierOutcome({}), 'prompt');
  });
});
