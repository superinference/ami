/**
 * U061 — `overloaded` flag on ClassifiedError.
 *
 * A transient capacity overload (HTTP 503/529, "overloaded", "capacity") must be
 * flagged `overloaded: true` so the engine rides it out on the same model for a few
 * retries before switching to a fallback model (parity: the reference's MAX_529_RETRIES
 * threshold before FallbackTriggeredError). A genuine model-unavailable error
 * (`model_not_found`) is still `model_unavailable`/`shouldFallback` but is NOT an
 * overload, so it keeps falling back immediately.
 *
 * RED before the error-classifier change: the 503/529/"overloaded"/"capacity" cases
 * have no `overloaded` field (undefined), so `assert.equal(..., true)` fails.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { APICallError } from 'ai';
import { classifyError } from '../src/error-classifier';

function apiError(statusCode: number, message = `HTTP ${statusCode}`): APICallError {
  return new APICallError({
    message,
    statusCode,
    url: 'https://example.test/v1',
    requestBodyValues: {},
    responseHeaders: {},
    responseBody: message,
    isRetryable: true,
  });
}

describe('classifyError — overloaded flag', () => {
  it('flags a 529 APICallError as overloaded', () => {
    const r = classifyError(apiError(529));
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.shouldFallback, true);
    assert.equal(r.overloaded, true);
  });

  it('flags a 503 APICallError as overloaded', () => {
    const r = classifyError(apiError(503));
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.overloaded, true);
  });

  it('flags a textual "HTTP 529" as overloaded', () => {
    const r = classifyError('Error: HTTP 529 server overloaded');
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.overloaded, true);
  });

  it('flags "overloaded" text as overloaded', () => {
    const r = classifyError('The upstream is overloaded, try again');
    assert.equal(r.overloaded, true);
  });

  it('flags "capacity" text as overloaded', () => {
    const r = classifyError('No capacity available right now');
    assert.equal(r.overloaded, true);
  });

  it('does NOT flag model_not_found as overloaded (immediate fallback)', () => {
    const r = classifyError('Error: model_not_found');
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.shouldFallback, true);
    assert.equal(r.overloaded, undefined);
  });

  it('does NOT flag a 500/502 server error as overloaded', () => {
    assert.equal(classifyError(apiError(500)).overloaded, undefined);
    assert.equal(classifyError(apiError(502)).overloaded, undefined);
    assert.equal(classifyError('HTTP 500 internal server error').overloaded, undefined);
  });

  it('does NOT flag rate_limited (429) as overloaded', () => {
    assert.equal(classifyError('HTTP 429 Too Many Requests').overloaded, undefined);
  });
});
