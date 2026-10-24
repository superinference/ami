import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { APICallError, LoadAPIKeyError, NoSuchModelError, InvalidPromptError, NoContentGeneratedError, EmptyResponseBodyError } from 'ai';

import { classifyError, friendlyErrorMessage, type ClassifiedError } from '../src/error-classifier';

// ---------------------------------------------------------------------------
// Helper to assert the full shape of a classified error
// ---------------------------------------------------------------------------
function assertClassification(
  result: ClassifiedError,
  expected: {
    category: ClassifiedError['category'];
    retryable: boolean;
    shouldCompact: boolean;
    shouldFallback: boolean;
  },
) {
  assert.equal(result.category, expected.category);
  assert.equal(result.retryable, expected.retryable);
  assert.equal(result.shouldCompact, expected.shouldCompact);
  assert.equal(result.shouldFallback, expected.shouldFallback);
}

// ---------------------------------------------------------------------------
// Rate limited
// ---------------------------------------------------------------------------
describe('classifyError — rate_limited', () => {
  const expected = { category: 'rate_limited' as const, retryable: true, shouldCompact: false, shouldFallback: false };

  it('detects "429"', () => {
    assertClassification(classifyError('HTTP 429 Too Many Requests'), expected);
  });

  it('detects "rate limit"', () => {
    assertClassification(classifyError('You have hit the rate limit'), expected);
  });

  it('detects "quota"', () => {
    assertClassification(classifyError('Quota exceeded for this project'), expected);
  });

  it('has suggestedDelay', () => {
    const result = classifyError('rate limit');
    assert.equal(result.suggestedDelay, 5000);
  });
});

// ---------------------------------------------------------------------------
// Context overflow
// ---------------------------------------------------------------------------
describe('classifyError — context_overflow', () => {
  const expected = { category: 'context_overflow' as const, retryable: true, shouldCompact: true, shouldFallback: false };

  it('detects "context too long"', () => {
    assertClassification(classifyError('The context is too long for this model'), expected);
  });

  it('detects "HTTP 413"', () => {
    assertClassification(classifyError('HTTP 413 Request Entity Too Large'), expected);
  });

  it('detects "prompt_too_long"', () => {
    assertClassification(classifyError('Error: prompt_too_long'), expected);
  });

  it('detects "max_tokens"', () => {
    assertClassification(classifyError('max_tokens exceeded'), expected);
  });

  it('detects "context length"', () => {
    assertClassification(classifyError('exceeds context length limit'), expected);
  });
});

// ---------------------------------------------------------------------------
// Model unavailable
// ---------------------------------------------------------------------------
describe('classifyError — model_unavailable', () => {
  const expected = { category: 'model_unavailable' as const, retryable: true, shouldCompact: false, shouldFallback: true };

  it('detects "HTTP 503"', () => {
    assertClassification(classifyError('HTTP 503 Service Unavailable'), expected);
  });

  it('detects "HTTP 529"', () => {
    assertClassification(classifyError('HTTP 529 overloaded'), expected);
  });

  it('detects "overloaded"', () => {
    assertClassification(classifyError('The model is currently overloaded'), expected);
  });

  it('detects "capacity"', () => {
    assertClassification(classifyError('No capacity available'), expected);
  });

  it('has suggestedDelay', () => {
    const result = classifyError('HTTP 503 Service Unavailable');
    assert.equal(result.suggestedDelay, 10000);
  });
});

// ---------------------------------------------------------------------------
// Auth error
// ---------------------------------------------------------------------------
describe('classifyError — auth_error (permanent)', () => {
  const expected = { category: 'auth_error' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects "401"', () => {
    assertClassification(classifyError('HTTP 401 Unauthorized'), expected);
  });

  it('detects "API key"', () => {
    assertClassification(classifyError('Invalid API key provided'), expected);
  });

  it('detects "unauthorized"', () => {
    assertClassification(classifyError('Unauthorized access'), expected);
  });
});

describe('classifyError — auth_error (transient 403)', () => {
  const expected = { category: 'auth_error' as const, retryable: true, shouldCompact: false, shouldFallback: true };

  it('detects "403" as retryable', () => {
    assertClassification(classifyError('HTTP 403 Forbidden'), expected);
  });

  it('detects "forbidden" as retryable', () => {
    assertClassification(classifyError('Access forbidden'), expected);
  });
});

// ---------------------------------------------------------------------------
// Output too large
// ---------------------------------------------------------------------------
describe('classifyError — output_too_large', () => {
  const expected = { category: 'output_too_large' as const, retryable: true, shouldCompact: false, shouldFallback: false };

  it('detects "output too large"', () => {
    assertClassification(classifyError('The output is too large'), expected);
  });

  it('detects "max_output"', () => {
    assertClassification(classifyError('Error: max_output limit reached'), expected);
  });
});

// ---------------------------------------------------------------------------
// Content filter
// ---------------------------------------------------------------------------
describe('classifyError — content_filter', () => {
  const expected = { category: 'content_filter' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects "content_filter"', () => {
    assertClassification(classifyError('content_filter triggered'), expected);
  });

  it('detects "content_management"', () => {
    assertClassification(classifyError('content_management policy violation'), expected);
  });

  it('detects "content policy"', () => {
    assertClassification(classifyError('This request violates our content policy'), expected);
  });
});

// ---------------------------------------------------------------------------
// Server error
// ---------------------------------------------------------------------------
describe('classifyError — server_error', () => {
  const expected = { category: 'server_error' as const, retryable: true, shouldCompact: false, shouldFallback: true };

  it('detects "500"', () => {
    assertClassification(classifyError('HTTP 500 Internal Server Error'), expected);
  });

  it('detects "HTTP 502"', () => {
    assertClassification(classifyError('HTTP 502 Bad Gateway'), expected);
  });

  it('detects "internal server error"', () => {
    assertClassification(classifyError('Internal server error'), expected);
  });

  it('has suggestedDelay', () => {
    const result = classifyError('HTTP 500 Internal Server Error');
    assert.equal(result.suggestedDelay, 3000);
  });
});

// ---------------------------------------------------------------------------
// Network error
// ---------------------------------------------------------------------------
describe('classifyError — network_error', () => {
  const expected = { category: 'network_error' as const, retryable: true, shouldCompact: false, shouldFallback: false };

  it('detects "ECONNREFUSED"', () => {
    assertClassification(classifyError('connect ECONNREFUSED 127.0.0.1:443'), expected);
  });

  it('detects "ETIMEDOUT"', () => {
    assertClassification(classifyError('connect ETIMEDOUT'), expected);
  });

  it('detects "network"', () => {
    assertClassification(classifyError('Network error occurred'), expected);
  });

  it('has suggestedDelay', () => {
    const result = classifyError('ECONNREFUSED');
    assert.equal(result.suggestedDelay, 5000);
  });

  it('detects OPT-09 "Request timed out after Nms"', () => {
    assertClassification(classifyError('Request timed out after 300000ms'), expected);
  });

  it('detects OPT-09 "Request timed out after Nms (partial content received)"', () => {
    assertClassification(classifyError('Request timed out after 300000ms (partial content received)'), expected);
  });

  it('detects generic "timed out" message', () => {
    assertClassification(classifyError('Connection timed out'), expected);
  });

  it('does not false-positive on "Internal timeout" (no space in timeout)', () => {
    assert.equal(classifyError('Internal timeout after 3000ms').category, 'unknown');
  });

  it('detects "ECONNABORTED"', () => {
    assertClassification(classifyError('connect ECONNABORTED'), expected);
  });

  it('detects "ENOTFOUND" (DNS resolution failure)', () => {
    assertClassification(classifyError('getaddrinfo ENOTFOUND api.example.com'), expected);
  });

  it('detects "EHOSTUNREACH"', () => {
    assertClassification(classifyError('connect EHOSTUNREACH 10.0.0.1:443'), expected);
  });

  it('detects "ENETUNREACH"', () => {
    assertClassification(classifyError('connect ENETUNREACH 192.168.1.1:443 - Local (0.0.0.0:0)'), expected);
  });

  it('detects "EPIPE" (broken pipe)', () => {
    assertClassification(classifyError('write EPIPE'), expected);
  });

  it('detects "socket hang up"', () => {
    assertClassification(classifyError('socket hang up'), expected);
  });

  it('detects "ECONNRESET"', () => {
    assertClassification(classifyError('read ECONNRESET'), expected);
  });

  it('detects "fetch failed"', () => {
    assertClassification(classifyError('fetch failed'), expected);
  });

  it('detects DNS errors', () => {
    assertClassification(classifyError('DNS lookup failed for api.provider.com'), expected);
  });
});

// ---------------------------------------------------------------------------
// Abort
// ---------------------------------------------------------------------------
describe('classifyError — abort', () => {
  const expected = { category: 'abort' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects "abort"', () => {
    assertClassification(classifyError('The operation was aborted'), expected);
  });

  it('detects "AbortError"', () => {
    assertClassification(classifyError('AbortError: signal timed out'), expected);
  });

  it('detects "cancel"', () => {
    assertClassification(classifyError('Request cancelled by user'), expected);
  });
});

// ---------------------------------------------------------------------------
// Repetition
// ---------------------------------------------------------------------------
describe('classifyError — repetition', () => {
  const expected = { category: 'repetition' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects "Repetitive output detected"', () => {
    assertClassification(classifyError('Repetitive output detected'), expected);
  });

  it('detects "repetitive" in any context', () => {
    assertClassification(classifyError('Model produced repetitive tokens'), expected);
  });

  it('is not retryable', () => {
    assert.equal(classifyError('Repetitive output detected').retryable, false);
  });
});

// ---------------------------------------------------------------------------
// Unknown
// ---------------------------------------------------------------------------
describe('classifyError — unknown', () => {
  const expected = { category: 'unknown' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('classifies random text as unknown', () => {
    assertClassification(classifyError('something completely unexpected happened'), expected);
  });

  it('classifies empty string as unknown', () => {
    assertClassification(classifyError(''), expected);
  });
});

// ---------------------------------------------------------------------------
// Cross-cutting concerns
// ---------------------------------------------------------------------------
describe('classifyError — cross-cutting', () => {
  it('preserves the original error message', () => {
    const msg = 'HTTP 429 Too Many Requests — retry after 30s';
    const result = classifyError(msg);
    assert.equal(result.message, msg);
  });

  it('is case insensitive', () => {
    const lowerResult = classifyError('rate limit exceeded');
    const upperResult = classifyError('RATE LIMIT EXCEEDED');
    const mixedResult = classifyError('Rate Limit Exceeded');

    assert.equal(lowerResult.category, 'rate_limited');
    assert.equal(upperResult.category, 'rate_limited');
    assert.equal(mixedResult.category, 'rate_limited');
  });

  it('abort takes priority over other patterns', () => {
    // "abort" is checked first, even if the message also contains "500"
    const result = classifyError('aborted due to 500 error');
    assert.equal(result.category, 'abort');
  });

  it('auth takes priority over rate limit', () => {
    const result = classifyError('HTTP 401 rate limit');
    assert.equal(result.category, 'auth_error');
  });

  it('retryable categories have retryable=true', () => {
    const retryableCategories = ['rate_limited', 'context_overflow', 'output_too_large', 'model_unavailable', 'server_error', 'network_error'];
    const triggers = ['HTTP 429 Too Many Requests', 'context too long', 'output too large', 'HTTP 503 Service Unavailable', 'HTTP 500 Internal Server Error', 'ECONNREFUSED'];

    for (let i = 0; i < retryableCategories.length; i++) {
      const result = classifyError(triggers[i]);
      assert.equal(result.category, retryableCategories[i], `${triggers[i]} should be ${retryableCategories[i]}`);
      assert.equal(result.retryable, true, `${retryableCategories[i]} should be retryable`);
    }
  });

  it('non-retryable categories have retryable=false', () => {
    const result1 = classifyError('abort');
    assert.equal(result1.retryable, false);

    const result2 = classifyError('HTTP 401 Unauthorized');
    assert.equal(result2.retryable, false);

    const result3 = classifyError('completely random text');
    assert.equal(result3.retryable, false);

    const result4 = classifyError('Repetitive output detected');
    assert.equal(result4.retryable, false);

    const result5 = classifyError('content_filter triggered');
    assert.equal(result5.retryable, false, 'content_filter should not be retryable — policy decisions are not transient');
  });

  it('only context_overflow has shouldCompact=true', () => {
    const result = classifyError('context too long');
    assert.equal(result.shouldCompact, true);

    const others = ['HTTP 429', 'HTTP 401', 'HTTP 503', 'HTTP 500 Internal Server Error', 'ECONNREFUSED', 'abort', 'output too large', 'Repetitive output', 'random'];
    for (const msg of others) {
      assert.equal(classifyError(msg).shouldCompact, false, `"${msg}" should not have shouldCompact`);
    }
  });

  it('model_unavailable and server_error have shouldFallback=true', () => {
    assert.equal(classifyError('HTTP 503 Service Unavailable').shouldFallback, true);
    assert.equal(classifyError('HTTP 500 Internal Server Error').shouldFallback, true);

    const noFallback = ['HTTP 429', 'HTTP 401', 'context too long', 'ECONNREFUSED', 'abort', 'Repetitive output', 'random'];
    for (const msg of noFallback) {
      assert.equal(classifyError(msg).shouldFallback, false, `"${msg}" should not have shouldFallback`);
    }
  });

  it('bare status codes in context (port numbers, token counts) do not trigger false positives', () => {
    assert.equal(classifyError('Failed to connect to port 5000').category, 'unknown');
    assert.equal(classifyError('Used 50000 tokens').category, 'unknown');
    assert.equal(classifyError('Internal timeout after 3000ms').category, 'unknown');
  });

  it('403 with auth keywords classified as non-retryable auth_error', () => {
    const result = classifyError('HTTP 403 Forbidden - Invalid API key');
    assert.equal(result.category, 'auth_error');
    assert.equal(result.retryable, false);
  });

  it('max_tokens output limit error not misclassified as context_overflow', () => {
    const result = classifyError('max_tokens: 100000 > 8192, which is the maximum number of output tokens');
    assert.notEqual(result.category, 'context_overflow');
    assert.equal(result.shouldCompact, false);
  });
});

// ---------------------------------------------------------------------------
// friendlyErrorMessage — user-facing error messages (Gap #17)
// ---------------------------------------------------------------------------

describe('friendlyErrorMessage', () => {
  it('returns friendly message for rate_limited', () => {
    const classified = classifyError('HTTP 429 rate limit exceeded');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('Rate limited'));
    assert.ok(!msg.includes('HTTP 429'));
  });

  it('returns friendly message for context_overflow', () => {
    const classified = classifyError('context too long');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('compacting'));
    assert.ok(!msg.includes('context too long'));
  });

  it('returns friendly message for server_error', () => {
    const classified = classifyError('HTTP 500 internal server error');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('server error'));
  });

  it('returns friendly message for network_error', () => {
    const classified = classifyError('ECONNREFUSED 127.0.0.1:443');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('Network error'));
    assert.ok(!msg.includes('ECONNREFUSED'));
  });

  it('returns raw message for unknown errors', () => {
    const classified = classifyError('some weird unexpected error');
    const msg = friendlyErrorMessage(classified);
    assert.equal(msg, 'some weird unexpected error');
  });

  it('returns raw message for abort errors', () => {
    const classified = classifyError('AbortError: signal aborted');
    const msg = friendlyErrorMessage(classified);
    assert.equal(msg, 'AbortError: signal aborted');
  });

  it('returns friendly message for model_unavailable', () => {
    const classified = classifyError('HTTP 503 service overloaded');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('unavailable'));
  });

  it('returns friendly message for auth_error', () => {
    const classified = classifyError('HTTP 401 unauthorized');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('API key'));
  });

  it('returns friendly message for ssl_error', () => {
    const classified = classifyError('UNABLE_TO_VERIFY_LEAF_SIGNATURE');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('SSL') || msg.includes('certificate'));
  });
});

// ---------------------------------------------------------------------------
// SSL/TLS certificate errors
// ---------------------------------------------------------------------------
describe('classifyError — ssl_error', () => {
  const expected = { category: 'ssl_error' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects UNABLE_TO_VERIFY_LEAF_SIGNATURE', () => {
    assertClassification(classifyError('Error: UNABLE_TO_VERIFY_LEAF_SIGNATURE'), expected);
  });

  it('detects DEPTH_ZERO_SELF_SIGNED_CERT', () => {
    assertClassification(classifyError('Error: DEPTH_ZERO_SELF_SIGNED_CERT'), expected);
  });

  it('detects SELF_SIGNED_CERT_IN_CHAIN', () => {
    assertClassification(classifyError('Error: SELF_SIGNED_CERT_IN_CHAIN'), expected);
  });

  it('detects CERT_HAS_EXPIRED', () => {
    assertClassification(classifyError('Error: CERT_HAS_EXPIRED'), expected);
  });

  it('detects ERR_TLS_CERT_ALTNAME_INVALID', () => {
    assertClassification(classifyError('Error: ERR_TLS_CERT_ALTNAME_INVALID'), expected);
  });

  it('detects "certificate expired"', () => {
    assertClassification(classifyError('certificate has expired'), expected);
  });

  it('detects "self-signed certificate"', () => {
    assertClassification(classifyError('self-signed certificate in certificate chain'), expected);
  });

  it('detects "certificate verify failed"', () => {
    assertClassification(classifyError('unable to verify the first certificate'), expected);
  });

  it('is not retryable (config issue, not transient)', () => {
    const result = classifyError('UNABLE_TO_VERIFY_LEAF_SIGNATURE');
    assert.equal(result.retryable, false);
    assert.equal(result.shouldFallback, false);
  });
});

describe('plan_limit category', () => {
  const expected = { category: 'plan_limit' as const, retryable: false, shouldCompact: false, shouldFallback: false };

  it('detects billing limit', () => {
    assertClassification(classifyError('Your billing limit has been reached'), expected);
  });

  it('detects credit balance errors', () => {
    assertClassification(classifyError('Insufficient credit balance'), expected);
  });

  it('detects usage limit', () => {
    assertClassification(classifyError('usage limit exceeded for this plan'), expected);
  });

  it('detects plan limit', () => {
    assertClassification(classifyError('You have reached your plan limit'), expected);
  });

  it('detects spending limit', () => {
    assertClassification(classifyError('Monthly spending limit exceeded'), expected);
  });

  it('is not retryable (requires account action)', () => {
    const result = classifyError('Your billing limit has been reached');
    assert.equal(result.retryable, false);
    assert.equal(result.shouldFallback, false);
  });

  it('takes priority over rate_limit for billing-related 429s', () => {
    const result = classifyError('HTTP 429: billing limit exceeded');
    assert.equal(result.category, 'plan_limit');
    assert.equal(result.retryable, false);
  });
});

describe('server_error friendly message includes status page hint', () => {
  it('friendly message mentions status page', () => {
    const result = classifyError('HTTP 500 Internal Server Error');
    const msg = friendlyErrorMessage(result);
    assert.ok(msg.includes('status page'), 'Server error friendly message should mention status page');
  });
});

// ---------------------------------------------------------------------------
// SDK typed error classification — fast path via Error objects
// ---------------------------------------------------------------------------

function makeAPICallError(statusCode: number, message: string): APICallError {
  return new APICallError({
    message,
    url: 'https://api.example.com/v1/chat',
    requestBodyValues: {},
    statusCode,
    responseHeaders: {},
    responseBody: '',
  });
}

describe('classifyError — SDK APICallError fast path', () => {
  it('classifies 429 as rate_limited', () => {
    const err = makeAPICallError(429, 'Too Many Requests');
    const result = classifyError(err);
    assertClassification(result, { category: 'rate_limited', retryable: true, shouldCompact: false, shouldFallback: false });
    assert.equal(result.suggestedDelay, 5000);
  });

  it('classifies 401 as auth_error (non-retryable)', () => {
    const err = makeAPICallError(401, 'Invalid API key');
    const result = classifyError(err);
    assertClassification(result, { category: 'auth_error', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('classifies 403 as retryable auth_error', () => {
    const err = makeAPICallError(403, 'Forbidden');
    const result = classifyError(err);
    assertClassification(result, { category: 'auth_error', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 5000);
  });

  it('classifies 403 with api key message as non-retryable auth_error', () => {
    const err = makeAPICallError(403, 'Invalid API key');
    const result = classifyError(err);
    assertClassification(result, { category: 'auth_error', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('classifies 413 as context_overflow', () => {
    const err = makeAPICallError(413, 'Payload Too Large');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('classifies 400 with context message as context_overflow', () => {
    const err = makeAPICallError(400, 'This model maximum context length is 128000 tokens');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('classifies 400 with token message as context_overflow', () => {
    const err = makeAPICallError(400, 'prompt has too many tokens');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('classifies 400 without context/token keywords as unknown (falls through)', () => {
    const err = makeAPICallError(400, 'Bad Request: invalid JSON');
    const result = classifyError(err);
    assertClassification(result, { category: 'unknown', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('classifies 500 as server_error', () => {
    const err = makeAPICallError(500, 'Internal Server Error');
    const result = classifyError(err);
    assertClassification(result, { category: 'server_error', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 3000);
  });

  it('classifies 502 as server_error', () => {
    const err = makeAPICallError(502, 'Bad Gateway');
    const result = classifyError(err);
    assertClassification(result, { category: 'server_error', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 3000);
  });

  it('classifies 503 as model_unavailable', () => {
    const err = makeAPICallError(503, 'Service Unavailable');
    const result = classifyError(err);
    assertClassification(result, { category: 'model_unavailable', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 10000);
  });

  it('classifies 529 as model_unavailable', () => {
    const err = makeAPICallError(529, 'Overloaded');
    const result = classifyError(err);
    assertClassification(result, { category: 'model_unavailable', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 10000);
  });

  it('classifies billing limit with 429 status as plan_limit (not rate_limited)', () => {
    const err = makeAPICallError(429, 'Your billing limit has been exceeded');
    const result = classifyError(err);
    assertClassification(result, { category: 'plan_limit', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('classifies unknown status code via regex fallback', () => {
    const err = makeAPICallError(418, "I'm a teapot");
    const result = classifyError(err);
    assertClassification(result, { category: 'unknown', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('preserves the error message from APICallError', () => {
    const err = makeAPICallError(429, 'Rate limited by provider');
    const result = classifyError(err);
    assert.equal(result.message, 'Rate limited by provider');
  });
});

describe('classifyError — SDK LoadAPIKeyError', () => {
  it('classifies as auth_error', () => {
    const err = new LoadAPIKeyError({ message: 'API key not found in environment' });
    const result = classifyError(err);
    assertClassification(result, { category: 'auth_error', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('preserves the message', () => {
    const err = new LoadAPIKeyError({ message: 'ANTHROPIC_API_KEY not set' });
    const result = classifyError(err);
    assert.equal(result.message, 'ANTHROPIC_API_KEY not set');
  });
});

describe('classifyError — SDK NoSuchModelError', () => {
  it('classifies as model_unavailable with shouldFallback', () => {
    const err = new NoSuchModelError({ modelId: 'gpt-99', modelType: 'chat' });
    const result = classifyError(err);
    assertClassification(result, { category: 'model_unavailable', retryable: false, shouldCompact: false, shouldFallback: true });
  });
});

describe('classifyError — SDK InvalidPromptError', () => {
  it('classifies as context_overflow with shouldCompact', () => {
    const err = new InvalidPromptError({ prompt: [], reason: 'Prompt too long' });
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });
});

describe('classifyError — SDK NoContentGeneratedError', () => {
  it('classifies as server_error (retryable)', () => {
    const err = new NoContentGeneratedError({ message: 'Model returned empty response' });
    const result = classifyError(err);
    assertClassification(result, { category: 'server_error', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 3000);
  });
});

describe('classifyError — SDK EmptyResponseBodyError', () => {
  it('classifies as server_error (retryable)', () => {
    const err = new EmptyResponseBodyError({ url: 'https://api.example.com', message: 'Empty response' });
    const result = classifyError(err);
    assertClassification(result, { category: 'server_error', retryable: true, shouldCompact: false, shouldFallback: true });
    assert.equal(result.suggestedDelay, 3000);
  });
});

describe('classifyError — plain Error objects (non-SDK)', () => {
  it('classifies an Error with abort message via regex fallback', () => {
    const err = new Error('The operation was aborted');
    const result = classifyError(err);
    assertClassification(result, { category: 'abort', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('classifies an Error with rate limit message via regex fallback', () => {
    const err = new Error('rate limit exceeded');
    const result = classifyError(err);
    assertClassification(result, { category: 'rate_limited', retryable: true, shouldCompact: false, shouldFallback: false });
  });

  it('classifies an Error with network error message via regex fallback', () => {
    const err = new Error('connect ECONNREFUSED 127.0.0.1:443');
    const result = classifyError(err);
    assertClassification(result, { category: 'network_error', retryable: true, shouldCompact: false, shouldFallback: false });
  });

  it('classifies an Error with unknown message as unknown', () => {
    const err = new Error('something unexpected');
    const result = classifyError(err);
    assertClassification(result, { category: 'unknown', retryable: false, shouldCompact: false, shouldFallback: false });
  });

  it('preserves Error.message as the classified message', () => {
    const err = new Error('specific error text');
    const result = classifyError(err);
    assert.equal(result.message, 'specific error text');
  });

  it('AbortError name is caught by SDK fast path', () => {
    const err = new DOMException('signal aborted', 'AbortError');
    const result = classifyError(err);
    assertClassification(result, { category: 'abort', retryable: false, shouldCompact: false, shouldFallback: false });
  });
});

// ---------------------------------------------------------------------------
// P5 ERR-001: classifier tight pattern — false positive regression guard
// ---------------------------------------------------------------------------
describe('classifier tight pattern — false positive regression guard', () => {
  it('does not misclassify 400 "authentication token expired" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'authentication token expired');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"authentication token expired" must not trigger context_overflow — "token" alone is too broad');
    assert.equal(result.shouldCompact, false);
  });

  it('does not misclassify 400 "context_management: Extra inputs are not permitted" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'context_management: Extra inputs are not permitted');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"context_management: Extra inputs" must not trigger context_overflow — "context" alone is too broad');
    assert.equal(result.shouldCompact, false);
  });

  it('does not misclassify 400 "field value is too long" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'field value is too long');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"field value is too long" must not trigger context_overflow — bare "too long" is too broad');
    assert.equal(result.shouldCompact, false);
  });

  it('does not misclassify 400 "invalid context parameter" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'invalid context parameter');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"invalid context parameter" must not trigger context_overflow');
    assert.equal(result.shouldCompact, false);
  });

  it('does not misclassify 400 "token not found" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'token not found');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"token not found" must not trigger context_overflow');
    assert.equal(result.shouldCompact, false);
  });

  it('does not misclassify 400 "The session token is too long" as context_overflow (false positive)', () => {
    const err = makeAPICallError(400, 'The session token is too long');
    const result = classifyError(err);
    assert.notEqual(result.category, 'context_overflow',
      '"The session token is too long" must not trigger context_overflow');
    assert.equal(result.shouldCompact, false);
  });

  // TRUE POSITIVES — these must STILL be classified as context_overflow
  it('still classifies 400 "maximum context length exceeded" as context_overflow', () => {
    const err = makeAPICallError(400, 'maximum context length exceeded');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "token limit exceeded" as context_overflow', () => {
    const err = makeAPICallError(400, 'token limit exceeded');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "prompt is too long" as context_overflow', () => {
    const err = makeAPICallError(400, 'prompt is too long');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "input is too long" as context_overflow', () => {
    const err = makeAPICallError(400, 'input is too long');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "context_length_exceeded" as context_overflow', () => {
    const err = makeAPICallError(400, 'context_length_exceeded');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "reduce the length of the messages" as context_overflow', () => {
    const err = makeAPICallError(400, 'reduce the length of the messages');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });

  it('still classifies 400 "too many tokens in prompt" as context_overflow', () => {
    const err = makeAPICallError(400, 'too many tokens in prompt');
    const result = classifyError(err);
    assertClassification(result, { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false });
  });
});

// ---------------------------------------------------------------------------
// CC1: Gateway rejects effort beta header
// ---------------------------------------------------------------------------
describe('classifyError — gateway_rejection', () => {
  const expected = {
    category: 'gateway_rejection' as const,
    retryable: true,
    shouldCompact: false,
    shouldFallback: false,
  };

  it('detects "unsupported header anthropic-beta" (HTTP 400)', () => {
    const err = makeAPICallError(400, 'unsupported header anthropic-beta');
    assertClassification(classifyError(err), expected);
  });

  it('detects "unknown beta version effort-2025-11-24" (HTTP 400)', () => {
    const err = makeAPICallError(400, 'unknown beta version effort-2025-11-24');
    assertClassification(classifyError(err), expected);
  });

  it('detects "invalid header: anthropic-beta is not supported" (HTTP 400)', () => {
    const err = makeAPICallError(400, 'invalid header: anthropic-beta is not supported');
    assertClassification(classifyError(err), expected);
  });

  it('detects "unrecognized request header: anthropic-beta" (HTTP 400)', () => {
    const err = makeAPICallError(400, 'unrecognized request header: anthropic-beta');
    assertClassification(classifyError(err), expected);
  });

  it('detects gateway rejection via regex fallback (string)', () => {
    assertClassification(
      classifyError('HTTP 400: unsupported header anthropic-beta'),
      expected,
    );
  });

  it('detects gateway rejection from "unknown beta" in string form', () => {
    assertClassification(
      classifyError('HTTP 400: unknown beta version effort-2025-11-24'),
      expected,
    );
  });

  it('does not false-positive on regular 400 without gateway keywords', () => {
    const err = makeAPICallError(400, 'Bad Request: invalid JSON');
    const result = classifyError(err);
    assert.notEqual(result.category, 'gateway_rejection');
  });

  it('existing 400 context_overflow still classifies correctly (regression)', () => {
    const err = makeAPICallError(400, 'maximum context length exceeded');
    assertClassification(classifyError(err), {
      category: 'context_overflow',
      retryable: true,
      shouldCompact: true,
      shouldFallback: false,
    });
  });

  it('is retryable so the retry wrapper can rebuild without beta headers', () => {
    const err = makeAPICallError(400, 'unsupported header anthropic-beta');
    const result = classifyError(err);
    assert.equal(result.retryable, true);
    assert.equal(result.shouldFallback, false);
    assert.equal(result.shouldCompact, false);
  });

  it('has a friendly message distinct from the raw error', () => {
    const err = makeAPICallError(400, 'unsupported header anthropic-beta');
    const result = classifyError(err);
    const msg = friendlyErrorMessage(result);
    // The friendly message should NOT be the raw error text — it should be a
    // user-oriented explanation from the FRIENDLY_MESSAGES map
    assert.notEqual(msg, 'unsupported header anthropic-beta',
      'Friendly message should not be the raw error text');
    assert.ok(msg.includes('retry') || msg.includes('gateway') || msg.includes('beta header'),
      `Friendly message should mention retry/gateway/beta header: ${msg}`);
  });
});

describe('classifyError — Error vs string consistency', () => {
  it('produces same category for Error and string with same message', () => {
    const messages = [
      'HTTP 429 Too Many Requests',
      'HTTP 401 Unauthorized',
      'connect ECONNREFUSED',
      'context too long',
      'Repetitive output detected',
      'content_filter triggered',
      'HTTP 503 Service Unavailable',
      'HTTP 500 Internal Server Error',
    ];
    for (const msg of messages) {
      const fromString = classifyError(msg);
      const fromError = classifyError(new Error(msg));
      assert.equal(fromString.category, fromError.category,
        `Category mismatch for "${msg}": string=${fromString.category}, Error=${fromError.category}`);
      assert.equal(fromString.retryable, fromError.retryable,
        `Retryable mismatch for "${msg}"`);
    }
  });
});
