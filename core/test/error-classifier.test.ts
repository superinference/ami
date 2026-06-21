import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { classifyError, type ClassifiedError } from '../src/error-classifier';

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
  const expected = { category: 'content_filter' as const, retryable: true, shouldCompact: false, shouldFallback: false };

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
    const retryableCategories = ['rate_limited', 'context_overflow', 'output_too_large', 'content_filter', 'model_unavailable', 'server_error', 'network_error'];
    const triggers = ['HTTP 429 Too Many Requests', 'context too long', 'output too large', 'content_filter triggered', 'HTTP 503 Service Unavailable', 'HTTP 500 Internal Server Error', 'ECONNREFUSED'];

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
  });

  it('only context_overflow has shouldCompact=true', () => {
    const result = classifyError('context too long');
    assert.equal(result.shouldCompact, true);

    const others = ['HTTP 429', 'HTTP 401', 'HTTP 503', 'HTTP 500 Internal Server Error', 'ECONNREFUSED', 'abort', 'output too large', 'random'];
    for (const msg of others) {
      assert.equal(classifyError(msg).shouldCompact, false, `"${msg}" should not have shouldCompact`);
    }
  });

  it('model_unavailable and server_error have shouldFallback=true', () => {
    assert.equal(classifyError('HTTP 503 Service Unavailable').shouldFallback, true);
    assert.equal(classifyError('HTTP 500 Internal Server Error').shouldFallback, true);

    const noFallback = ['HTTP 429', 'HTTP 401', 'context too long', 'ECONNREFUSED', 'abort', 'random'];
    for (const msg of noFallback) {
      assert.equal(classifyError(msg).shouldFallback, false, `"${msg}" should not have shouldFallback`);
    }
  });

  it('bare status codes in context (port numbers, token counts) do not trigger false positives', () => {
    assert.equal(classifyError('Failed to connect to port 5000').category, 'unknown');
    assert.equal(classifyError('Used 50000 tokens').category, 'unknown');
    assert.equal(classifyError('Internal timeout after 3000ms').category, 'unknown');
  });
});
