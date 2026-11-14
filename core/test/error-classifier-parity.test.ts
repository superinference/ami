import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { APICallError } from 'ai';
import {
  classifyError,
  friendlyErrorMessage,
  sanitizeMessageHTML,
  extractNestedErrorMessage,
  normalizeErrorMessage,
  parsePromptTooLongTokenCounts,
  getPromptTooLongTokenGap,
  parseMaxTokensContextOverflow,
} from '../src/error-classifier';

function apiError(status: number, message: string): Error {
  return new APICallError({
    message,
    url: 'https://api.example.com',
    requestBodyValues: {},
    statusCode: status,
    responseHeaders: {},
    responseBody: message,
    isRetryable: false,
  });
}

describe('sanitizeMessageHTML', () => {
  it('extracts <title> from an HTML error page', () => {
    const html = '<!DOCTYPE html><html><head><title>502 Bad Gateway</title></head><body>...</body></html>';
    assert.equal(sanitizeMessageHTML(html), '502 Bad Gateway');
  });
  it('returns empty string for an HTML body without a title', () => {
    assert.equal(sanitizeMessageHTML('<html><body>nope</body></html>'), '');
  });
  it('passes plain text through unchanged', () => {
    assert.equal(sanitizeMessageHTML('rate limit exceeded'), 'rate limit exceeded');
  });
});

describe('extractNestedErrorMessage', () => {
  it('recovers the deep Anthropic shape error.error.error.message', () => {
    const err = { error: { error: { message: 'overloaded_error' } } };
    assert.equal(extractNestedErrorMessage(err), 'overloaded_error');
  });
  it('recovers the Bedrock shape error.error.message', () => {
    const err = { error: { message: 'throttled by bedrock' } };
    assert.equal(extractNestedErrorMessage(err), 'throttled by bedrock');
  });
  it('returns null for a non-nested object', () => {
    assert.equal(extractNestedErrorMessage({ foo: 'bar' }), null);
  });
  it('sanitizes HTML inside a nested message', () => {
    const err = { error: { message: '<!DOCTYPE html><title>Service Unavailable</title>' } };
    assert.equal(extractNestedErrorMessage(err), 'Service Unavailable');
  });
});

describe('normalizeErrorMessage', () => {
  it('reads a top-level string', () => {
    assert.equal(normalizeErrorMessage('plain'), 'plain');
  });
  it('reads Error.message', () => {
    assert.equal(normalizeErrorMessage(new Error('boom')), 'boom');
  });
  it('recovers a deserialized nested message with no top-level .message', () => {
    assert.equal(normalizeErrorMessage({ error: { error: { message: 'nested text' } } }), 'nested text');
  });
});

describe('classifyError — nested + HTML normalization', () => {
  it('classifies a nested rate-limit error that lost its top-level message', () => {
    const c = classifyError({ error: { error: { message: 'rate limit exceeded' } } });
    assert.equal(c.category, 'rate_limited');
  });
  it('cleans an HTML error page before surfacing it', () => {
    const c = classifyError('<!DOCTYPE html><html><title>504 Gateway Timeout</title></html>');
    assert.equal(c.message, '504 Gateway Timeout');
  });
});

describe('prompt-too-long token parsing', () => {
  it('parses actual/limit tokens leniently', () => {
    const r = parsePromptTooLongTokenCounts('prompt is too long: 137500 tokens > 135000 maximum');
    assert.equal(r.actualTokens, 137500);
    assert.equal(r.limitTokens, 135000);
  });
  it('computes the over-budget gap', () => {
    assert.equal(getPromptTooLongTokenGap('prompt is too long: 137500 tokens > 135000 maximum'), 2500);
  });
  it('returns undefined for a non-PTL message', () => {
    assert.equal(getPromptTooLongTokenGap('some other error'), undefined);
  });
  it('attaches tokenGap to a context_overflow classification', () => {
    const c = classifyError('prompt is too long: 200000 tokens > 180000 maximum');
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.tokenGap, 20000);
  });
});

describe('max_tokens context overflow parsing', () => {
  it('parses input + max_tokens > limit', () => {
    const r = parseMaxTokensContextOverflow('input length and `max_tokens` exceed context limit: 188059 + 20000 > 200000');
    assert.deepEqual(r, { inputTokens: 188059, maxTokens: 20000, contextLimit: 200000 });
  });
  it('returns null when it does not match', () => {
    assert.equal(parseMaxTokensContextOverflow('unrelated'), null);
  });
});

describe('status-code classification parity', () => {
  it('treats 404 as non-retryable model-not-found with /model guidance', () => {
    const c = classifyError(apiError(404, 'model: foo-bar does not exist'));
    assert.equal(c.retryable, false);
    assert.equal(c.shouldFallback, true);
    const friendly = friendlyErrorMessage(c);
    assert.match(friendly, /\/model/);
  });
  it('treats 408 (request timeout) as retryable', () => {
    const c = classifyError(apiError(408, 'request timeout'));
    assert.equal(c.retryable, true);
  });
  it('treats 409 (conflict/lock timeout) as retryable', () => {
    const c = classifyError(apiError(409, 'lock timeout'));
    assert.equal(c.retryable, true);
  });
  it('treats a 403 OAuth-revoked body as a retryable auth error', () => {
    const c = classifyError(apiError(403, 'OAuth token has been revoked'));
    assert.equal(c.category, 'auth_error');
    assert.equal(c.retryable, true);
  });
  it('still treats a plain 403 as non-retryable', () => {
    const c = classifyError(apiError(403, 'forbidden: no access'));
    assert.equal(c.retryable, false);
  });
});

describe('timeout env-var hint', () => {
  afterEach(() => { delete process.env.SUPERINFERENCE_API_TIMEOUT_MS; });
  it('names the timeout knob + value when set', () => {
    process.env.SUPERINFERENCE_API_TIMEOUT_MS = '30000';
    const c = classifyError('connection timed out');
    const friendly = friendlyErrorMessage(c);
    assert.match(friendly, /SUPERINFERENCE_API_TIMEOUT_MS=30000/);
    assert.match(friendly, /increas/i);
  });
  it('omits the knob hint when unset', () => {
    const c = classifyError('connection timed out');
    const friendly = friendlyErrorMessage(c);
    assert.ok(!friendly.includes('SUPERINFERENCE_API_TIMEOUT_MS'));
  });
});
