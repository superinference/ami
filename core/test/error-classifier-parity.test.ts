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
  isAbortError,
  getSSLErrorHint,
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

// ---------------------------------------------------------------------------
// isAbortError — minify-safe cancellation detection (F1773 / F0984)
// ---------------------------------------------------------------------------
describe('isAbortError', () => {
  it('detects an AbortError by name', () => {
    assert.equal(isAbortError({ name: 'AbortError' }), true);
  });
  it('detects a DOMException AbortError', () => {
    assert.equal(isAbortError(new DOMException('stream closed', 'AbortError')), true);
  });
  it('detects an aborted/cancelled message string', () => {
    assert.equal(isAbortError('The operation was aborted'), true);
    assert.equal(isAbortError('request cancelled by user'), true);
  });
  it('does NOT treat an ECONNABORTED network drop as an abort', () => {
    assert.equal(isAbortError('connect ECONNABORTED'), false);
  });
  it('returns false for unrelated errors and non-objects', () => {
    assert.equal(isAbortError('some weird error'), false);
    assert.equal(isAbortError(null), false);
    assert.equal(isAbortError(42), false);
    assert.equal(isAbortError({ foo: 'bar' }), false);
  });
  it('an APICallError whose body mentions ECONNABORTED is a server error, not an abort', () => {
    // Discriminates the new detector: the old `message.includes("abort")` SDK
    // check mis-classified ECONNABORTED as a (non-retryable) abort.
    const c = classifyError(apiError(500, 'upstream read ECONNABORTED'));
    assert.equal(c.category, 'server_error');
    assert.equal(c.retryable, true);
  });
});

// ---------------------------------------------------------------------------
// Generic 5xx / status fallback (F0166)
// ---------------------------------------------------------------------------
describe('generic 5xx retryable fallback', () => {
  it('classifies a 504 gateway-timeout APICallError as retryable server_error', () => {
    const c = classifyError(apiError(504, 'gateway timeout'));
    assert.equal(c.category, 'server_error');
    assert.equal(c.retryable, true);
    assert.equal(c.shouldFallback, true);
  });
  it('classifies a 507 APICallError as retryable server_error', () => {
    const c = classifyError(apiError(507, 'insufficient storage'));
    assert.equal(c.category, 'server_error');
    assert.equal(c.retryable, true);
  });
  it('keeps 503 as model_unavailable (explicit case wins over generic 5xx)', () => {
    const c = classifyError(apiError(503, 'service overloaded'));
    assert.equal(c.category, 'model_unavailable');
  });
  it('classifies a bare "HTTP 504" string as retryable server_error', () => {
    const c = classifyError('HTTP 504 Gateway Timeout');
    assert.equal(c.category, 'server_error');
    assert.equal(c.retryable, true);
  });
});

// ---------------------------------------------------------------------------
// SSL/TLS per-code classification + actionable hints (F1273 / F0166)
// ---------------------------------------------------------------------------
describe('SSL per-code classification', () => {
  it('classifies CERT_NOT_YET_VALID as ssl_error', () => {
    assert.equal(classifyError('Error: CERT_NOT_YET_VALID').category, 'ssl_error');
  });
  it('classifies CERT_REVOKED as ssl_error', () => {
    assert.equal(classifyError('Error: CERT_REVOKED').category, 'ssl_error');
  });
  it('classifies "unable to get local issuer certificate" as ssl_error', () => {
    assert.equal(classifyError('unable to get local issuer certificate').category, 'ssl_error');
  });
  it('does NOT misclassify an OAuth revoked-token message as ssl_error', () => {
    // "revoked" alone must not trip SSL — it needs "certificate" nearby.
    assert.notEqual(classifyError('oauth token has been revoked').category, 'ssl_error');
  });
});

describe('getSSLErrorHint — per-code actionable guidance', () => {
  it('expired cert', () => {
    assert.match(getSSLErrorHint('CERT_HAS_EXPIRED'), /expired/i);
    assert.match(getSSLErrorHint('CERT_HAS_EXPIRED'), /certificate/i);
  });
  it('not-yet-valid cert mentions the clock', () => {
    assert.match(getSSLErrorHint('CERT_NOT_YET_VALID'), /not yet valid/i);
    assert.match(getSSLErrorHint('CERT_NOT_YET_VALID'), /clock/i);
  });
  it('revoked cert', () => {
    assert.match(getSSLErrorHint('CERT_REVOKED'), /revoked/i);
  });
  it('hostname mismatch', () => {
    assert.match(getSSLErrorHint('ERR_TLS_CERT_ALTNAME_INVALID'), /hostname/i);
  });
  it('self-signed recommends NODE_EXTRA_CA_CERTS', () => {
    assert.match(getSSLErrorHint('DEPTH_ZERO_SELF_SIGNED_CERT'), /NODE_EXTRA_CA_CERTS/);
  });
  it('unverifiable chain recommends NODE_EXTRA_CA_CERTS', () => {
    assert.match(getSSLErrorHint('unable to get local issuer certificate'), /NODE_EXTRA_CA_CERTS/);
  });
  it('friendlyErrorMessage routes ssl_error through the per-code hint', () => {
    const friendly = friendlyErrorMessage(classifyError('CERT_HAS_EXPIRED'));
    assert.match(friendly, /expired/i);
  });
});

// ---------------------------------------------------------------------------
// Actionable context-overflow + bare-error fallback messages (F0738 / F1272)
// ---------------------------------------------------------------------------
describe('actionable friendly messages', () => {
  it('context_overflow names the recovery commands', () => {
    const friendly = friendlyErrorMessage(classifyError('context too long'));
    assert.match(friendly, /\/compact/);
    assert.match(friendly, /\/clear/);
  });
  it('a bare/empty error body gets an actionable retry fallback', () => {
    const friendly = friendlyErrorMessage(classifyError(''));
    assert.notEqual(friendly.trim().length, 0);
    assert.match(friendly, /try again/i);
  });
});
