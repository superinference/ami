/**
 * POV: core/core/src/error-classifier.ts — full behavior matrix.
 *
 * Drives the REAL classifyError / friendlyErrorMessage / isGatewayRejection
 * / networkErrorHint across the full status-code grid (via genuine `ai` SDK
 * APICallError instances) and the message-based regex grid, asserting the
 * complete recovery shape (category + retryable + shouldCompact +
 * shouldFallback + suggestedDelay) each time. No source-text assertions —
 * every expectation is produced by calling the real function.
 *
 * Also re-locks the #34 invariant: auth errors (401/403, "api key",
 * "forbidden", "authentication") are NEVER retryable.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  APICallError,
  LoadAPIKeyError,
  NoSuchModelError,
  InvalidPromptError,
  NoContentGeneratedError,
  EmptyResponseBodyError,
} from 'ai';

import {
  classifyError,
  friendlyErrorMessage,
  networkErrorHint,
  isGatewayRejection,
  ERROR_CATEGORIES,
  type ClassifiedError,
  type ErrorCategory,
} from '../src/error-classifier';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface Shape {
  category: ErrorCategory;
  retryable: boolean;
  shouldCompact: boolean;
  shouldFallback: boolean;
  suggestedDelay?: number;
}

function assertShape(result: ClassifiedError, expected: Shape, label: string) {
  assert.equal(result.category, expected.category, `${label}: category`);
  assert.equal(result.retryable, expected.retryable, `${label}: retryable`);
  assert.equal(result.shouldCompact, expected.shouldCompact, `${label}: shouldCompact`);
  assert.equal(result.shouldFallback, expected.shouldFallback, `${label}: shouldFallback`);
  assert.equal(result.suggestedDelay, expected.suggestedDelay, `${label}: suggestedDelay`);
}

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

// Canonical shapes (single source of truth for the taxonomy under test)
const S = {
  auth: { category: 'auth_error', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  rate: { category: 'rate_limited', retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 } as Shape,
  ctx: { category: 'context_overflow', retryable: true, shouldCompact: true, shouldFallback: false } as Shape,
  server: { category: 'server_error', retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 } as Shape,
  modelUnavail: { category: 'model_unavailable', retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000 } as Shape,
  planLimit: { category: 'plan_limit', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  gateway: { category: 'gateway_rejection', retryable: true, shouldCompact: false, shouldFallback: false } as Shape,
  content: { category: 'content_filter', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  output: { category: 'output_too_large', retryable: true, shouldCompact: false, shouldFallback: false } as Shape,
  ssl: { category: 'ssl_error', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  network: { category: 'network_error', retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 } as Shape,
  repetition: { category: 'repetition', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  abort: { category: 'abort', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
  unknown: { category: 'unknown', retryable: false, shouldCompact: false, shouldFallback: false } as Shape,
};

// ---------------------------------------------------------------------------
// Status-code matrix via genuine SDK APICallError
// ---------------------------------------------------------------------------
describe('classifyError — APICallError status-code matrix', () => {
  const grid: Array<[number, string, Shape]> = [
    [401, 'Unauthorized', S.auth],
    [403, 'Forbidden', S.auth],
    [403, 'Project does not have access to this model', S.auth],
    [429, 'Too Many Requests', S.rate],
    [413, 'Payload Too Large', S.ctx],
    [500, 'Internal Server Error', S.server],
    [502, 'Bad Gateway', S.server],
    [503, 'Service Unavailable', S.modelUnavail],
    [529, 'Overloaded', S.modelUnavail],
  ];

  for (const [status, msg, shape] of grid) {
    it(`status ${status} "${msg}" → ${shape.category}`, () => {
      assertShape(classifyError(makeAPICallError(status, msg)), shape, `${status}`);
    });
  }

  it('429 preserves the original message verbatim', () => {
    assert.equal(classifyError(makeAPICallError(429, 'slow down please')).message, 'slow down please');
  });

  it('413 carries no suggestedDelay', () => {
    assert.equal(classifyError(makeAPICallError(413, 'too big')).suggestedDelay, undefined);
  });
});

// ---------------------------------------------------------------------------
// 400 is ambiguous: SDK path returns null and defers to the message regex
// ---------------------------------------------------------------------------
describe('classifyError — 400 defers to message content', () => {
  it('400 generic "Bad Request" → unknown (no retry)', () => {
    assertShape(classifyError(makeAPICallError(400, 'Bad Request')), S.unknown, '400-generic');
  });

  it('400 with context-length wording → context_overflow + compact', () => {
    assertShape(
      classifyError(makeAPICallError(400, 'This model maximum context length is 128000 tokens')),
      S.ctx,
      '400-ctx',
    );
  });

  it('400 "prompt is too long" → context_overflow', () => {
    assertShape(classifyError(makeAPICallError(400, 'prompt is too long for this request')), S.ctx, '400-prompt');
  });

  it('400 gateway beta rejection → gateway_rejection (retry without header)', () => {
    assertShape(
      classifyError(makeAPICallError(400, 'unsupported anthropic-beta header value')),
      S.gateway,
      '400-gateway',
    );
  });

  it('400 "authentication token expired" → auth_error, NOT context_overflow (#34)', () => {
    const r = classifyError(makeAPICallError(400, 'authentication token expired'));
    assertShape(r, S.auth, '400-auth-token');
    assert.equal(r.retryable, false);
    assert.equal(r.shouldCompact, false);
  });

  it('400 "field value is too long" does NOT falsely compact', () => {
    const r = classifyError(makeAPICallError(400, 'field value is too long'));
    assert.notEqual(r.category, 'context_overflow');
    assert.equal(r.shouldCompact, false);
  });
});

// ---------------------------------------------------------------------------
// Billing/plan limit outranks a 429 rate-limit status
// ---------------------------------------------------------------------------
describe('classifyError — plan_limit beats rate_limited on billing wording', () => {
  const cases = ['Your billing limit has been exceeded', 'Insufficient credit balance', 'monthly usage limit reached', 'spending limit hit', 'budget exceeded for org'];
  for (const msg of cases) {
    it(`429 "${msg}" → plan_limit (non-retryable)`, () => {
      assertShape(classifyError(makeAPICallError(429, msg)), S.planLimit, msg);
    });
  }

  it('string "credit exhausted" → plan_limit', () => {
    assertShape(classifyError('credit exhausted on account'), S.planLimit, 'credit');
  });
});

// ---------------------------------------------------------------------------
// Other SDK typed errors (no status code)
// ---------------------------------------------------------------------------
describe('classifyError — other SDK error classes', () => {
  it('LoadAPIKeyError → auth_error', () => {
    assertShape(classifyError(new LoadAPIKeyError({ message: 'API key not found' })), S.auth, 'loadkey');
  });

  it('NoSuchModelError → model_unavailable (fallback, no delay, no retry)', () => {
    const r = classifyError(new NoSuchModelError({ modelId: 'ghost-9', modelType: 'languageModel' }));
    assert.equal(r.category, 'model_unavailable');
    assert.equal(r.retryable, false);
    assert.equal(r.shouldFallback, true);
    assert.equal(r.suggestedDelay, undefined);
  });

  it('InvalidPromptError → context_overflow (compact + retry)', () => {
    const r = classifyError(new InvalidPromptError({ prompt: [], reason: 'bad prompt' }));
    assert.equal(r.category, 'context_overflow');
    assert.equal(r.shouldCompact, true);
    assert.equal(r.retryable, true);
  });

  it('NoContentGeneratedError → server_error', () => {
    assertShape(classifyError(new NoContentGeneratedError({ message: 'empty' })), S.server, 'nocontent');
  });

  it('EmptyResponseBodyError → server_error', () => {
    assertShape(
      classifyError(new EmptyResponseBodyError({ url: 'https://api.example.com', message: 'empty body' })),
      S.server,
      'emptybody',
    );
  });
});

// ---------------------------------------------------------------------------
// Message-based regex matrix (string inputs)
// ---------------------------------------------------------------------------
describe('classifyError — message regex matrix', () => {
  const grid: Array<[string, Shape]> = [
    // abort
    ['The operation was aborted by the user', S.abort],
    ['Request cancelled', S.abort],
    // auth
    ['HTTP 401 - invalid credentials', S.auth],
    ['Your api key is invalid', S.auth],
    ['unauthorized access', S.auth],
    ['invalid_api_key supplied', S.auth],
    ['authentication failed', S.auth],
    ['HTTP 403 forbidden resource', S.auth],
    // plan
    ['billing account is past due', S.planLimit],
    // gateway
    ['unknown beta version requested', S.gateway],
    // rate
    ['HTTP 429 slow down', S.rate],
    ['you are being throttled', S.rate],
    ['quota exceeded', S.rate],
    ['too many requests in a row', S.rate],
    // context
    ['the context is too long', S.ctx],
    ['HTTP 413 entity too large', S.ctx],
    ['prompt_too_long error', S.ctx],
    ['context length exceeded', S.ctx],
    ['please reduce the length of your input', S.ctx],
    // output
    ['output too large to return', S.output],
    ['max_output tokens exceeded', S.output],
    // content filter
    ['content_filter triggered on response', S.content],
    ['blocked by content policy', S.content],
    // model unavailable
    ['HTTP 503 service down', S.modelUnavail],
    ['the model is overloaded right now', S.modelUnavail],
    ['provider at capacity', S.modelUnavail],
    ['model_not_found for id', S.modelUnavail],
    // server error
    ['HTTP 500 - something broke', S.server],
    ['500 internal error on backend', S.server],
    ['internal server error occurred', S.server],
    ['HTTP 502 upstream', S.server],
    // ssl
    ['UNABLE_TO_VERIFY_LEAF_SIGNATURE', S.ssl],
    ['CERT_HAS_EXPIRED', S.ssl],
    ['certificate has expired', S.ssl],
    // network
    ['ECONNREFUSED on connect', S.network],
    ['ETIMEDOUT reaching host', S.network],
    ['fetch failed', S.network],
    ['socket hang up', S.network],
    ['DNS resolution error', S.network],
    // repetition
    ['repetitive output detected', S.repetition],
    // unknown
    ['something totally unexpected', S.unknown],
    ['', S.unknown],
  ];

  for (const [msg, shape] of grid) {
    it(`"${msg}" → ${shape.category}`, () => {
      assertShape(classifyError(msg), shape, msg || '<empty>');
    });
  }
});

// ---------------------------------------------------------------------------
// ECONNABORTED must be a network error, not a user abort
// ---------------------------------------------------------------------------
describe('classifyError — abort vs ECONNABORTED disambiguation', () => {
  it('ECONNABORTED → network_error (not abort)', () => {
    assertShape(classifyError('ECONNABORTED connection aborted'), S.network, 'econnaborted');
  });

  it('AbortError name on a plain Error → abort', () => {
    const e = new Error('stream closed');
    e.name = 'AbortError';
    assertShape(classifyError(e), S.abort, 'abortname');
  });
});

// ---------------------------------------------------------------------------
// #34 invariant: every auth-flavored input is non-retryable
// ---------------------------------------------------------------------------
describe('classifyError — auth is never retryable (#34)', () => {
  const inputs: Array<string | Error> = [
    makeAPICallError(401, 'Unauthorized'),
    makeAPICallError(403, 'Forbidden'),
    makeAPICallError(403, 'no access to this model'),
    'HTTP 401 bad key',
    'HTTP 403 forbidden',
    'your api key is wrong',
    'authentication token expired',
    new LoadAPIKeyError({ message: 'key missing' }),
  ];
  for (const inp of inputs) {
    const label = inp instanceof Error ? `${inp.name}:${inp.message}` : inp;
    it(`"${label}" → auth_error, retryable=false`, () => {
      const r = classifyError(inp);
      assert.equal(r.category, 'auth_error');
      assert.equal(r.retryable, false);
      assert.equal(r.shouldCompact, false);
      assert.equal(r.shouldFallback, false);
    });
  }
});

// ---------------------------------------------------------------------------
// Every classification yields a category within ERROR_CATEGORIES
// ---------------------------------------------------------------------------
describe('classifyError — category membership + message preservation', () => {
  const samples = [
    'HTTP 429', 'context too long', 'ECONNREFUSED', 'content policy', 'overloaded',
    'HTTP 500 internal', 'CERT_HAS_EXPIRED', 'repetitive', 'aborted', 'random', 'api key',
  ];
  for (const s of samples) {
    it(`"${s}" category is a known ERROR_CATEGORY`, () => {
      const r = classifyError(s);
      assert.ok(ERROR_CATEGORIES.includes(r.category), `${r.category} not in ERROR_CATEGORIES`);
      assert.equal(r.message, s);
    });
  }

  it('ERROR_CATEGORIES has no duplicates and includes the full taxonomy', () => {
    assert.equal(new Set(ERROR_CATEGORIES).size, ERROR_CATEGORIES.length);
    for (const c of ['rate_limited', 'auth_error', 'context_overflow', 'server_error', 'model_unavailable', 'unknown'] as const) {
      assert.ok(ERROR_CATEGORIES.includes(c));
    }
  });
});

// ---------------------------------------------------------------------------
// isGatewayRejection matrix
// ---------------------------------------------------------------------------
describe('isGatewayRejection', () => {
  const truthy = [
    'unsupported anthropic-beta header',
    'anthropic-beta not recognized by gateway',
    'anthropic-beta is unknown',
    'invalid anthropic-beta',
    'unknown beta version',
    'unsupported beta',
    'unrecognized beta flag',
    'invalid beta channel',
    'invalid header beta value',
    'unrecognized header for beta',
  ];
  const falsy = [
    '',
    'rate limit exceeded',
    'anthropic-beta header accepted',
    'beta program full', // "beta" alone, no reject verb
    'unsupported media type', // no beta/header
    'unsupported header', // header reject verb but no "beta"
  ];

  for (const m of truthy) {
    it(`"${m}" → true`, () => assert.equal(isGatewayRejection(m), true));
  }
  for (const m of falsy) {
    it(`"${m}" → false`, () => assert.equal(isGatewayRejection(m.toLowerCase()), false));
  }
});

// ---------------------------------------------------------------------------
// friendlyErrorMessage
// ---------------------------------------------------------------------------
describe('friendlyErrorMessage', () => {
  it('maps mapped categories to their human-readable friendly string', () => {
    const cases: Array<[string, string]> = [
      ['you hit the rate limit', 'Rate limited by the provider — retrying after a delay.'],
      ['billing limit exceeded', 'Plan or billing limit reached — check your account usage and billing settings.'],
      ['context too long', 'Response context too large — compacting conversation.'],
      ['the model is overloaded', 'Model temporarily unavailable — retrying.'],
      ['invalid api key', 'Authentication error — check your API key.'],
      ['output too large', 'Model output exceeded size limit — retrying with smaller output.'],
      ['content policy violation', 'Response blocked by content policy.'],
      ['CERT_HAS_EXPIRED', 'SSL/TLS certificate error — check your provider URL or set NODE_TLS_REJECT_UNAUTHORIZED=0 for self-signed certs.'],
      ['HTTP 500 internal server error', 'Provider server error — retrying. Check provider status page if this persists.'],
      ['repetitive output', 'Repetitive output detected.'],
      ['unsupported anthropic-beta header', 'Gateway rejected beta header — retrying without it.'],
    ];
    for (const [raw, friendly] of cases) {
      assert.equal(friendlyErrorMessage(classifyError(raw)), friendly, raw);
    }
  });

  it('unknown returns the raw message unchanged', () => {
    const c = classifyError('weird opaque failure xyz');
    assert.equal(friendlyErrorMessage(c), 'weird opaque failure xyz');
  });

  it('abort returns the raw message unchanged', () => {
    const c = classifyError('operation was aborted');
    assert.equal(friendlyErrorMessage(c), 'operation was aborted');
  });

  it('network_error routes through networkErrorHint', () => {
    const c = classifyError('ECONNREFUSED');
    assert.equal(c.category, 'network_error');
    assert.equal(friendlyErrorMessage(c), networkErrorHint(c.message.toLowerCase()));
  });

  it('DNS network error gives the DNS-specific hint', () => {
    const c = classifyError('getaddrinfo ENOTFOUND api.example.com');
    assert.equal(friendlyErrorMessage(c), networkErrorHint(c.message.toLowerCase()));
    assert.match(friendlyErrorMessage(c), /DNS lookup failed/);
  });

  it('OpenRouter ZDR-violation wording surfaces ZDR guidance over the category fallback', () => {
    const c: ClassifiedError = {
      category: 'model_unavailable',
      message: 'No endpoints found matching your data policy (ZDR)',
      retryable: true,
      shouldCompact: false,
      shouldFallback: true,
    };
    const out = friendlyErrorMessage(c);
    assert.match(out, /Zero-Data-Retention/);
    assert.notEqual(out, 'Model temporarily unavailable — retrying.');
  });
});

// ---------------------------------------------------------------------------
// networkErrorHint variants
// ---------------------------------------------------------------------------
describe('networkErrorHint', () => {
  it('DNS signatures → DNS lead', () => {
    for (const m of ['enotfound host', 'getaddrinfo failed', 'dns error']) {
      assert.match(networkErrorHint(m), /DNS lookup failed/);
    }
  });
  it('timeout signatures → timeout lead', () => {
    for (const m of ['etimedout', 'request timed out']) {
      assert.match(networkErrorHint(m), /connection timed out/);
    }
  });
  it('generic → generic lead, always naming proxy/gateway first', () => {
    const out = networkErrorHint('econnreset by peer');
    assert.match(out, /proxy\/gateway first, then VPN\/firewall/);
  });
});
