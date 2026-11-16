/**
 * U073-w69 — error-classifier parity additions:
 *  - getRefusalMessage (stop_reason 'refusal' / content-filter AUP notice)
 *  - suggestFallbackModel (per-family fallback suggestion)
 *  - media-size detection + typed PDF/image/request-too-large messages + constants
 *  - ClassifiedError.actualTokens / limitTokens (structured overflow counts)
 *  - friendlyErrorMessage: media interception, fallback-model naming, overflow counts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { APICallError } from 'ai';
import {
  classifyError,
  friendlyErrorMessage,
  getRefusalMessage,
  suggestFallbackModel,
  isMediaSizeError,
  getMediaSizeErrorMessage,
  getPdfTooLargeErrorMessage,
  getPdfPasswordProtectedErrorMessage,
  getPdfInvalidErrorMessage,
  getImageTooLargeErrorMessage,
  getRequestTooLargeErrorMessage,
  AUP_POLICY_URL,
  REFUSAL_FALLBACK_MODEL,
} from '../src/error-classifier';
import { API_PDF_MAX_PAGES, PDF_TARGET_RAW_SIZE } from '../src/media-limits';

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

// ---------------------------------------------------------------------------
// Refusal / content-filter (AUP) message
// ---------------------------------------------------------------------------
describe('getRefusalMessage', () => {
  it('interactive wording invites editing the message / new session', () => {
    const msg = getRefusalMessage('some-other-model', false);
    assert.match(msg, /usage policy/);
    assert.ok(msg.includes(AUP_POLICY_URL));
    assert.match(msg, /Edit your last message or start a new session/);
  });

  it('non-interactive wording tells the user to rephrase (no esc affordance)', () => {
    const msg = getRefusalMessage('some-other-model', true);
    assert.match(msg, /Rephrase the request or try a different approach/);
    assert.ok(!msg.includes('Edit your last message'));
  });

  it('names a /model switch when not already on the fallback model', () => {
    const msg = getRefusalMessage('claude-opus-4-6', false);
    assert.ok(msg.includes(`/model ${REFUSAL_FALLBACK_MODEL}`));
  });

  it('omits the switch affordance when already on the fallback model', () => {
    const msg = getRefusalMessage(REFUSAL_FALLBACK_MODEL, false);
    assert.ok(!msg.includes('/model'));
  });
});

// ---------------------------------------------------------------------------
// Per-model-family fallback suggestion
// ---------------------------------------------------------------------------
describe('suggestFallbackModel', () => {
  it('opus 4.6 → opus 4.1', () => {
    assert.equal(suggestFallbackModel('claude-opus-4-6-20991231'), 'claude-opus-4-1');
  });
  it('sonnet 4.6 → sonnet 4.5', () => {
    assert.equal(suggestFallbackModel('claude-sonnet-4-6'), 'claude-sonnet-4-5');
  });
  it('sonnet 4.5 → sonnet 4', () => {
    assert.equal(suggestFallbackModel('claude-sonnet-4-5-20250929'), 'claude-sonnet-4');
  });
  it('opus 4.5 → opus 4.1', () => {
    assert.equal(suggestFallbackModel('claude-opus-4-5'), 'claude-opus-4-1');
  });
  it('underscore variant is matched too', () => {
    assert.equal(suggestFallbackModel('claude_sonnet_4_6'), 'claude-sonnet-4-5');
  });
  it('unknown family → undefined', () => {
    assert.equal(suggestFallbackModel('gpt-4o'), undefined);
  });
  it('non-string → undefined', () => {
    assert.equal(suggestFallbackModel(undefined as unknown as string), undefined);
  });
});

// ---------------------------------------------------------------------------
// Media-size detection + typed messages
// ---------------------------------------------------------------------------
describe('isMediaSizeError', () => {
  it('detects image-exceeds-maximum', () => {
    assert.ok(isMediaSizeError('image exceeds the 5MB maximum'));
  });
  it('detects image dimensions exceed', () => {
    assert.ok(isMediaSizeError('image dimensions exceed 8000px'));
  });
  it('detects PDF page-limit', () => {
    assert.ok(isMediaSizeError('document has a maximum of 100 PDF pages'));
  });
  it('detects password-protected PDF', () => {
    assert.ok(isMediaSizeError('This PDF is password protected'));
  });
  it('detects invalid PDF', () => {
    assert.ok(isMediaSizeError('The PDF is invalid'));
  });
  it('detects request too large', () => {
    assert.ok(isMediaSizeError('request too large'));
  });
  it('returns false for an unrelated error', () => {
    assert.ok(!isMediaSizeError('rate limit exceeded'));
  });
  it('returns false for a non-string', () => {
    assert.ok(!isMediaSizeError(undefined as unknown as string));
  });
});

describe('typed media messages honor interactivity', () => {
  it('PDF-too-large names the page + size limits', () => {
    const m = getPdfTooLargeErrorMessage(false);
    assert.ok(m.includes(`max ${API_PDF_MAX_PAGES} pages`));
    assert.ok(m.includes('20MB'));
    assert.match(m, /Edit your message/);
    assert.match(getPdfTooLargeErrorMessage(true), /pdftotext/);
  });
  it('request-too-large names the size limit', () => {
    assert.ok(getRequestTooLargeErrorMessage(false).includes('20MB'));
    assert.match(getRequestTooLargeErrorMessage(true), /smaller file/);
  });
  it('password / invalid / image variants differ', () => {
    assert.match(getPdfPasswordProtectedErrorMessage(false), /password protected/);
    assert.match(getPdfInvalidErrorMessage(false), /not valid/);
    assert.match(getImageTooLargeErrorMessage(false), /Image was too large/);
  });
});

describe('getMediaSizeErrorMessage dispatch', () => {
  it('password beats the generic PDF-too-large branch', () => {
    assert.equal(
      getMediaSizeErrorMessage('PDF is password protected'),
      getPdfPasswordProtectedErrorMessage(false),
    );
  });
  it('invalid PDF routes to the invalid message', () => {
    assert.equal(getMediaSizeErrorMessage('PDF is invalid'), getPdfInvalidErrorMessage(false));
  });
  it('page-limit routes to PDF-too-large', () => {
    assert.equal(
      getMediaSizeErrorMessage('maximum of 100 PDF pages'),
      getPdfTooLargeErrorMessage(false),
    );
  });
  it('image routes to image-too-large', () => {
    assert.equal(
      getMediaSizeErrorMessage('image exceeds the maximum'),
      getImageTooLargeErrorMessage(false),
    );
  });
  it('request-too-large is the residual branch', () => {
    assert.equal(getMediaSizeErrorMessage('request too large'), getRequestTooLargeErrorMessage(false));
  });
  it('returns undefined for a non-media error', () => {
    assert.equal(getMediaSizeErrorMessage('rate limit exceeded'), undefined);
  });
  it('honors non-interactive', () => {
    assert.equal(
      getMediaSizeErrorMessage('image exceeds the maximum', true),
      getImageTooLargeErrorMessage(true),
    );
  });
});

describe('media-limits constants', () => {
  it('match the Claude Code API limits', () => {
    assert.equal(API_PDF_MAX_PAGES, 100);
    assert.equal(PDF_TARGET_RAW_SIZE, 20 * 1024 * 1024);
  });
});

// ---------------------------------------------------------------------------
// Structured overflow token fields
// ---------------------------------------------------------------------------
describe('classifyError — context_overflow structured counts', () => {
  it('populates actualTokens / limitTokens / tokenGap from a 400 overflow', () => {
    const c = classifyError(apiError(400, 'prompt is too long: 137500 tokens > 135000 maximum'));
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.actualTokens, 137500);
    assert.equal(c.limitTokens, 135000);
    assert.equal(c.tokenGap, 2500);
  });
  it('leaves the fields unset when counts are not parseable', () => {
    const c = classifyError('context too long');
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.actualTokens, undefined);
    assert.equal(c.limitTokens, undefined);
  });
});

// ---------------------------------------------------------------------------
// friendlyErrorMessage integration
// ---------------------------------------------------------------------------
describe('friendlyErrorMessage — new branches', () => {
  it('intercepts a media-size error with the typed message', () => {
    const c = classifyError('PDF is password protected');
    assert.equal(friendlyErrorMessage(c), getPdfPasswordProtectedErrorMessage(false));
  });

  it('media interception honors nonInteractive opt', () => {
    const c = classifyError('maximum of 100 PDF pages');
    assert.equal(friendlyErrorMessage(c, { nonInteractive: true }), getPdfTooLargeErrorMessage(true));
  });

  it('names a concrete fallback model for a 404 model-not-found when the family maps', () => {
    const c = classifyError(apiError(404, 'model claude-opus-4-6 not found'));
    const msg = friendlyErrorMessage(c, { model: 'claude-opus-4-6' });
    assert.ok(msg.includes('/model claude-opus-4-1'));
  });

  it('falls back to generic /model guidance when no suggestion applies', () => {
    const c = classifyError(apiError(404, 'model gpt-4o not found'));
    const msg = friendlyErrorMessage(c, { model: 'gpt-4o' });
    assert.match(msg, /Run \/model to pick a different one/);
  });

  it('shows the exact overflow counts when the server reported them', () => {
    const c = classifyError(apiError(400, 'prompt is too long: 200000 tokens > 150000 maximum'));
    const msg = friendlyErrorMessage(c);
    assert.ok(msg.includes('200000 > 150000 tokens'));
    assert.match(msg, /\/compact or \/clear/);
  });

  it('keeps the plain overflow message when no counts were parsed', () => {
    const c = classifyError('context too long');
    assert.equal(
      friendlyErrorMessage(c),
      'Context limit reached — compacting conversation. Run /compact or /clear to continue.',
    );
  });
});
