import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isAwsCredentialsProviderError,
  getAwsCredentialsErrorMessage,
  apiErrorStatusFallback,
  normalizeErrorMessage,
  classifyError,
  friendlyErrorMessage,
} from '../src/error-classifier';

// ---------------------------------------------------------------------------
// U073 w71 — AWS Bedrock credential-chain classification (gap: CredentialsProviderError)
// ---------------------------------------------------------------------------

describe('isAwsCredentialsProviderError', () => {
  it('matches the AWS SDK error by its stable .name', () => {
    const err = Object.assign(new Error('Could not load credentials from any providers'), {
      name: 'CredentialsProviderError',
    });
    assert.equal(isAwsCredentialsProviderError(err), true);
  });
  it('is false for an ordinary Error', () => {
    assert.equal(isAwsCredentialsProviderError(new Error('boom')), false);
  });
  it('is false for a null / string value', () => {
    assert.equal(isAwsCredentialsProviderError(null), false);
    assert.equal(isAwsCredentialsProviderError('CredentialsProviderError'), false);
  });
});

describe('getAwsCredentialsErrorMessage', () => {
  it('carries the "AWS credentials" marker and actionable steps', () => {
    const m = getAwsCredentialsErrorMessage();
    assert.ok(m.includes('AWS credentials'));
    assert.ok(m.includes('AWS_PROFILE'));
    assert.ok(m.includes('aws sso login'));
  });
});

describe('classifyError — AWS CredentialsProviderError', () => {
  it('classifies it as a non-retryable auth error with Bedrock guidance', () => {
    const err = Object.assign(new Error('Could not load credentials from any providers'), {
      name: 'CredentialsProviderError',
    });
    const c = classifyError(err);
    assert.equal(c.category, 'auth_error');
    assert.equal(c.retryable, false);
    assert.equal(c.shouldFallback, false);
    assert.ok(c.message.includes('AWS credentials'));
  });
  it('friendlyErrorMessage surfaces the AWS guidance, not the generic API-key hint', () => {
    const err = Object.assign(new Error('Could not load credentials from any providers'), {
      name: 'CredentialsProviderError',
    });
    const friendly = friendlyErrorMessage(classifyError(err));
    assert.ok(friendly.includes('aws sso login'));
    assert.ok(!friendly.includes('check your API key'));
  });
});

// ---------------------------------------------------------------------------
// U073 w71 — status fallback for deserialized / message-less API errors (gap: formatAPIError guard)
// ---------------------------------------------------------------------------

describe('apiErrorStatusFallback', () => {
  it('reads a numeric .status', () => {
    assert.equal(apiErrorStatusFallback({ status: 503 }), 'API error (status 503)');
  });
  it('reads a numeric .statusCode', () => {
    assert.equal(apiErrorStatusFallback({ statusCode: 404 }), 'API error (status 404)');
  });
  it('says "unknown" when there is no numeric status', () => {
    assert.equal(apiErrorStatusFallback({}), 'API error (status unknown)');
    assert.equal(apiErrorStatusFallback(null), 'API error (status unknown)');
    assert.equal(apiErrorStatusFallback({ status: 'oops' }), 'API error (status unknown)');
  });
});

describe('normalizeErrorMessage — message-less API error fallback', () => {
  it('still reads a top-level string / Error.message / nested shape (regression)', () => {
    assert.equal(normalizeErrorMessage('plain'), 'plain');
    assert.equal(normalizeErrorMessage(new Error('boom')), 'boom');
    assert.equal(normalizeErrorMessage({ error: { error: { message: 'nested text' } } }), 'nested text');
  });
  it('falls back to API error (status N) for an Error that lost its .message', () => {
    const err = Object.assign(new Error(''), { status: 500 });
    assert.equal(normalizeErrorMessage(err), 'API error (status 500)');
  });
  it('falls back to API error (status N) for a plain object with only a status', () => {
    assert.equal(normalizeErrorMessage({ statusCode: 503 }), 'API error (status 503)');
  });
  it('falls back to API error (status unknown) instead of [object Object]', () => {
    const m = normalizeErrorMessage({});
    assert.equal(m, 'API error (status unknown)');
    assert.notEqual(m, '[object Object]');
  });
  it('prefers a present plain-object .message over the status fallback', () => {
    assert.equal(normalizeErrorMessage({ message: 'hi', status: 500 }), 'hi');
  });
});

describe('classifyError — plain-object API error carries the status fallback message', () => {
  it('surfaces API error (status 503) rather than [object Object]', () => {
    const c = classifyError({ statusCode: 503 });
    assert.notEqual(c.message, '[object Object]');
    assert.equal(c.message, 'API error (status 503)');
  });
});
