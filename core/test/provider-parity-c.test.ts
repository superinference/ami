import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  streamChatCompletionWithRetry,
  streamChatCompletion,
  sanitizeErrorMessage,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Parity batch C — two fully-in-file provider.ts gaps:
//
//  (1) FUNCTIONAL: body-detected overload retry. The Anthropic SDK sometimes
//      fails to propagate the 529 status during streaming, so an overload
//      surfaces as an error whose body carries `"type":"overloaded_error"` but
//      no parseable HTTP status. The retry gate was purely status-code driven
//      (extractStatusCode -> RETRYABLE_STATUS_CODES), so a status-less overload
//      was treated as non-retryable and terminated the turn.
//
//  (2) credential-redaction coverage for secret classes AMI itself provisions
//      (AWS access-key IDs, GCP service-account identities, AWS_*/GOOGLE_* and
//      generic SECRET/TOKEN/PASSWORD env assignments) that previously passed
//      through sanitizeErrorMessage unredacted.
// ---------------------------------------------------------------------------

const config: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1',
  apiKey: 'test-key',
  model: 'test-model',
};

const messages: Message[] = [{ role: 'user', content: 'hi' }];

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

describe('streamChatCompletionWithRetry - body-detected overload retry', () => {
  it('retries a status-less "overloaded_error" body and reports status 529', async () => {
    let attempts = 0;
    const retryStatuses: number[] = [];

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        // No "HTTP <n>" / "status code <n>" in the message => extractStatusCode
        // returns null. The overload is only discoverable from the JSON body.
        yield {
          type: 'error',
          error: 'stream error: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}',
        } as StreamChunk;
        return;
      }
      yield { type: 'content_delta', text: 'ok' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 10,
        streamImpl: fakeStream,
        onRetry: (_a, _m, _d, status) => { retryStatuses.push(status); },
      }),
    );

    // RED (before fix): statusCode is null, RETRYABLE_STATUS_CODES.has(null) is
    // false and no x-should-retry header => the error chunk is yielded and the
    // generator returns on the first attempt (attempts === 1, no content).
    // GREEN (after fix): the body overload is retried => attempts === 2, content
    // is produced and onRetry reports the 529 overload status.
    assert.equal(attempts, 2, `overload body must be retried, got ${attempts} attempt(s)`);
    assert.ok(chunks.some((c) => c.type === 'content_delta'), 'expected successful content after retry');
    assert.deepEqual(retryStatuses, [529], 'onRetry must report 529 for a body-detected overload');
  });

  it('does NOT retry a body overload when x-should-retry is false', async () => {
    let attempts = 0;
    const fakeStream = (async function* () {
      attempts++;
      yield {
        type: 'error',
        error: 'stream error: {"type":"overloaded_error"}',
        responseHeaders: { 'x-should-retry': 'false' },
      } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 10, streamImpl: fakeStream,
      }),
    );

    // Guard against over-retrying: an explicit x-should-retry:false still wins.
    assert.equal(attempts, 1, `x-should-retry:false must block overload retry, got ${attempts}`);
    assert.equal(chunks[chunks.length - 1].type, 'error');
  });

  it('does NOT misfire on an unrelated error with no overload body', async () => {
    let attempts = 0;
    const fakeStream = (async function* () {
      attempts++;
      yield { type: 'error', error: 'some other transport failure with no status' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 10, streamImpl: fakeStream,
      }),
    );

    // A status-less non-overload error is still terminal (no false retry).
    assert.equal(attempts, 1, `non-overload status-less error must not retry, got ${attempts}`);
    assert.equal(chunks[chunks.length - 1].type, 'error');
  });
});

describe('sanitizeErrorMessage - provisioned-credential coverage', () => {
  it('redacts an AWS access-key ID (AKIA…)', () => {
    const out = sanitizeErrorMessage('401 from bedrock using AKIAIOSFODNN7EXAMPLE now');
    assert.ok(!out.includes('AKIAIOSFODNN7EXAMPLE'), 'AWS access-key ID must be redacted');
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts a temporary/STS access-key ID (ASIA…)', () => {
    const out = sanitizeErrorMessage('error: ASIAABCDEFGHIJKLMNOP failed');
    assert.ok(!out.includes('ASIAABCDEFGHIJKLMNOP'));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts a GCP service-account identity', () => {
    const out = sanitizeErrorMessage('vertex auth failed for svc-deploy@my-project-123.iam.gserviceaccount.com');
    assert.ok(!out.includes('svc-deploy@my-project-123.iam.gserviceaccount.com'));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts an AWS_* env assignment value but keeps the variable name', () => {
    const out = sanitizeErrorMessage('env dump: AWS_SECRET_ACCESS_KEY=abc123/DEF+ghi456 present');
    assert.ok(!out.includes('abc123/DEF+ghi456'), 'secret value must be redacted');
    assert.ok(out.includes('AWS_SECRET_ACCESS_KEY='), 'variable name should remain for debuggability');
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts a GOOGLE_* env assignment value', () => {
    const out = sanitizeErrorMessage('GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/key.json');
    assert.ok(!out.includes('/etc/secrets/key.json'));
    assert.ok(out.includes('GOOGLE_APPLICATION_CREDENTIALS='));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts a generic SECRET/TOKEN/PASSWORD assignment value', () => {
    const out = sanitizeErrorMessage('config has API_KEY: supersecretvalue in body');
    assert.ok(!out.includes('supersecretvalue'));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('leaves non-credential assignment-shaped text with safe keys intact', () => {
    // Guard against over-redaction: no AWS/GOOGLE/secret keyword => no change.
    const msg = 'retries: 3 and region: us-east-1 configured';
    assert.equal(sanitizeErrorMessage(msg), msg);
  });
});
