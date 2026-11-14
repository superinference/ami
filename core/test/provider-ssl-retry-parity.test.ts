import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { isSslError, streamChatCompletionWithRetry } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _ of gen) { /* consume */ }
}

const baseConfig: ProviderConfig = {
  provider: 'openai', apiKey: 'k', baseUrl: 'http://127.0.0.1:1/v1', model: 'gpt-4o',
} as ProviderConfig;
const msgs: Message[] = [{ role: 'user', content: 'hi' }];

describe('isSslError — expanded OpenSSL/TLS code set', () => {
  for (const code of [
    'UNABLE_TO_GET_ISSUER_CERT',
    'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
    'CERT_REVOKED',
    'CERT_NOT_YET_VALID',
    'HOSTNAME_MISMATCH',
    'ERR_TLS_HANDSHAKE_TIMEOUT',
    'ERR_SSL_WRONG_VERSION_NUMBER',
    'ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC',
  ]) {
    it(`recognizes ${code}`, () => {
      const e = Object.assign(new Error('tls failure'), { code });
      assert.equal(isSslError(e), true);
    });
  }
  it('still returns false for a non-SSL code', () => {
    const e = Object.assign(new Error('nope'), { code: 'ECONNRESET' });
    assert.equal(isSslError(e), false);
  });
});

describe('streamChatCompletionWithRetry — 408/409 are retryable', () => {
  for (const status of [408, 409]) {
    it(`retries HTTP ${status}`, async () => {
      let calls = 0;
      const seen: number[] = [];
      const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
        if (calls++ === 0) { yield { type: 'error', error: `HTTP ${status}: transient` }; return; }
        yield { type: 'done', finishReason: 'stop' };
      }) as unknown as Parameters<typeof streamChatCompletionWithRetry>[5]['streamImpl'];
      await drain(streamChatCompletionWithRetry(
        baseConfig, msgs, [], new AbortController().signal,
        { streamImpl, baseDelayMs: 0, onRetry: (_a, _m, _d, s) => seen.push(s) },
      ));
      assert.deepEqual(seen, [status], `expected one retry on ${status}`);
      assert.equal(calls, 2);
    });
  }
});
