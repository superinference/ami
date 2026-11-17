import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { streamChatCompletionWithRetry } from '../src/provider';
import { getLogPath, __resetLoggerForTest } from '../src/logger';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _ of gen) { /* consume */ }
}

const baseConfig: ProviderConfig = {
  provider: 'openai', apiKey: 'k', baseUrl: 'http://127.0.0.1:1/v1', model: 'gpt-4o',
} as ProviderConfig;
const msgs: Message[] = [{ role: 'user', content: 'hi' }];

function readLog(): string {
  const p = getLogPath();
  return fs.existsSync(p) ? fs.readFileSync(p, 'utf-8') : '';
}

describe('streamChatCompletionWithRetry — connection-error diagnostic is logged on network retry', () => {
  beforeEach(() => {
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    delete process.env.AMI_DEBUG_TO_STDERR;
    delete process.env.AMI_DEBUG_COMPONENTS;
    __resetLoggerForTest();
  });

  it('writes the root errno + code to the debug log when a connection drops and retries', async () => {
    const before = readLog();
    let calls = 0;
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      if (calls++ === 0) {
        // A dropped connection surfaces as a thrown error whose root errno is
        // buried under a code-less wrapper, exactly like the undici/SDK shape.
        const root = Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
        throw Object.assign(new Error('Connection error.'), { cause: root });
      }
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as Parameters<typeof streamChatCompletionWithRetry>[4]['streamImpl'];

    await drain(streamChatCompletionWithRetry(
      baseConfig, msgs, [], new AbortController().signal,
      { streamImpl, baseDelayMs: 0, maxRetries: 3 },
    ));

    assert.equal(calls, 2, 'should retry once after the dropped connection');
    const written = readLog().slice(before.length);
    assert.match(written, /Connection error details: code=ECONNRESET/);
  });
});
