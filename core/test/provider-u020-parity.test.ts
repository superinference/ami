import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  isStaleConnectionError,
  withFreshConnection,
  computeBedrockBetaExtraBody,
  BEDROCK_EXTRA_PARAMS_BETAS,
  endpointAcceptsClientRequestId,
  buildRequestIdHeader,
  withRequestIdDebugTrace,
  REQUEST_ID_HEADER,
  extractProviderRequestId,
  PROVIDER_RESPONSE_REQUEST_ID_HEADERS,
  INTERLEAVED_THINKING_BETA,
  withExtraBody,
  streamChatCompletion,
  streamChatCompletionWithRetry,
} from '../src/provider';
import { __resetLoggerForTest } from '../src/logger';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _ of gen) { /* consume */ }
}

const msgs: Message[] = [{ role: 'user', content: 'hi' }];

// ---------------------------------------------------------------------------
// U020 #16/#20 — stale keep-alive socket recovery (ECONNRESET/EPIPE)
// ---------------------------------------------------------------------------
describe('isStaleConnectionError — ECONNRESET/EPIPE detection', () => {
  for (const code of ['ECONNRESET', 'EPIPE']) {
    it(`true for a ${code} error code`, () => {
      assert.equal(isStaleConnectionError(Object.assign(new Error('socket'), { code })), true);
    });
  }
  it('true when the code is nested under .cause', () => {
    const inner = Object.assign(new Error('reset'), { code: 'ECONNRESET' });
    assert.equal(isStaleConnectionError(Object.assign(new Error('Connection error.'), { cause: inner })), true);
  });
  it('true when the message mentions ECONNRESET without a code', () => {
    assert.equal(isStaleConnectionError(new Error('write ECONNRESET on socket')), true);
  });
  it('false for a non-stale network error (ETIMEDOUT)', () => {
    assert.equal(isStaleConnectionError(Object.assign(new Error('t/o'), { code: 'ETIMEDOUT' })), false);
  });
  it('false for a plain string', () => {
    assert.equal(isStaleConnectionError('nope'), false);
  });
});

describe('streamChatCompletionWithRetry — stale socket forces a fresh connection on retry', () => {
  const baseConfig = { provider: 'openai', apiKey: 'k', baseUrl: 'http://127.0.0.1:1/v1', model: 'gpt-4o' } as ProviderConfig;

  it('sets config.forceFreshConnection on the attempt after an ECONNRESET', async () => {
    const seenConfigs: ProviderConfig[] = [];
    let calls = 0;
    const streamImpl = (async function* (cfg: ProviderConfig): AsyncGenerator<StreamChunk> {
      seenConfigs.push(cfg);
      if (calls++ === 0) throw Object.assign(new Error('write ECONNRESET'), { code: 'ECONNRESET' });
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;
    await drain(streamChatCompletionWithRetry(
      baseConfig, msgs, [], new AbortController().signal,
      { streamImpl, baseDelayMs: 0, maxRetries: 2 },
    ));
    assert.equal(calls, 2, 'must retry after the stale-socket drop');
    assert.equal(seenConfigs[0].forceFreshConnection ?? false, false, 'first attempt uses the pool');
    assert.equal(seenConfigs[1].forceFreshConnection, true, 'retry must force a fresh connection');
  });

  it('does NOT force a fresh connection for a non-stale network error (ETIMEDOUT)', async () => {
    const seenConfigs: ProviderConfig[] = [];
    let calls = 0;
    const streamImpl = (async function* (cfg: ProviderConfig): AsyncGenerator<StreamChunk> {
      seenConfigs.push(cfg);
      if (calls++ === 0) throw Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' });
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;
    await drain(streamChatCompletionWithRetry(
      baseConfig, msgs, [], new AbortController().signal,
      { streamImpl, baseDelayMs: 0, maxRetries: 2 },
    ));
    assert.equal(calls, 2);
    assert.notEqual(seenConfigs[1].forceFreshConnection, true, 'ETIMEDOUT must not trigger keep-alive drop');
  });
});

describe('withFreshConnection', () => {
  it('invokes the base fetch and returns its response', async () => {
    let called = false;
    const base = (async () => { called = true; return new Response('{}'); }) as unknown as typeof globalThis.fetch;
    const res = await withFreshConnection(base)('http://x/v1/chat', { method: 'POST', body: '{}' } as RequestInit);
    assert.equal(called, true);
    assert.equal(await res.text(), '{}');
  });
  it('preserves a caller-provided dispatcher (never overrides it)', async () => {
    let seenInit: RequestInit | undefined;
    const base = (async (_i: unknown, init?: RequestInit) => { seenInit = init; return new Response('{}'); }) as unknown as typeof globalThis.fetch;
    const dispatcher = { tag: 'caller' };
    await withFreshConnection(base)('http://x', { dispatcher } as unknown as RequestInit);
    assert.equal((seenInit as unknown as { dispatcher?: unknown }).dispatcher, dispatcher);
  });
});

// ---------------------------------------------------------------------------
// U020 #8 — request-id correlation header gated to first-party endpoints
// ---------------------------------------------------------------------------
describe('endpointAcceptsClientRequestId — managed-gateway gating', () => {
  for (const p of ['amazon-bedrock', 'anthropic-vertex', 'google-vertex', 'azure-openai']) {
    it(`false for managed gateway ${p}`, () => {
      assert.equal(endpointAcceptsClientRequestId(p), false);
    });
  }
  for (const p of ['openai', 'anthropic', 'openrouter', 'groq', undefined]) {
    it(`true for direct endpoint ${p ?? '(inferred)'}`, () => {
      assert.equal(endpointAcceptsClientRequestId(p), true);
    });
  }
  it('buildRequestIdHeader returns {} when injection is disabled', () => {
    assert.deepEqual(buildRequestIdHeader(undefined, false), {});
  });
  it('buildRequestIdHeader still injects by default', () => {
    assert.ok(buildRequestIdHeader()[REQUEST_ID_HEADER]);
  });
});

describe('extractProviderRequestId — server correlation id from response headers', () => {
  it('returns undefined when headers are omitted or empty', () => {
    assert.equal(extractProviderRequestId(undefined), undefined);
    assert.equal(extractProviderRequestId({}), undefined);
  });
  it('matches the first known header name, case-insensitively', () => {
    assert.equal(extractProviderRequestId({ 'Anthropic-Request-Id': 'req_1' }), 'req_1');
    assert.equal(extractProviderRequestId({ 'X-Request-Id': 'req_2' }), 'req_2');
    assert.equal(extractProviderRequestId({ 'request-id': 'req_3' }), 'req_3');
    assert.equal(extractProviderRequestId({ 'openai-request-id': 'req_4' }), 'req_4');
  });
  it('matches the echoed client request-id header', () => {
    assert.equal(extractProviderRequestId({ [REQUEST_ID_HEADER]: 'req_client' }), 'req_client');
  });
  it('prefers anthropic-request-id over other present headers (first-match order)', () => {
    const id = extractProviderRequestId({ 'x-request-id': 'b', 'anthropic-request-id': 'a' });
    assert.equal(id, 'a');
  });
  it('ignores unrelated headers', () => {
    assert.equal(extractProviderRequestId({ 'content-type': 'text/event-stream' }), undefined);
  });
  it('exposes the header priority list with anthropic-request-id first', () => {
    assert.equal(PROVIDER_RESPONSE_REQUEST_ID_HEADERS[0], 'anthropic-request-id');
    assert.ok(PROVIDER_RESPONSE_REQUEST_ID_HEADERS.includes(REQUEST_ID_HEADER));
  });
});

describe('request-id gating is wired through resolveModel (on the wire)', () => {
  it('omits the request-id header for a managed gateway (azure-openai)', async () => {
    const seen: Array<string | undefined> = [];
    const srv = http.createServer((req, res) => {
      seen.push(req.headers[REQUEST_ID_HEADER] as string | undefined);
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}\n\ndata: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\ndata: [DONE]\n\n');
    });
    await new Promise<void>(r => srv.listen(0, '127.0.0.1', () => r()));
    const port = (srv.address() as { port: number }).port;
    try {
      const cfg = { provider: 'azure-openai', apiKey: 'k', baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model', maxTokens: 50 } as ProviderConfig;
      await drain(streamChatCompletion(cfg, msgs, [], new AbortController().signal));
    } finally {
      await new Promise<void>(r => srv.close(() => r()));
    }
    assert.equal(seen[0], undefined, 'managed-gateway request must NOT carry the request-id header');
  });
});

// ---------------------------------------------------------------------------
// U020 #9 — per-request debug trace surfacing the client request-id
// ---------------------------------------------------------------------------
describe('withRequestIdDebugTrace', () => {
  it('writes an [API REQUEST] trace line with the request-id to the debug log', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u020-trace-'));
    const file = path.join(dir, 'engine.log');
    const prevFile = process.env.AMI_DEBUG_FILE;
    const prevLevel = process.env.AMI_DEBUG_LOG_LEVEL;
    process.env.AMI_DEBUG_FILE = file;
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    __resetLoggerForTest();
    try {
      const base = (async () => new Response('{}')) as unknown as typeof globalThis.fetch;
      const wrapped = withRequestIdDebugTrace(base, 'openai');
      await wrapped('http://host/v1/chat/completions', { headers: { [REQUEST_ID_HEADER]: 'trace-id-123' } } as RequestInit);
      const contents = fs.readFileSync(file, 'utf8');
      assert.ok(contents.includes('[API REQUEST]'), 'must emit the API REQUEST marker');
      assert.ok(contents.includes('/v1/chat/completions'), 'must include the pathname');
      assert.ok(contents.includes(`${REQUEST_ID_HEADER}=trace-id-123`), 'must include the request-id');
      assert.ok(contents.includes('source=openai'), 'must include the source label');
    } finally {
      if (prevFile === undefined) delete process.env.AMI_DEBUG_FILE; else process.env.AMI_DEBUG_FILE = prevFile;
      if (prevLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL; else process.env.AMI_DEBUG_LOG_LEVEL = prevLevel;
      __resetLoggerForTest();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
  it('never throws on an unparseable input (best-effort tracing)', async () => {
    let called = false;
    const base = (async () => { called = true; return new Response('{}'); }) as unknown as typeof globalThis.fetch;
    await withRequestIdDebugTrace(base, 'x')({} as unknown as string);
    assert.equal(called, true, 'the live fetch must still run even if tracing could not parse the URL');
  });
});

// ---------------------------------------------------------------------------
// U020 #1 — Bedrock betas routed through extraBodyParams, not headers
// ---------------------------------------------------------------------------
describe('computeBedrockBetaExtraBody', () => {
  it('emits the interleaved-thinking beta under anthropic_beta for a Claude model', () => {
    const body = computeBedrockBetaExtraBody('claude-opus-4-1');
    assert.ok(body, 'a thinking-capable Claude model must produce a body fragment');
    const betas = (body as { anthropic_beta: string[] }).anthropic_beta;
    assert.ok(Array.isArray(betas));
    assert.ok(betas.includes(INTERLEAVED_THINKING_BETA), 'interleaved-thinking must be routed via the body');
    for (const b of betas) assert.ok(BEDROCK_EXTRA_PARAMS_BETAS.has(b), `${b} must be in the Bedrock extra-params subset`);
  });
  it('returns undefined for a non-Claude model', () => {
    assert.equal(computeBedrockBetaExtraBody('gpt-4o'), undefined);
  });
  it('returns undefined for a Claude model with no applicable betas (e.g. claude-3-opus)', () => {
    // claude-3-opus is not interleaved-thinking/1M-capable, so nothing is routed.
    assert.equal(computeBedrockBetaExtraBody('claude-3-opus-20240229'), undefined);
  });
});

describe('Bedrock beta routing — body composition (the exact wiring resolveModel applies)', () => {
  it('withExtraBody(computeBedrockBetaExtraBody(model), fetch) merges anthropic_beta into the posted JSON body', async () => {
    // resolveModel's bedrock branch composes exactly this: the beta fragment is
    // deep-merged into the outgoing request body (NOT sent as a header). Driving
    // that composition directly keeps the assertion deterministic.
    let seenBody: Record<string, unknown> | undefined;
    const base = (async (_i: unknown, init?: RequestInit) => {
      seenBody = JSON.parse(String(init?.body));
      return new Response('{}');
    }) as unknown as typeof globalThis.fetch;
    const betaBody = computeBedrockBetaExtraBody('claude-opus-4-1');
    const bedrockFetch = withExtraBody(betaBody, base) ?? base;
    await bedrockFetch('https://bedrock.example/v1/chat/completions', {
      method: 'POST',
      body: JSON.stringify({ model: 'claude-opus-4-1', messages: [] }),
    } as RequestInit);
    assert.ok(seenBody, 'the request body must have been observed');
    const betas = (seenBody as { anthropic_beta?: unknown }).anthropic_beta;
    assert.ok(Array.isArray(betas) && (betas as string[]).includes(INTERLEAVED_THINKING_BETA),
      'the beta must ride in the body, not a header');
    assert.equal((seenBody as { model?: string }).model, 'claude-opus-4-1', 'existing body fields preserved');
  });

  it('is a passthrough (no body mutation) for a non-Claude Bedrock model', async () => {
    let seenBody: Record<string, unknown> | undefined;
    const base = (async (_i: unknown, init?: RequestInit) => {
      seenBody = JSON.parse(String(init?.body));
      return new Response('{}');
    }) as unknown as typeof globalThis.fetch;
    const betaBody = computeBedrockBetaExtraBody('amazon.titan-text');
    const bedrockFetch = withExtraBody(betaBody, base) ?? base;
    await bedrockFetch('https://bedrock.example/v1/chat/completions', {
      method: 'POST',
      body: JSON.stringify({ model: 'amazon.titan-text', messages: [] }),
    } as RequestInit);
    assert.equal((seenBody as { anthropic_beta?: unknown }).anthropic_beta, undefined);
  });
});
