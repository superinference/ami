// ===========================================================================
// provider-u020-w68.test.ts
//
// Parity unit U020 (wave 68) — additive provider-transport capabilities in
// core/core/src/provider.ts, each mirroring a reference reference:
//
//   - CLAUDE_CODE_EXTRA_BODY / AMI_EXTRA_BODY env-var request-body knob:
//     resolveExtraBodyFromEnv() + deep-merge into config.extraBody in resolveModel
//     so a deploy-time JSON object rides every OpenAI-compatible request.
//     [ref: services/api/claude.ts L272-295 getExtraBodyParams]
//
//   - Request metadata user id (Anthropic metadata.user_id / OpenAI user) from
//     config.metadata.userId or the AMI_METADATA_USER_ID / CLAUDE_CODE_METADATA_USER_ID
//     env knob: resolveMetadataUserId() + buildMetadataProviderOptions() +
//     mergeProviderOptionFragments() wiring into the streamText call.
//     [ref: services/api/claude.ts L503-527 getAPIMetadata]
//
//   - Context-overflow 400 auto-recovery: parseMaxTokensContextOverflowError() +
//     recoverMaxTokensFromContextOverflow() + a 400 recovery branch in
//     streamChatCompletionWithRetry that recomputes a smaller max_tokens and
//     retries the SAME request instead of failing the turn.
//     [ref: services/api/withRetry.ts L388-427,550-595]
//
// ESM-coverage-safe: no source-text assertions; env snapshotted + restored per
// test; the only injected dependencies are the streamText / stream seams and a
// localhost http server bound to port 0.
// ===========================================================================
import { describe, it, beforeEach, afterEach, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';

import {
  resolveExtraBodyFromEnv,
  EXTRA_BODY_ENV_VARS,
  resolveMetadataUserId,
  METADATA_USER_ID_ENV_VARS,
  buildMetadataProviderOptions,
  mergeProviderOptionFragments,
  parseMaxTokensContextOverflowError,
  recoverMaxTokensFromContextOverflow,
  CONTEXT_OVERFLOW_SAFETY_BUFFER,
  FLOOR_OUTPUT_TOKENS,
  streamChatCompletion,
  streamChatCompletionWithRetry,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk, ThinkingConfig } from '../src/types';

// ---------------------------------------------------------------------------
const TRACKED_ENV = [...EXTRA_BODY_ENV_VARS, ...METADATA_USER_ID_ENV_VARS];
let savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  savedEnv = {};
  for (const k of TRACKED_ENV) { savedEnv[k] = process.env[k]; delete process.env[k]; }
});
afterEach(() => {
  for (const k of TRACKED_ENV) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

function captureStreamText() {
  const calls: Array<Record<string, unknown>> = [];
  const fn = (cfg: Record<string, unknown>) => {
    calls.push(cfg);
    return {
      stream: (async function* () {
        yield { type: 'finish-step', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } };
      })(),
    };
  };
  return { calls, fn };
}

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

// ===========================================================================
// resolveExtraBodyFromEnv
// ===========================================================================
describe('resolveExtraBodyFromEnv', () => {
  it('returns undefined when no env var is set', () => {
    assert.equal(resolveExtraBodyFromEnv({}), undefined);
  });

  it('parses a valid AMI_EXTRA_BODY JSON object', () => {
    assert.deepEqual(
      resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '{"chat_template_kwargs":{"enable_thinking":false}}' }),
      { chat_template_kwargs: { enable_thinking: false } },
    );
  });

  it('falls back to CLAUDE_CODE_EXTRA_BODY for drop-in compatibility', () => {
    assert.deepEqual(resolveExtraBodyFromEnv({ CLAUDE_CODE_EXTRA_BODY: '{"foo":1}' }), { foo: 1 });
  });

  it('prefers the AMI-native spelling over the Claude name', () => {
    assert.deepEqual(
      resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '{"src":"ami"}', CLAUDE_CODE_EXTRA_BODY: '{"src":"claude"}' }),
      { src: 'ami' },
    );
  });

  it('ignores a malformed JSON value and falls through to the next var', () => {
    assert.deepEqual(
      resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '{not json', CLAUDE_CODE_EXTRA_BODY: '{"ok":true}' }),
      { ok: true },
    );
  });

  it('rejects non-object JSON (array / scalar / null) as undefined', () => {
    assert.equal(resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '[1,2,3]' }), undefined);
    assert.equal(resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '42' }), undefined);
    assert.equal(resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: 'null' }), undefined);
    assert.equal(resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '"str"' }), undefined);
  });

  it('treats an empty / whitespace value as unset', () => {
    assert.equal(resolveExtraBodyFromEnv({ AMI_EXTRA_BODY: '   ' }), undefined);
  });

  it('defaults to process.env when no arg is passed', () => {
    process.env.AMI_EXTRA_BODY = '{"z":9}';
    assert.deepEqual(resolveExtraBodyFromEnv(), { z: 9 });
  });
});

// ===========================================================================
// resolveMetadataUserId
// ===========================================================================
describe('resolveMetadataUserId', () => {
  const base: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: '', model: 'gpt-4o' };

  it('returns undefined when neither config nor env provides an id', () => {
    assert.equal(resolveMetadataUserId(base, {}), undefined);
  });

  it('prefers an explicit config.metadata.userId', () => {
    assert.equal(resolveMetadataUserId({ ...base, metadata: { userId: 'cfg-1' } }, { AMI_METADATA_USER_ID: 'env-1' }), 'cfg-1');
  });

  it('reads AMI_METADATA_USER_ID from the env', () => {
    assert.equal(resolveMetadataUserId(base, { AMI_METADATA_USER_ID: 'env-1' }), 'env-1');
  });

  it('falls back to CLAUDE_CODE_METADATA_USER_ID', () => {
    assert.equal(resolveMetadataUserId(base, { CLAUDE_CODE_METADATA_USER_ID: 'env-claude' }), 'env-claude');
  });

  it('prefers the AMI-native spelling over the Claude name', () => {
    assert.equal(resolveMetadataUserId(base, { AMI_METADATA_USER_ID: 'a', CLAUDE_CODE_METADATA_USER_ID: 'c' }), 'a');
  });

  it('ignores an empty config id and falls through to the env', () => {
    assert.equal(resolveMetadataUserId({ ...base, metadata: { userId: '  ' } }, { AMI_METADATA_USER_ID: 'env-1' }), 'env-1');
  });
});

// ===========================================================================
// buildMetadataProviderOptions
// ===========================================================================
describe('buildMetadataProviderOptions', () => {
  it('returns an empty object when there is no id', () => {
    assert.deepEqual(buildMetadataProviderOptions(undefined, true), {});
    assert.deepEqual(buildMetadataProviderOptions('', false), {});
  });

  it('uses the Anthropic metadata.userId shape', () => {
    assert.deepEqual(buildMetadataProviderOptions('u1', true), { providerOptions: { anthropic: { metadata: { userId: 'u1' } } } });
  });

  it('uses the OpenAI user shape', () => {
    assert.deepEqual(buildMetadataProviderOptions('u1', false), { providerOptions: { openai: { user: 'u1' } } });
  });
});

// ===========================================================================
// mergeProviderOptionFragments
// ===========================================================================
describe('mergeProviderOptionFragments', () => {
  it('returns {} when no fragment contributes providerOptions', () => {
    assert.deepEqual(mergeProviderOptionFragments({}, { foo: 1 }), {});
  });

  it('deep-merges anthropic cacheControl + thinking + metadata without clobbering', () => {
    const merged = mergeProviderOptionFragments(
      { providerOptions: { anthropic: { thinking: { type: 'adaptive' }, effort: 'high' } } },
      { providerOptions: { anthropic: { metadata: { userId: 'u1' } } } },
    );
    assert.deepEqual(merged, {
      providerOptions: { anthropic: { thinking: { type: 'adaptive' }, effort: 'high', metadata: { userId: 'u1' } } },
    });
  });

  it('keeps distinct provider namespaces side by side', () => {
    const merged = mergeProviderOptionFragments(
      { providerOptions: { openai: { promptCacheKey: 'k' } } },
      { providerOptions: { openai: { user: 'u1' } } },
    );
    assert.deepEqual(merged, { providerOptions: { openai: { promptCacheKey: 'k', user: 'u1' } } });
  });
});

// ===========================================================================
// parseMaxTokensContextOverflowError
// ===========================================================================
describe('parseMaxTokensContextOverflowError', () => {
  it('parses the A + B > C token triple', () => {
    assert.deepEqual(
      parseMaxTokensContextOverflowError('HTTP 400: input length and `max_tokens` exceed context limit: 188059 + 20000 > 200000'),
      { inputTokens: 188059, maxTokens: 20000, contextLimit: 200000 },
    );
  });

  it('returns null for an unrelated error', () => {
    assert.equal(parseMaxTokensContextOverflowError('HTTP 400: some other bad request'), null);
    assert.equal(parseMaxTokensContextOverflowError(undefined), null);
    assert.equal(parseMaxTokensContextOverflowError(''), null);
  });

  it('returns null when the substring matches but the numeric triple does not', () => {
    assert.equal(parseMaxTokensContextOverflowError('input length and `max_tokens` exceed context limit: not numbers'), null);
  });
});

// ===========================================================================
// recoverMaxTokensFromContextOverflow
// ===========================================================================
describe('recoverMaxTokensFromContextOverflow', () => {
  it('returns availableContext when it dominates the floor', () => {
    // 200000 - 188059 - 1000 = 10941
    assert.equal(recoverMaxTokensFromContextOverflow({ inputTokens: 188059, contextLimit: 200000 }), 10941);
    assert.equal(CONTEXT_OVERFLOW_SAFETY_BUFFER, 1000);
  });

  it('returns FLOOR_OUTPUT_TOKENS at the floor boundary (availableContext === floor)', () => {
    // 200000 - 196000 - 1000 = 3000 === FLOOR_OUTPUT_TOKENS
    assert.equal(recoverMaxTokensFromContextOverflow({ inputTokens: 196000, contextLimit: 200000 }), FLOOR_OUTPUT_TOKENS);
  });

  it('returns null when even the floor will not fit (unrecoverable)', () => {
    // 200000 - 199000 - 1000 = 0 < 3000
    assert.equal(recoverMaxTokensFromContextOverflow({ inputTokens: 199000, contextLimit: 200000 }), null);
  });

  it('respects the thinking-budget floor (minRequired = budget + 1)', () => {
    const thinking: ThinkingConfig = { enabled: true, level: 'high', budgetTokens: 15000 };
    // availableContext = 200000 - 190000 - 1000 = 9000; minRequired = 15001 dominates
    assert.equal(recoverMaxTokensFromContextOverflow({ inputTokens: 190000, contextLimit: 200000 }, thinking), 15001);
  });
});

// ===========================================================================
// Integration — streamChatCompletionWithRetry context-overflow recovery
// ===========================================================================
describe('streamChatCompletionWithRetry — context-overflow 400 auto-recovery', () => {
  const config: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: 'claude-sonnet-4-5' };
  const msgs: Message[] = [{ role: 'user', content: 'hi' }];

  it('recomputes a smaller max_tokens and retries the SAME request', async () => {
    const seenMaxTokens: Array<number | undefined> = [];
    let attempt = 0;
    const streamImpl = (async function* (cfg: ProviderConfig): AsyncGenerator<StreamChunk> {
      seenMaxTokens.push(cfg.maxTokens);
      if (attempt++ === 0) {
        yield { type: 'error', error: 'HTTP 400: input length and `max_tokens` exceed context limit: 188059 + 20000 > 200000' };
        return;
      }
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      config, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2 },
    ));

    assert.equal(seenMaxTokens.length, 2, 'should retry exactly once');
    assert.equal(seenMaxTokens[0], undefined, 'first attempt uses the caller default');
    assert.equal(seenMaxTokens[1], 10941, 'retry carries the recomputed reduced budget');
    assert.equal(chunks.at(-1)!.type, 'done');
  });

  it('does NOT retry when the recomputed budget makes no progress', async () => {
    const seen: Array<number | undefined> = [];
    const streamImpl = (async function* (cfg: ProviderConfig): AsyncGenerator<StreamChunk> {
      seen.push(cfg.maxTokens);
      // thinking budget (15000) forces minRequired 15001 >= overflow max_tokens 12000
      yield { type: 'error', error: 'HTTP 400: input length and `max_tokens` exceed context limit: 190000 + 12000 > 200000' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      config, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2, thinking: { enabled: true, level: 'high', budgetTokens: 15000 } },
    ));

    assert.equal(seen.length, 1, 'no-progress overflow is terminal (no retry)');
    assert.equal(chunks.at(-1)!.type, 'error');
    assert.match(chunks.at(-1)!.error ?? '', /exceed context limit/);
  });

  it('surfaces the error when even the floor will not fit (unrecoverable)', async () => {
    let attempt = 0;
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      attempt++;
      yield { type: 'error', error: 'HTTP 400: input length and `max_tokens` exceed context limit: 199000 + 5000 > 200000' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      config, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    assert.equal(attempt, 1, 'unrecoverable overflow does not retry');
    assert.equal(chunks.at(-1)!.type, 'error');
  });

  it('leaves a non-overflow 400 terminal (no recovery branch)', async () => {
    let attempt = 0;
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      attempt++;
      yield { type: 'error', error: 'HTTP 400: malformed request' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      config, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    assert.equal(attempt, 1);
    assert.equal(chunks.at(-1)!.type, 'error');
  });
});

// ===========================================================================
// Integration — transient 5xx retry set (504 / 507 gateway/storage timeouts)
// ===========================================================================
describe('streamChatCompletionWithRetry — transient 5xx gateway/storage codes', () => {
  const config: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: 'claude-sonnet-4-5' };
  const msgs: Message[] = [{ role: 'user', content: 'hi' }];

  async function attemptsFor(status: number): Promise<number> {
    let attempt = 0;
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      attempt++;
      if (attempt === 1) { yield { type: 'error', error: `HTTP ${status}: transient` }; return; }
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;
    await drain(streamChatCompletionWithRetry(
      config, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    return attempt;
  }

  it('retries a 504 (gateway timeout)', async () => {
    assert.equal(await attemptsFor(504), 2);
  });

  it('retries a 507 (insufficient storage)', async () => {
    assert.equal(await attemptsFor(507), 2);
  });

  it('does NOT retry a terminal 501 (not implemented)', async () => {
    assert.equal(await attemptsFor(501), 1);
  });
});

// ===========================================================================
// Integration — metadata providerOptions wiring into streamChatCompletion
// ===========================================================================
describe('metadata providerOptions wiring into streamChatCompletion', () => {
  const anthropic: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: 'claude-sonnet-4-5' };
  const openai: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: 'https://api.openai.com/v1', model: 'gpt-4o' };
  const msgs: Message[] = [{ role: 'user', content: 'hi' }];

  async function providerOptionsFor(cfg: ProviderConfig, thinking?: ThinkingConfig) {
    const cap = captureStreamText();
    await drain(streamChatCompletion(cfg, msgs, [], new AbortController().signal, { streamTextImpl: cap.fn as never, thinking }));
    return cap.calls[0].providerOptions as Record<string, Record<string, unknown>> | undefined;
  }

  it('forwards the Anthropic metadata.userId from config', async () => {
    const po = await providerOptionsFor({ ...anthropic, metadata: { userId: 'acct-7' } });
    assert.deepEqual(po?.anthropic?.metadata, { userId: 'acct-7' });
  });

  it('forwards the OpenAI user from the AMI_METADATA_USER_ID env knob', async () => {
    process.env.AMI_METADATA_USER_ID = 'env-session';
    const po = await providerOptionsFor(openai);
    assert.equal(po?.openai?.user, 'env-session');
  });

  it('attaches no metadata when neither config nor env sets an id', async () => {
    const po = await providerOptionsFor(openai);
    assert.equal(po?.openai?.user, undefined);
  });

  it('combines thinking + metadata on the SAME anthropic providerOptions (no clobber)', async () => {
    const po = await providerOptionsFor(
      { ...anthropic, metadata: { userId: 'u9' } },
      { enabled: true, level: 'high' },
    );
    // metadata must survive alongside the thinking options on the shared anthropic namespace
    assert.deepEqual(po?.anthropic?.metadata, { userId: 'u9' });
    assert.ok(po?.anthropic?.thinking, 'thinking options are present too, not clobbered by metadata');
  });
});

// ===========================================================================
// E2E — env knobs reach the wire through streamChatCompletion
// ===========================================================================
describe('streamChatCompletion — env knobs on the wire (OpenAI-compatible)', () => {
  let server: http.Server;
  let port: number;
  let lastBody: Record<string, unknown> | null = null;

  before(async () => {
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      try { lastBody = JSON.parse(Buffer.concat(chunks).toString('utf-8')); } catch { lastBody = null; }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
      for (const line of [
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"hi"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}',
        'data: [DONE]',
      ]) res.write(line + '\n\n');
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });
  after(() => { server.close(); });

  it('merges AMI_EXTRA_BODY into the outgoing request body', async () => {
    process.env.AMI_EXTRA_BODY = '{"chat_template_kwargs":{"enable_thinking":false}}';
    const config: ProviderConfig = { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'test-key', model: 'Qwen/Qwen3-8B' };
    await drain(streamChatCompletion(config, [{ role: 'user', content: 'Hi' }], [], new AbortController().signal));
    assert.ok(lastBody, 'server received a JSON body');
    assert.deepEqual(lastBody!['chat_template_kwargs'], { enable_thinking: false });
  });

  it('lets an explicit config.extraBody win over the env default', async () => {
    process.env.AMI_EXTRA_BODY = '{"shared":"env","only_env":1}';
    const config: ProviderConfig = {
      baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'test-key', model: 'Qwen/Qwen3-8B',
      extraBody: { shared: 'config' },
    };
    await drain(streamChatCompletion(config, [{ role: 'user', content: 'Hi' }], [], new AbortController().signal));
    assert.equal(lastBody!['shared'], 'config', 'config value wins over env');
    assert.equal(lastBody!['only_env'], 1, 'env-only keys still merged in');
  });

  it('forwards the metadata user id to the wire as the OpenAI user field', async () => {
    process.env.AMI_METADATA_USER_ID = 'wire-user-1';
    const config: ProviderConfig = {
      baseUrl: `http://127.0.0.1:${port}/api.openai.com/v1`, apiKey: 'test-key', provider: 'openai', model: 'gpt-4o',
    };
    await drain(streamChatCompletion(config, [{ role: 'user', content: 'Hi' }], [], new AbortController().signal));
    assert.equal(lastBody!['user'], 'wire-user-1');
  });
});
