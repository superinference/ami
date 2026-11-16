// ===========================================================================
// provider-u020-w69.test.ts
//
// Parity unit U020 (wave 69) — additive provider-transport capabilities in
// core/core/src/provider.ts, each mirroring a Claude Code reference:
//
//   - stop_sequences generation control: ProviderConfig.stopSequences threaded
//     into the AI SDK streamText call so a caller can halt generation on a
//     sentinel string. [ref: utils/sideQuery.ts L60-61,192]
//
//   - Client request-id surfaced on a terminal API failure for self-service log
//     correlation: resolveClientRequestId() mints/reuses a stable id, pins it to
//     extraHeaders so the SAME id rides the wire, and appendRequestIdToError()
//     decorates terminal error chunks. [ref: services/api/logging.ts L262-301]
//
//   - Actionable SSL/TLS error hint extended with custom-CA + mutual-TLS remedies
//     via formatSslError(). [ref: utils/caCerts.ts, utils/mtls.ts]
//
//   - Force-enable-effort env override (SUPERINFERENCE_ALWAYS_ENABLE_EFFORT /
//     CLAUDE_CODE_ALWAYS_ENABLE_EFFORT) so an unrecognized 3P reasoning model
//     still receives reasoning_effort. [ref: utils/effort.ts L22-49]
//
//   - Per-attempt retry diagnostic (status + sanitized reason) in the debug log.
//     [ref: services/api/withRetry.ts L256-259]
//
// ESM-coverage-safe: no source-text assertions; env snapshotted + restored;
// deterministic request-id via the requestIdHooks seam; the streamText /
// streamImpl seams drive the stream deterministically with no network.
// ===========================================================================
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  streamChatCompletion,
  streamChatCompletionWithRetry,
  resolveClientRequestId,
  appendRequestIdToError,
  formatSslError,
  buildThinkingOptions,
  alwaysEnableEffort,
  ALWAYS_ENABLE_EFFORT_ENV_VARS,
  requestIdHooks,
  REQUEST_ID_HEADER,
} from '../src/provider';
import { MTLS_ENV } from '../src/utils/mtls';
import { fsHooks } from '../src/logger';
import type { ProviderConfig, Message, StreamChunk, ThinkingConfig } from '../src/types';

const TRACKED_ENV = [
  ...ALWAYS_ENABLE_EFFORT_ENV_VARS,
  'AMI_DEBUG_LOG_LEVEL', 'CLAUDE_CODE_DEBUG_LOG_LEVEL', 'AMI_DISABLE_DEBUG_LOG',
  'AMI_DEBUG_TO_STDERR', 'CLAUDE_CODE_DEBUG_TO_STDERR', 'AMI_DEBUG_COMPONENTS',
];
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
  mock.restoreAll();
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

const anthropic: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: 'claude-sonnet-4-5' };
const msgs: Message[] = [{ role: 'user', content: 'hi' }];

// ===========================================================================
// stop_sequences — generation stop-string parameter
// ===========================================================================
describe('stop_sequences forwarding into streamChatCompletion', () => {
  async function stopSeqFor(cfg: ProviderConfig) {
    const cap = captureStreamText();
    await drain(streamChatCompletion(cfg, msgs, [], new AbortController().signal, { streamTextImpl: cap.fn as never }));
    return cap.calls[0].stopSequences as string[] | undefined;
  }

  it('forwards config.stopSequences verbatim to the AI SDK', async () => {
    assert.deepEqual(await stopSeqFor({ ...anthropic, stopSequences: ['</answer>', 'STOP'] }), ['</answer>', 'STOP']);
  });

  it('omits stopSequences when unset (byte-identical common case)', async () => {
    assert.equal(await stopSeqFor(anthropic), undefined);
  });

  it('omits stopSequences when given an empty array', async () => {
    assert.equal(await stopSeqFor({ ...anthropic, stopSequences: [] }), undefined);
  });
});

// ===========================================================================
// resolveClientRequestId / appendRequestIdToError
// ===========================================================================
describe('resolveClientRequestId', () => {
  it('mints a fresh id for a first-party provider', () => {
    const id = resolveClientRequestId({}, 'anthropic');
    assert.ok(id && id.length > 0);
  });

  it('returns undefined for a managed gateway (no unknown header)', () => {
    assert.equal(resolveClientRequestId({}, 'amazon-bedrock'), undefined);
    assert.equal(resolveClientRequestId({}, 'anthropic-vertex'), undefined);
  });

  it('reuses a caller-provided request-id header (case-insensitive)', () => {
    const id = resolveClientRequestId({ extraHeaders: { 'X-SuperInference-Request-Id': 'caller-123' } }, 'anthropic');
    assert.equal(id, 'caller-123');
  });

  it('reuses the caller id even on a gateway provider', () => {
    const id = resolveClientRequestId({ extraHeaders: { [REQUEST_ID_HEADER]: 'caller-9' } }, 'amazon-bedrock');
    assert.equal(id, 'caller-9');
  });
});

describe('appendRequestIdToError', () => {
  it('appends the id + a correlation hint to an error message', () => {
    const out = appendRequestIdToError('Max retries exceeded', 'req-abc');
    assert.match(out, /Max retries exceeded/);
    assert.match(out, new RegExp(`${REQUEST_ID_HEADER}=req-abc`));
    assert.match(out, /provider\/gateway/);
  });

  it('is a no-op when there is no id', () => {
    assert.equal(appendRequestIdToError('boom', undefined), 'boom');
  });

  it('never appends twice (idempotent)', () => {
    const once = appendRequestIdToError('boom', 'req-x');
    assert.equal(appendRequestIdToError(once, 'req-x'), once);
  });

  it('handles an undefined error body', () => {
    assert.match(appendRequestIdToError(undefined, 'req-y'), /req-y/);
  });
});

// ===========================================================================
// Integration — request-id surfaced on a terminal failure through the retry loop
// ===========================================================================
describe('streamChatCompletionWithRetry — request-id surfacing', () => {
  it('appends the client request-id to a non-retryable terminal error', async () => {
    mock.method(requestIdHooks, 'generate', () => 'fixed-req-id');
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      yield { type: 'error', error: 'HTTP 400: malformed request' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      anthropic, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    const last = chunks.at(-1)!;
    assert.equal(last.type, 'error');
    assert.match(last.error ?? '', /HTTP 400: malformed request/);
    assert.match(last.error ?? '', new RegExp(`${REQUEST_ID_HEADER}=fixed-req-id`));
  });

  it('appends the id to a max-retries-exceeded terminal error', async () => {
    mock.method(requestIdHooks, 'generate', () => 'retry-exhausted-id');
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      yield { type: 'error', error: 'HTTP 503: service unavailable' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      anthropic, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    assert.match(chunks.at(-1)!.error ?? '', /retry-exhausted-id/);
  });

  it('does NOT append an id for a managed gateway provider', async () => {
    mock.method(requestIdHooks, 'generate', () => 'should-not-appear');
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      yield { type: 'error', error: 'HTTP 400: malformed request' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      { ...anthropic, provider: 'amazon-bedrock' }, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    assert.doesNotMatch(chunks.at(-1)!.error ?? '', /should-not-appear/);
    assert.doesNotMatch(chunks.at(-1)!.error ?? '', new RegExp(REQUEST_ID_HEADER));
  });

  it('does NOT append an id to a user-driven abort', async () => {
    mock.method(requestIdHooks, 'generate', () => 'abort-id');
    const ctrl = new AbortController();
    ctrl.abort();
    const chunks = await drain(streamChatCompletionWithRetry(
      anthropic, msgs, [], ctrl.signal,
      { streamImpl: (async function* () { /* never reached */ }) as never, maxRetries: 1 },
    ));
    assert.equal(chunks.at(-1)!.error, 'Request aborted');
  });
});

// ===========================================================================
// formatSslError — extended remedy enumeration (custom CA + mutual TLS)
// ===========================================================================
describe('formatSslError', () => {
  it('names the custom-CA trust affordance', () => {
    const msg = formatSslError('CERT_HAS_EXPIRED');
    assert.match(msg, new RegExp(MTLS_ENV.ca));
  });

  it('names the mutual-TLS client-certificate remedy', () => {
    const msg = formatSslError('handshake failure');
    assert.match(msg, new RegExp(MTLS_ENV.cert));
    assert.match(msg, new RegExp(MTLS_ENV.key));
    assert.match(msg, /mutual TLS/i);
  });

  it('keeps the original NODE_EXTRA_CA_CERTS hint', () => {
    assert.match(formatSslError('x'), /NODE_EXTRA_CA_CERTS/);
  });

  it('surfaces the extended hint end-to-end on an SSL stream error', async () => {
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      throw new Error('self signed certificate in certificate chain');
    }) as unknown as typeof streamChatCompletion;
    await assert.rejects(
      drain(streamChatCompletionWithRetry(anthropic, msgs, [], new AbortController().signal, { streamImpl, maxRetries: 1, baseDelayMs: 1 })),
      new RegExp(MTLS_ENV.cert),
    );
  });
});

// ===========================================================================
// Force-enable-effort env override
// ===========================================================================
describe('alwaysEnableEffort + buildThinkingOptions force-effort', () => {
  const thinking: ThinkingConfig = { enabled: true, level: 'high' };
  // A model name AMI does not statically recognize as a reasoning model.
  const unknownModel = 'acme-reasoner-x1';

  it('alwaysEnableEffort is false by default', () => {
    assert.equal(alwaysEnableEffort({}), false);
  });

  it('alwaysEnableEffort honors the AMI-native knob', () => {
    assert.equal(alwaysEnableEffort({ [ALWAYS_ENABLE_EFFORT_ENV_VARS[0]]: '1' }), true);
  });

  it('alwaysEnableEffort honors the Claude-compatible alias', () => {
    assert.equal(alwaysEnableEffort({ [ALWAYS_ENABLE_EFFORT_ENV_VARS[1]]: 'true' }), true);
  });

  it('emits NO effort for an unrecognized model by default', () => {
    assert.deepEqual(buildThinkingOptions(unknownModel, thinking), {});
  });

  it('forces reasoning_effort for an unrecognized model when the knob is set', () => {
    process.env[ALWAYS_ENABLE_EFFORT_ENV_VARS[0]] = '1';
    const opts = buildThinkingOptions(unknownModel, thinking) as {
      providerOptions?: { openai?: { reasoningEffort?: string } };
    };
    assert.equal(opts.providerOptions?.openai?.reasoningEffort, 'high');
  });

  it('maps the max thinking level onto high (API ceiling)', () => {
    process.env[ALWAYS_ENABLE_EFFORT_ENV_VARS[1]] = 'yes';
    const opts = buildThinkingOptions(unknownModel, { enabled: true, level: 'max' }) as {
      providerOptions?: { openai?: { reasoningEffort?: string } };
    };
    assert.equal(opts.providerOptions?.openai?.reasoningEffort, 'high');
  });

  it('does not force effort when thinking is disabled', () => {
    process.env[ALWAYS_ENABLE_EFFORT_ENV_VARS[0]] = '1';
    assert.deepEqual(buildThinkingOptions(unknownModel, { enabled: false, level: 'high' }), {});
  });
});

// ===========================================================================
// Per-attempt retry diagnostic debug log (#77)
// ===========================================================================
describe('per-attempt retry diagnostic', () => {
  it('writes a status + reason line tied to the attempt number on a retry', async () => {
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, data: string) => { lines.push(data); });

    let attempt = 0;
    const streamImpl = (async function* (): AsyncGenerator<StreamChunk> {
      attempt++;
      if (attempt === 1) { yield { type: 'error', error: 'HTTP 503: service unavailable' }; return; }
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;

    await drain(streamChatCompletionWithRetry(
      anthropic, msgs, [], new AbortController().signal,
      { streamImpl, maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    const retryLine = lines.find(l => l.includes('API retry (attempt 1/2)'));
    assert.ok(retryLine, 'a per-attempt retry diagnostic was logged');
    assert.match(retryLine!, /status=503/);
    assert.match(retryLine!, /service unavailable/);
  });
});
