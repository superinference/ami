import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import {
  buildThinkingOptions,
  thinkingDisabledByEnv,
  adaptiveThinkingDisabledByEnv,
  DISABLE_THINKING_ENV_VARS,
  DISABLE_ADAPTIVE_THINKING_ENV_VARS,
  resolveExtraMetadata,
  resolveMetadataUserIdValue,
  EXTRA_METADATA_ENV_VARS,
  streamChatCompletionWithRetry,
  STREAM_STALL_THRESHOLD_MS,
} from '../src/provider';
import { getLogPath, __resetLoggerForTest } from '../src/logger';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w77 — provider-layer request knobs + streaming-stall telemetry
// (core/core/src/provider.ts). Covered parity gaps:
//   - Knobs to disable thinking / disable adaptive-thinking (force budget mode)
//   - Request metadata param + CLAUDE_CODE_EXTRA_METADATA knob (arbitrary extra)
//   - Soft streaming-stall / pre-kill idle warning (slow-but-alive stream)
//   - Mid-stream stall detection/telemetry (inter-chunk gap counting + summary)
// ---------------------------------------------------------------------------

const ALL_ENV = [
  ...DISABLE_THINKING_ENV_VARS,
  ...DISABLE_ADAPTIVE_THINKING_ENV_VARS,
  ...EXTRA_METADATA_ENV_VARS,
  'AMI_METADATA_USER_ID',
  'CLAUDE_CODE_METADATA_USER_ID',
];
function clearEnv(): void {
  for (const k of ALL_ENV) delete process.env[k];
}

const ADAPTIVE_MODEL = 'claude-opus-4-8';
const ALWAYS_ON_MODEL = 'claude-opus-5';
const THINKING: { enabled: true; level: 'high' } = { enabled: true, level: 'high' };

describe('U020/w77 — disable-thinking / disable-adaptive-thinking env knobs', () => {
  beforeEach(clearEnv);
  afterEach(clearEnv);

  it('thinkingDisabledByEnv honors both the AMI and Claude spellings', () => {
    assert.equal(thinkingDisabledByEnv({}), false);
    assert.equal(thinkingDisabledByEnv({ AMI_DISABLE_THINKING: '1' }), true);
    assert.equal(thinkingDisabledByEnv({ CLAUDE_CODE_DISABLE_THINKING: 'true' }), true);
    assert.equal(thinkingDisabledByEnv({ CLAUDE_CODE_DISABLE_THINKING: '0' }), false);
  });

  it('adaptiveThinkingDisabledByEnv honors both spellings', () => {
    assert.equal(adaptiveThinkingDisabledByEnv({}), false);
    assert.equal(adaptiveThinkingDisabledByEnv({ AMI_DISABLE_ADAPTIVE_THINKING: '1' }), true);
    assert.equal(adaptiveThinkingDisabledByEnv({ CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING: 'yes' }), true);
  });

  it('DISABLE_THINKING forces reasoning off for a normal reasoning model', () => {
    // Baseline: thinking is on → adaptive shape is emitted.
    assert.deepEqual(buildThinkingOptions(ADAPTIVE_MODEL, THINKING), {
      providerOptions: { anthropic: { thinking: { type: 'adaptive' }, effort: 'high' } },
    });
    // RED (pre-fix): the env knob was not consulted, so the adaptive shape still
    // came back. GREEN: thinking is forced off → no thinking options.
    process.env.CLAUDE_CODE_DISABLE_THINKING = '1';
    assert.deepEqual(buildThinkingOptions(ADAPTIVE_MODEL, THINKING), {});
  });

  it('DISABLE_THINKING on an always-on model still sends the minimal config (never a 400)', () => {
    process.env.AMI_DISABLE_THINKING = '1';
    // An always-on-thinking model rejects a no-thinking request, so disable must
    // route through the not-enabled path that still emits the minimal effort floor.
    assert.deepEqual(buildThinkingOptions(ALWAYS_ON_MODEL, THINKING), {
      providerOptions: { anthropic: { thinking: { type: 'adaptive' }, effort: 'low' } },
    });
  });

  it('DISABLE_ADAPTIVE_THINKING forces the legacy fixed-budget shape on an adaptive model', () => {
    process.env.CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING = '1';
    // RED (pre-fix): the adaptive branch ran unconditionally. GREEN: it is skipped
    // and the legacy { type:'enabled', budgetTokens } shape is produced instead.
    const opts = buildThinkingOptions(ADAPTIVE_MODEL, THINKING) as {
      providerOptions: { anthropic: { thinking: { type: string; budgetTokens: number } } };
    };
    assert.equal(opts.providerOptions.anthropic.thinking.type, 'enabled');
    assert.ok(opts.providerOptions.anthropic.thinking.budgetTokens >= 1024);
  });
});

describe('U020/w77 — extra request metadata (CLAUDE_CODE_EXTRA_METADATA knob)', () => {
  beforeEach(clearEnv);
  afterEach(clearEnv);

  it('resolveExtraMetadata parses a JSON object from either env spelling; config wins', () => {
    assert.equal(resolveExtraMetadata({ model: 'm' } as ProviderConfig, {}), undefined);
    assert.deepEqual(
      resolveExtraMetadata({ model: 'm' } as ProviderConfig, { AMI_EXTRA_METADATA: '{"a":1}' }),
      { a: 1 },
    );
    assert.deepEqual(
      resolveExtraMetadata({ model: 'm' } as ProviderConfig, { CLAUDE_CODE_EXTRA_METADATA: '{"b":2}' }),
      { b: 2 },
    );
    // Explicit config.metadata.extra takes precedence over the env knob.
    assert.deepEqual(
      resolveExtraMetadata(
        { model: 'm', metadata: { extra: { c: 3 } } } as ProviderConfig,
        { AMI_EXTRA_METADATA: '{"a":1}' },
      ),
      { c: 3 },
    );
  });

  it('resolveExtraMetadata ignores a non-object / invalid-JSON knob without throwing', () => {
    assert.equal(resolveExtraMetadata({ model: 'm' } as ProviderConfig, { AMI_EXTRA_METADATA: 'nope' }), undefined);
    assert.equal(resolveExtraMetadata({ model: 'm' } as ProviderConfig, { AMI_EXTRA_METADATA: '[1,2]' }), undefined);
    assert.equal(resolveExtraMetadata({ model: 'm' } as ProviderConfig, { AMI_EXTRA_METADATA: '   ' }), undefined);
  });

  it('resolveMetadataUserIdValue folds extra + user id into a single JSON blob', () => {
    // No extra → plain user id (unchanged behavior).
    assert.equal(
      resolveMetadataUserIdValue({ model: 'm', metadata: { userId: 'u1' } } as ProviderConfig, {}),
      'u1',
    );
    // Extra present → JSON object string carrying extra fields + user_id.
    const combined = resolveMetadataUserIdValue(
      { model: 'm', metadata: { userId: 'u1' } } as ProviderConfig,
      { CLAUDE_CODE_EXTRA_METADATA: '{"team":"x"}' },
    );
    assert.deepEqual(JSON.parse(combined!), { team: 'x', user_id: 'u1' });
    // Extra with no user id → extra only.
    const extraOnly = resolveMetadataUserIdValue(
      { model: 'm' } as ProviderConfig,
      { AMI_EXTRA_METADATA: '{"deploy":"prod"}' },
    );
    assert.deepEqual(JSON.parse(extraOnly!), { deploy: 'prod' });
    // Nothing set → undefined (no metadata attached).
    assert.equal(resolveMetadataUserIdValue({ model: 'm' } as ProviderConfig, {}), undefined);
  });
});

// --- streaming-stall telemetry ---------------------------------------------

const CFG: ProviderConfig = { baseUrl: 'http://127.0.0.1:1', apiKey: 'k', model: 'test-model' };
const MSG: Message[] = [{ role: 'user', content: 'hi' }];

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _ of gen) { /* consume */ }
}
function readLog(): string {
  const p = getLogPath();
  return fs.existsSync(p) ? fs.readFileSync(p, 'utf-8') : '';
}

describe('U020/w77 — streaming stall telemetry', () => {
  beforeEach(() => {
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    delete process.env.AMI_DEBUG_TO_STDERR;
    delete process.env.AMI_DEBUG_COMPONENTS;
    __resetLoggerForTest();
  });

  it('logs inter-chunk stalls and a completion summary via an injected clock', async () => {
    const before = readLog();
    // Injected clock: a 40s jump between the two content chunks exceeds the 30s
    // STREAM_STALL_THRESHOLD_MS, so exactly one stall is counted + summarized.
    let t = 1_000;
    const times = [t, (t += STREAM_STALL_THRESHOLD_MS + 10_000), (t += 1_000), (t += 1_000)];
    let i = 0;
    const nowFn = () => times[Math.min(i++, times.length - 1)];

    const fakeStream = (async function* () {
      yield { type: 'content_delta', text: 'a' } as StreamChunk;
      yield { type: 'content_delta', text: 'b' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof import('../src/provider').streamChatCompletion;

    await drain(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        streamImpl: fakeStream,
        nowFn,
      }),
    );

    const written = readLog().slice(before.length);
    // RED (pre-fix): no stall telemetry existed. GREEN: a per-gap stall line and
    // an end-of-stream summary are recorded, distinct from the hard idle-abort.
    assert.match(written, /Streaming stall: \d+s gap between stream events \(stall #1\)/);
    assert.match(written, /Streaming stall summary: 1 stall\(s\)/);
  });

  it('does NOT record a stall when chunks arrive within the threshold', async () => {
    const before = readLog();
    let t = 1_000;
    const nowFn = () => (t += 1_000); // 1s gaps, well under the 30s threshold
    const fakeStream = (async function* () {
      yield { type: 'content_delta', text: 'a' } as StreamChunk;
      yield { type: 'content_delta', text: 'b' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof import('../src/provider').streamChatCompletion;

    await drain(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        streamImpl: fakeStream,
        nowFn,
      }),
    );
    const written = readLog().slice(before.length);
    assert.doesNotMatch(written, /Streaming stall/);
  });

  it('emits a pre-kill idle WARNING at half the idle timeout before the hard abort', async () => {
    const before = readLog();
    // Stream that ends ONLY when the idle watchdog aborts it: the half-idle warning
    // (idleMs/2) always fires first. idleMs is small so the test is fast.
    const fakeStream = ((_c: unknown, _m: unknown, _t: unknown, signal: AbortSignal) =>
      (async function* () {
        await new Promise<void>((res) => {
          if (signal.aborted) return res();
          signal.addEventListener('abort', () => res(), { once: true });
        });
      })()) as unknown as typeof import('../src/provider').streamChatCompletion;

    await drain(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        streamImpl: fakeStream,
        idleTimeoutMs: 80,
        maxRetries: 0,
      }),
    );
    const written = readLog().slice(before.length);
    // RED (pre-fix): the idle watchdog killed a quiet stream with no prior warning.
    // GREEN: a warning breadcrumb is logged at idleMs/2 before the abort.
    assert.match(written, /Streaming idle warning: no chunks received/);
  });
});
