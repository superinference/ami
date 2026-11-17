import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { Critic, isCriticDisabledByEnv } from '../src/superinference/critic';
import { SuperInferenceEngine } from '../src/superinference';
import { REDACTED_THINKING_PLACEHOLDER } from '../src/provider';
import type { StreamChunk, ProviderConfig } from '../src/types';

const PROVIDER: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'test-key',
  model: 'main-loop-model',
};

// A single-attempt stream impl (the Critic's DI seam / the retry wrapper's streamImpl)
// that yields the given chunks for THIS attempt. Captured config lets a test assert the
// model the critic actually requested.
function makeStream(
  chunksPerAttempt: StreamChunk[][],
  seen: { configs: ProviderConfig[] },
): typeof import('../src/provider').streamChatCompletion {
  let attempt = 0;
  return async function* fake(config: ProviderConfig): AsyncGenerator<StreamChunk> {
    seen.configs.push(config);
    const chunks = chunksPerAttempt[Math.min(attempt, chunksPerAttempt.length - 1)];
    attempt += 1;
    for (const c of chunks) yield c;
  } as never;
}

function jsonChunk(obj: unknown): StreamChunk {
  return { type: 'content_delta', text: JSON.stringify(obj) };
}

// ---------------------------------------------------------------------------
// Gap: dedicated reviewer model (advisorModel parity) threaded into evaluate()
// ---------------------------------------------------------------------------
describe('Critic — dedicated reviewer model', () => {
  it('uses the critic model (not the main-loop model) for the request AND attribution', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[jsonChunk({ approved: true, score: 0.9, reason: 'ok' })]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, 'reviewer-model', { baseDelayMs: 0 });
    assert.equal(critic.criticModelId, 'reviewer-model');

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);

    // The wire request targeted the reviewer model, overriding provider.model.
    assert.equal(seen.configs[0].model, 'reviewer-model');
    // The verdict is attributed to the reviewer model.
    assert.equal(result.model, 'reviewer-model');
    assert.equal(result.approved, true);
  });

  it('inherits the main-loop model when criticModel is empty (default)', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[jsonChunk({ approved: false, score: 0.2, reason: 'no' })]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    assert.equal(seen.configs[0].model, 'main-loop-model');
    assert.equal(result.model, 'main-loop-model');
  });

  it('SuperInferenceEngine threads config.criticModel into its Critic', () => {
    const si = new SuperInferenceEngine({ criticModel: 'cheap-reviewer' });
    assert.equal(si.critic.criticModelId, 'cheap-reviewer');
  });
});

// ---------------------------------------------------------------------------
// Gap: per-auxiliary-model usage breakdown (reviewer/advisor spend surfaced
// separately — parity: Claude's getAdvisorUsage, usage tagged with model)
// ---------------------------------------------------------------------------
describe('Critic — auxiliary-model usage attribution', () => {
  function usageDone(usage: { promptTokens: number; completionTokens: number; totalTokens: number }): StreamChunk {
    return { type: 'done', finishReason: 'stop', usage: { ...usage, reasoningTokens: 0 } };
  }

  it('captures the critic call usage tagged with the resolved reviewer model', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[
        jsonChunk({ approved: true, score: 0.9, reason: 'ok' }),
        usageDone({ promptTokens: 120, completionTokens: 30, totalTokens: 150 }),
      ]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, 'reviewer-model', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);

    assert.ok(result.usage, 'critic must surface its auxiliary token usage');
    assert.equal(result.usage?.model, 'reviewer-model', 'usage is attributed to the reviewer model');
    assert.equal(result.usage?.promptTokens, 120);
    assert.equal(result.usage?.completionTokens, 30);
    assert.equal(result.usage?.totalTokens, 150);
  });

  it('attributes usage to the inherited main-loop model when no critic model is set', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[
        jsonChunk({ approved: false, score: 0.2, reason: 'no' }),
        usageDone({ promptTokens: 10, completionTokens: 5, totalTokens: 15 }),
      ]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    assert.equal(result.usage?.model, 'main-loop-model');
    assert.equal(result.usage?.totalTokens, 15);
  });

  it('omits usage when the provider reports none (no done/usage chunk)', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[jsonChunk({ approved: true, score: 0.7, reason: 'ok' })]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });
    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    assert.equal(result.usage, undefined);
    assert.equal(result.approved, true);
  });
});

// ---------------------------------------------------------------------------
// Gap: redacted-but-complete review (the reviewer's reasoning came back
// encrypted/redacted yet a verdict still rendered — parity: Claude's
// redacted-review completion state; the score/approval stand)
// ---------------------------------------------------------------------------
describe('Critic — redacted-but-complete review', () => {
  function redactedThinking(): StreamChunk {
    return { type: 'thinking_delta', text: `${REDACTED_THINKING_PLACEHOLDER}\n` };
  }

  it('flags redacted when a redacted-thinking chunk precedes a valid verdict', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[
        redactedThinking(),
        jsonChunk({ approved: true, score: 0.85, reason: 'ok' }),
      ]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    // A redacted review is a COMPLETE review: verdict stands, only rationale withheld.
    assert.equal(result.redacted, true);
    assert.equal(result.approved, true);
    assert.equal(result.score, 0.85);
    assert.notEqual(result.unavailable, true);
  });

  it('does NOT flag redacted on a normal (non-redacted) review', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[
        { type: 'thinking_delta', text: 'ordinary reasoning\n' },
        jsonChunk({ approved: true, score: 0.9, reason: 'ok' }),
      ]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    assert.notEqual(result.redacted, true);
  });
});

// ---------------------------------------------------------------------------
// Gap: retry transient capacity/connection errors on the auxiliary critic call
// ---------------------------------------------------------------------------
describe('Critic — transient-error retry', () => {
  it('retries a transient HTTP error then returns the eventual verdict', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    // Attempt 1: a retryable 503 error chunk. Attempt 2: a valid verdict.
    const stream = makeStream(
      [
        [{ type: 'error', error: 'API error: HTTP 503 service unavailable' }],
        [jsonChunk({ approved: true, score: 0.88, reason: 'passed on retry' })],
      ],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { maxRetries: 3, baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);

    // RED (pre-fix, single-attempt streamChatCompletion): the 503 failed closed with
    // unavailable:true. GREEN: the retry wrapper re-issued the call and got the verdict.
    assert.equal(seen.configs.length, 2, 'should have retried exactly once');
    assert.equal(result.approved, true);
    assert.equal(result.score, 0.88);
    assert.notEqual(result.unavailable, true);
  });

  it('fails closed as stream_error once retries are exhausted', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeStream(
      [[{ type: 'error', error: 'API error: HTTP 503 service unavailable' }]],
      seen,
    );
    const critic = new Critic(0.05, 0.1, stream, '', { maxRetries: 2, baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    // Retried maxRetries+1 total attempts, all 503, then surfaced unavailable.
    assert.equal(seen.configs.length, 3);
    assert.equal(result.unavailable, true);
    assert.equal(result.errorCode, 'stream_error');
    assert.equal(result.approved, false);
  });
});

// ---------------------------------------------------------------------------
// Gap: env-var kill switch for the critic/advisor
// ---------------------------------------------------------------------------
describe('Critic — env kill switch', () => {
  const saved = {
    a: process.env.SUPERINFERENCE_DISABLE_CRITIC,
    b: process.env.CLAUDE_CODE_DISABLE_CRITIC,
  };
  beforeEach(() => {
    delete process.env.SUPERINFERENCE_DISABLE_CRITIC;
    delete process.env.CLAUDE_CODE_DISABLE_CRITIC;
  });
  afterEach(() => {
    if (saved.a === undefined) delete process.env.SUPERINFERENCE_DISABLE_CRITIC;
    else process.env.SUPERINFERENCE_DISABLE_CRITIC = saved.a;
    if (saved.b === undefined) delete process.env.CLAUDE_CODE_DISABLE_CRITIC;
    else process.env.CLAUDE_CODE_DISABLE_CRITIC = saved.b;
  });

  it('isCriticDisabledByEnv honors both the AMI name and the Claude-Code alias', () => {
    assert.equal(isCriticDisabledByEnv({} as NodeJS.ProcessEnv), false);
    assert.equal(isCriticDisabledByEnv({ SUPERINFERENCE_DISABLE_CRITIC: '1' } as NodeJS.ProcessEnv), true);
    assert.equal(isCriticDisabledByEnv({ CLAUDE_CODE_DISABLE_CRITIC: '1' } as NodeJS.ProcessEnv), true);
    // A non-'1' value does not disable (parity with web_fetch's exact-'1' check).
    assert.equal(isCriticDisabledByEnv({ SUPERINFERENCE_DISABLE_CRITIC: 'true' } as NodeJS.ProcessEnv), false);
  });

  it('SuperInferenceEngine forces useLLMCritic off when the kill switch is set', () => {
    process.env.SUPERINFERENCE_DISABLE_CRITIC = '1';
    const si = new SuperInferenceEngine({ useLLMCritic: true, enabled: true });
    // The engine's tool-observe gate reads config.useLLMCritic, so forcing it false here
    // makes the whole engine skip the auxiliary critic call end-to-end.
    assert.equal(si.config.useLLMCritic, false);
  });

  it('SuperInferenceEngine keeps useLLMCritic on when the kill switch is unset', () => {
    const si = new SuperInferenceEngine({ useLLMCritic: true, enabled: true });
    assert.equal(si.config.useLLMCritic, true);
  });
});
