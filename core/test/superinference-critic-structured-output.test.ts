// ===========================================================================
// superinference-critic-structured-output.test.ts
//
// Parity unit U262 (gap: provider-native structured output / JSON schema for
// the LLM-judge auxiliary call). The critic's verdict must be requested via
// provider-native structured output so a formatting miss can no longer
// masquerade as a substantive negative verdict:
//
//   - ProviderConfig.responseFormat ({ type:'json_schema', schema, name? }) is
//     threaded into the AI SDK `output: Output.object({ schema })` option by
//     streamChatCompletion (parity: the reference's `outputFormat:
//     { type:'json_schema', schema }` on auxiliary queries).
//   - Critic.evaluate passes CRITIC_VERDICT_SCHEMA so the reviewer model is
//     constrained to emit schema-valid {approved,score,reason} JSON; extractJSON
//     stays as the fallback for providers that ignore structured output.
//
// ESM-coverage-safe: no source-text assertions; deterministic DI seams
// (streamTextImpl for the transport, the Critic's streamFn seam for the judge);
// no network.
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { streamChatCompletion } from '../src/provider';
import { Critic, CRITIC_VERDICT_SCHEMA } from '../src/superinference/critic';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

const PROVIDER: ProviderConfig = {
  provider: 'anthropic',
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'test-key',
  model: 'main-loop-model',
};
const MSGS: Message[] = [{ role: 'user', content: 'hi' }];

// A capturing AI SDK streamText impl: records the options object so a test can
// assert exactly which request fields were forwarded, then yields a terminal
// finish-step so the stream drains cleanly.
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

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _c of gen) { /* consume */ }
}

// The Critic's DI seam: a single-attempt stream impl that records the config it
// was handed (so we can assert responseFormat was threaded) and yields a valid
// verdict so evaluate() returns a parsed decision.
function makeCriticStream(seen: { configs: ProviderConfig[] }, chunks: StreamChunk[]): typeof streamChatCompletion {
  return async function* fake(config: ProviderConfig): AsyncGenerator<StreamChunk> {
    seen.configs.push(config);
    for (const c of chunks) yield c;
  } as never;
}

// ---------------------------------------------------------------------------
// Transport: ProviderConfig.responseFormat → AI SDK `output`
// ---------------------------------------------------------------------------
describe('streamChatCompletion — provider-native structured output', () => {
  async function outputFor(cfg: ProviderConfig): Promise<unknown> {
    const cap = captureStreamText();
    await drain(streamChatCompletion(cfg, MSGS, [], new AbortController().signal, { streamTextImpl: cap.fn as never }));
    return cap.calls[0].output;
  }

  it('threads config.responseFormat into the AI SDK output option', async () => {
    const output = await outputFor({
      ...PROVIDER,
      responseFormat: { type: 'json_schema', schema: CRITIC_VERDICT_SCHEMA, name: 'critic_verdict' },
    });
    assert.ok(output, 'expected an output specification to be forwarded');
    // It is an AI SDK Output.object spec (exposes the parse surface), not a bare
    // schema object passed through verbatim.
    assert.equal(typeof (output as { parseCompleteOutput?: unknown }).parseCompleteOutput, 'function');
  });

  it('forwards the output spec even without a schema name', async () => {
    const output = await outputFor({
      ...PROVIDER,
      responseFormat: { type: 'json_schema', schema: CRITIC_VERDICT_SCHEMA },
    });
    assert.ok(output, 'expected an output specification to be forwarded');
  });

  it('omits output when responseFormat is unset (byte-identical common case)', async () => {
    assert.equal(await outputFor(PROVIDER), undefined);
  });
});

// ---------------------------------------------------------------------------
// CRITIC_VERDICT_SCHEMA — the judge contract
// ---------------------------------------------------------------------------
describe('CRITIC_VERDICT_SCHEMA', () => {
  it('requires approved/score/reason and forbids extra keys (strict JSON mode)', () => {
    assert.deepEqual(CRITIC_VERDICT_SCHEMA.required, ['approved', 'score', 'reason']);
    assert.equal(CRITIC_VERDICT_SCHEMA.additionalProperties, false);
    const props = CRITIC_VERDICT_SCHEMA.properties as Record<string, { type: string }>;
    assert.equal(props.approved.type, 'boolean');
    assert.equal(props.score.type, 'number');
    assert.equal(props.reason.type, 'string');
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate → responseFormat threaded through the real retry wrapper
// (integration: Critic → streamChatCompletionWithRetry → streamImpl config)
// ---------------------------------------------------------------------------
describe('Critic.evaluate — constrains the reviewer to structured output', () => {
  it('passes CRITIC_VERDICT_SCHEMA as responseFormat on the auxiliary request', async () => {
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeCriticStream(seen, [
      { type: 'content_delta', text: JSON.stringify({ approved: true, score: 0.9, reason: 'ok' }) },
    ]);
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);

    // The verdict still parses (extractJSON fallback remains intact).
    assert.equal(result.approved, true);
    assert.equal(result.score, 0.9);
    // RED-GREEN DISCRIMINATOR: the reviewer request carried provider-native
    // structured output. Reverting the critic change (dropping responseFormat)
    // makes this assertion fail.
    const rf = seen.configs[0].responseFormat;
    assert.ok(rf, 'critic request must set responseFormat for provider-native structured output');
    assert.equal(rf!.type, 'json_schema');
    assert.equal(rf!.name, 'critic_verdict');
    assert.equal(rf!.schema, CRITIC_VERDICT_SCHEMA);
  });

  it('still returns a parsed verdict from a prose-wrapped JSON body (extractJSON fallback)', async () => {
    // Providers that ignore structured output may still wrap the JSON in prose;
    // the balanced-brace extractJSON fallback must keep working.
    const seen = { configs: [] as ProviderConfig[] };
    const stream = makeCriticStream(seen, [
      { type: 'content_delta', text: 'Here is my verdict: {"approved": false, "score": 0.2, "reason": "missing edge case"} done.' },
    ]);
    const critic = new Critic(0.05, 0.1, stream, '', { baseDelayMs: 0 });

    const result = await critic.evaluate('q', 'r', PROVIDER, new AbortController().signal);
    assert.equal(result.approved, false);
    assert.equal(result.score, 0.2);
    assert.equal(result.reason, 'missing edge case');
    // responseFormat was still requested even on the fallback path.
    assert.equal(seen.configs[0].responseFormat?.name, 'critic_verdict');
  });
});
