// ===========================================================================
// provider-u020-w67.test.ts
//
// Parity unit U020 (wave 67) — additive provider capabilities in
// core/core/src/provider.ts:
//   - API-enforced structured outputs: STRUCTURED_OUTPUTS_BETA constant,
//     modelSupportsStructuredOutputs() capability gate, and its wiring into
//     computeAnthropicBetas (env opt-in, model-gated, kill-switchable).
//     [gaps: constants/betas.ts L8 ; utils/betas.ts L142-157,L313-357]
//   - Per-tool strict / structured-outputs flag + the experimental-betas kill
//     switch choke point in toolDefinitionsToOpenAI.  [gap: utils/api.ts L180-260]
//   - tool_choice request parameter: normalizeToolChoice() validator + wiring
//     into the streamText call.  [gaps: claude.ts L679 ; sideQuery.ts]
//   - skipCacheWrite one-off cache-write suppression + the selectable global
//     prompt-cache strategy knob (tool_based | system_prompt | none).
//     [gaps: awaySummary.ts L56 ; query.ts ; logging.ts L46,L526-531]
//   - Gateway/proxy beta-rejection fallback that drops ALL beta headers
//     (disableBetas) via computeAnthropicBetas + the retry recovery path.
//     [gap: utils/betas.ts L313-357]
//
// ESM-coverage-safe: no source-text assertions; env is snapshotted + restored
// per test; the only injected dependency is the streamText seam.
// ===========================================================================
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  computeAnthropicBetas,
  modelSupportsStructuredOutputs,
  structuredOutputsEnabled,
  STRUCTURED_OUTPUTS_BETA,
  toolDefinitionsToOpenAI,
  normalizeToolChoice,
  resolvePromptCacheStrategy,
  PROMPT_CACHE_STRATEGY_ENV,
  streamChatCompletion,
  streamChatCompletionWithRetry,
} from '../src/provider';
import type { ProviderConfig, ToolDefinition, Message, StreamChunk } from '../src/types';

// Snapshot + restore every env var these features read.
const TRACKED_ENV = [
  'SUPERINFERENCE_STRUCTURED_OUTPUTS',
  'CLAUDE_CODE_STRUCTURED_OUTPUTS',
  'CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS',
  'SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS',
  PROMPT_CACHE_STRATEGY_ENV,
  'DISABLE_PROMPT_CACHING',
  'AMI_PROMPT_CACHE_TTL',
  'ANTHROPIC_BETAS',
  'SUPERINFERENCE_DISABLE_1M_CONTEXT',
  'DISABLE_INTERLEAVED_THINKING',
];
let savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  savedEnv = {};
  for (const k of TRACKED_ENV) savedEnv[k] = process.env[k];
  for (const k of TRACKED_ENV) delete process.env[k];
});
afterEach(() => {
  for (const k of TRACKED_ENV) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

const SUPPORTED = 'claude-sonnet-4-5';
const UNSUPPORTED_40 = 'claude-sonnet-4'; // Claude 4.0 — no structured outputs

// A streamText seam that records the exact options object it is called with and
// returns a trivial finishing stream, so we can assert what the wiring produced.
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

// ---------------------------------------------------------------------------
describe('modelSupportsStructuredOutputs', () => {
  it('accepts Opus 4.1+ and Sonnet/Haiku 4.5+', () => {
    for (const m of [
      'claude-sonnet-4-5', 'claude-sonnet-4-6', 'claude-sonnet-4-5-20251101',
      'claude-haiku-4-5', 'claude-opus-4-1', 'claude-opus-4-5', 'claude-opus-4-6',
    ]) {
      assert.equal(modelSupportsStructuredOutputs(m), true, m);
    }
  });

  it('rejects Claude 4.0 Sonnet/Opus, 3.x models, and non-Claude / invalid input', () => {
    for (const m of [
      'claude-sonnet-4', 'claude-opus-4', 'claude-3-5-sonnet', 'claude-3-opus',
      'gpt-4o', 'claude-haiku-4', '', 'x',
    ]) {
      assert.equal(modelSupportsStructuredOutputs(m), false, m);
    }
    assert.equal(modelSupportsStructuredOutputs(undefined as unknown as string), false);
  });
});

// ---------------------------------------------------------------------------
describe('structured-outputs beta wiring in computeAnthropicBetas', () => {
  it('is OFF by default even on a supported model (opt-in only)', () => {
    assert.equal(structuredOutputsEnabled(), false);
    assert.ok(!computeAnthropicBetas(SUPPORTED).includes(STRUCTURED_OUTPUTS_BETA));
  });

  it('is added when opted in on a supported model (both env spellings)', () => {
    process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS = '1';
    assert.ok(computeAnthropicBetas(SUPPORTED).includes(STRUCTURED_OUTPUTS_BETA));
    delete process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS;
    process.env.CLAUDE_CODE_STRUCTURED_OUTPUTS = '1';
    assert.ok(computeAnthropicBetas(SUPPORTED).includes(STRUCTURED_OUTPUTS_BETA));
  });

  it('is NOT added on an unsupported model even when opted in', () => {
    process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS = '1';
    assert.ok(!computeAnthropicBetas(UNSUPPORTED_40).includes(STRUCTURED_OUTPUTS_BETA));
  });

  it('is stripped by the experimental-betas kill switch', () => {
    process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS = '1';
    process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = '1';
    assert.ok(!computeAnthropicBetas(SUPPORTED).includes(STRUCTURED_OUTPUTS_BETA));
  });

  it('disableBetas returns an empty set (gateway-rejection full coverage) even when opted in', () => {
    process.env.SUPERINFERENCE_STRUCTURED_OUTPUTS = '1';
    process.env.ANTHROPIC_BETAS = 'user-escape-hatch';
    assert.deepEqual(computeAnthropicBetas(SUPPORTED, { cacheTtl: '1h', includeCacheTtlBeta: true, disableBetas: true }), []);
    // Sanity: without disableBetas the same call is non-empty.
    assert.ok(computeAnthropicBetas(SUPPORTED, { cacheTtl: '1h', includeCacheTtlBeta: true }).length > 0);
  });
});

// ---------------------------------------------------------------------------
describe('toolDefinitionsToOpenAI — per-tool strict + kill-switch strip', () => {
  const strictTool: ToolDefinition = {
    name: 'emit', description: 'emit', inputSchema: { type: 'object', properties: {} },
    isReadOnly: true, strict: true,
  };
  const looseTool: ToolDefinition = {
    name: 'plain', description: 'plain', inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
  };

  it('emits strict only for an opted-in tool on a structured-outputs-capable model', () => {
    const out = toolDefinitionsToOpenAI([strictTool, looseTool], SUPPORTED);
    assert.equal(out[0].function.strict, true);
    assert.equal(out[1].function.strict, undefined); // not opted in
  });

  it('omits strict on an unsupported model, and when no model is supplied', () => {
    assert.equal(toolDefinitionsToOpenAI([strictTool], UNSUPPORTED_40)[0].function.strict, undefined);
    assert.equal(toolDefinitionsToOpenAI([strictTool])[0].function.strict, undefined);
  });

  it('kill switch strips strict even for a capable model + opted-in tool', () => {
    process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = '1';
    assert.equal(toolDefinitionsToOpenAI([strictTool], SUPPORTED)[0].function.strict, undefined);
  });

  it('still emits object-rooted outputSchema (regression)', () => {
    const t: ToolDefinition = { ...looseTool, outputSchema: { type: 'object', properties: {} } };
    assert.deepEqual(toolDefinitionsToOpenAI([t], SUPPORTED)[0].function.outputSchema, { type: 'object', properties: {} });
  });
});

// ---------------------------------------------------------------------------
describe('normalizeToolChoice', () => {
  it('passes the string modes', () => {
    assert.equal(normalizeToolChoice('auto'), 'auto');
    assert.equal(normalizeToolChoice('required'), 'required');
    assert.equal(normalizeToolChoice('none'), 'none');
  });

  it('normalizes the OpenAI function form and the native tool form', () => {
    assert.deepEqual(normalizeToolChoice({ type: 'function', function: { name: 'bash' } }), { type: 'tool', toolName: 'bash' });
    assert.deepEqual(normalizeToolChoice({ type: 'tool', toolName: 'bash' }), { type: 'tool', toolName: 'bash' });
  });

  it('rejects unrecognized values as undefined', () => {
    for (const v of ['force', '', 42, null, undefined, {}, { type: 'tool' }, { type: 'function', function: {} }, { type: 'function' }]) {
      assert.equal(normalizeToolChoice(v), undefined, JSON.stringify(v));
    }
  });
});

// ---------------------------------------------------------------------------
describe('tool_choice wiring into streamChatCompletion', () => {
  const tool: ToolDefinition = { name: 'bash', description: 'run', inputSchema: { type: 'object', properties: {} }, isReadOnly: false };
  const base: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: 'https://api.openai.com/v1', model: 'gpt-4o' };

  it('forwards config.toolChoice to streamText when tools are present', async () => {
    const cap = captureStreamText();
    await drain(streamChatCompletion(
      { ...base, toolChoice: 'required' }, [{ role: 'user', content: 'hi' }], [tool],
      new AbortController().signal, { streamTextImpl: cap.fn as never },
    ));
    assert.equal(cap.calls[0].toolChoice, 'required');
  });

  it('omits toolChoice when no tools are advertised', async () => {
    const cap = captureStreamText();
    await drain(streamChatCompletion(
      { ...base, toolChoice: 'required' }, [{ role: 'user', content: 'hi' }], [],
      new AbortController().signal, { streamTextImpl: cap.fn as never },
    ));
    assert.equal('toolChoice' in cap.calls[0], false);
  });

  it('omits toolChoice when config does not set it', async () => {
    const cap = captureStreamText();
    await drain(streamChatCompletion(
      base, [{ role: 'user', content: 'hi' }], [tool],
      new AbortController().signal, { streamTextImpl: cap.fn as never },
    ));
    assert.equal('toolChoice' in cap.calls[0], false);
  });
});

// ---------------------------------------------------------------------------
describe('resolvePromptCacheStrategy', () => {
  it('defaults to tool_based when unset or junk', () => {
    assert.equal(resolvePromptCacheStrategy(), 'tool_based');
    assert.equal(resolvePromptCacheStrategy({ [PROMPT_CACHE_STRATEGY_ENV]: 'nonsense' }), 'tool_based');
  });
  it('honors the recognized values, case-insensitively', () => {
    assert.equal(resolvePromptCacheStrategy({ [PROMPT_CACHE_STRATEGY_ENV]: 'system_prompt' }), 'system_prompt');
    assert.equal(resolvePromptCacheStrategy({ [PROMPT_CACHE_STRATEGY_ENV]: 'NONE' }), 'none');
    assert.equal(resolvePromptCacheStrategy({ [PROMPT_CACHE_STRATEGY_ENV]: ' Tool_Based ' }), 'tool_based');
  });
});

// ---------------------------------------------------------------------------
describe('prompt-cache markers — strategy knob + skipCacheWrite', () => {
  const tool: ToolDefinition = { name: 'bash', description: 'run', inputSchema: { type: 'object', properties: {} }, isReadOnly: false };
  const anthropic: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: SUPPORTED };
  const msgs: Message[] = [{ role: 'system', content: 'sys' }, { role: 'user', content: 'hello' }];

  async function markers(cfg: ProviderConfig) {
    const cap = captureStreamText();
    await drain(streamChatCompletion(cfg, msgs, [tool], new AbortController().signal, { streamTextImpl: cap.fn as never }));
    const call = cap.calls[0];
    const toolsSet = call.tools as Record<string, { providerOptions?: { anthropic?: { cacheControl?: unknown } } }>;
    const messages = call.messages as Array<{ providerOptions?: { anthropic?: { cacheControl?: unknown } } }>;
    return {
      toolMarked: !!toolsSet?.bash?.providerOptions?.anthropic?.cacheControl,
      msgMarked: !!messages?.[0]?.providerOptions?.anthropic?.cacheControl,
    };
  }

  it('tool_based (default) marks BOTH the tools block and the message prefix', async () => {
    const r = await markers(anthropic);
    assert.deepEqual(r, { toolMarked: true, msgMarked: true });
  });

  it('system_prompt strategy marks the message prefix but NOT the tools block', async () => {
    process.env[PROMPT_CACHE_STRATEGY_ENV] = 'system_prompt';
    assert.deepEqual(await markers(anthropic), { toolMarked: false, msgMarked: true });
  });

  it('none strategy writes no cache markers at all', async () => {
    process.env[PROMPT_CACHE_STRATEGY_ENV] = 'none';
    assert.deepEqual(await markers(anthropic), { toolMarked: false, msgMarked: false });
  });

  it('skipCacheWrite suppresses all markers for the one request', async () => {
    assert.deepEqual(await markers({ ...anthropic, skipCacheWrite: true }), { toolMarked: false, msgMarked: false });
  });
});

// ---------------------------------------------------------------------------
describe('gateway beta-rejection fallback drops ALL betas on retry', () => {
  const config: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant', baseUrl: '', model: SUPPORTED, cacheTtl: '1h' };

  it('sets disableBetas on the retry after a 400 anthropic-beta rejection', async () => {
    const seen: Array<boolean | undefined> = [];
    let attempt = 0;
    const streamImpl = (async function* (cfg: ProviderConfig): AsyncGenerator<StreamChunk> {
      seen.push(cfg.disableBetas);
      if (attempt++ === 0) {
        yield { type: 'error', error: 'HTTP 400: unsupported anthropic-beta header: structured-outputs' };
        return;
      }
      yield { type: 'done', finishReason: 'stop' };
    }) as unknown as typeof streamChatCompletion;

    const chunks = await drain(streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], new AbortController().signal,
      { streamImpl, maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2 },
    ));
    assert.ok(seen.length >= 2, 'should have retried after the 400 gateway rejection');
    assert.notEqual(seen[0], true);         // first attempt: betas on the wire
    assert.equal(seen[1], true);            // retry: all betas dropped
    assert.equal(chunks.at(-1)!.type, 'done');
  });
});
