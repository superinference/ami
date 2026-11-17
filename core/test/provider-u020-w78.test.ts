import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'node:http';
import {
  withEagerToolStreaming,
  resolveModel,
  fineGrainedToolStreamingEnabled,
  sleepWithRetryHeartbeat,
  RETRY_HEARTBEAT_INTERVAL_MS,
  streamChatCompletionWithRetry,
  retryHooks,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk, ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w78 — provider.ts parity gaps:
//   (1) Per-tool `eager_input_streaming` opt-in on a ToolDefinition — fine-grained
//       tool streaming for just the flagged tools even when the process-wide knob
//       is off (Claude's per-tool eager_input_streaming on the cached tool schema).
//   (2) Heartbeat / live-countdown re-emission during a long retry backoff — a
//       multi-minute wait is sliced into RETRY_HEARTBEAT_INTERVAL_MS chunks so the
//       host sees periodic activity and the UI counts down (Claude's withRetry
//       HEARTBEAT_INTERVAL_MS chunked sleep).
// ---------------------------------------------------------------------------

// A minimal ToolDefinition for wire-body / selection assertions.
function tool(name: string, eagerInputStreaming?: boolean): ToolDefinition {
  return {
    name,
    description: `tool ${name}`,
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    ...(eagerInputStreaming !== undefined ? { eagerInputStreaming } : {}),
    execute: async () => ({ content: '', isError: false }),
  } as ToolDefinition;
}

// ===========================================================================
// (1a) withEagerToolStreaming — per-tool name-set gating
// ===========================================================================
describe('U020/w78 — withEagerToolStreaming per-tool name set', () => {
  function run(body: unknown, onlyToolNames?: ReadonlySet<string>): unknown {
    let captured: unknown;
    const base = ((_input: unknown, init?: { body?: unknown }) => {
      captured = init?.body;
      return Promise.resolve(new Response('{}'));
    }) as unknown as typeof globalThis.fetch;
    const wrapped = withEagerToolStreaming(base, onlyToolNames);
    void wrapped('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });
    return captured;
  }

  it('applies eager_input_streaming ONLY to tools named in the set', () => {
    const captured = run(
      { model: 'claude-x', tools: [{ name: 'a' }, { name: 'b' }, { name: 'c' }] },
      new Set(['a', 'c']),
    );
    const parsed = JSON.parse(captured as string);
    assert.deepEqual(parsed.tools, [
      { name: 'a', eager_input_streaming: true },
      { name: 'b' },
      { name: 'c', eager_input_streaming: true },
    ]);
  });

  it('an empty set opts in NO tool (field never added)', () => {
    const captured = run(
      { model: 'claude-x', tools: [{ name: 'a' }, { name: 'b' }] },
      new Set<string>(),
    );
    const parsed = JSON.parse(captured as string);
    assert.deepEqual(parsed.tools, [{ name: 'a' }, { name: 'b' }]);
  });

  it('a tool object with no string name is left untouched even if the set is given', () => {
    const captured = run(
      { model: 'claude-x', tools: [{ foo: 1 }, { name: 'a' }] },
      new Set(['a']),
    );
    const parsed = JSON.parse(captured as string);
    assert.deepEqual(parsed.tools, [{ foo: 1 }, { name: 'a', eager_input_streaming: true }]);
  });

  it('omitting the set keeps the global behavior (every tool opted in)', () => {
    const captured = run({ model: 'claude-x', tools: [{ name: 'a' }, { name: 'b' }] });
    const parsed = JSON.parse(captured as string);
    assert.deepEqual(parsed.tools, [
      { name: 'a', eager_input_streaming: true },
      { name: 'b', eager_input_streaming: true },
    ]);
  });
});

// ===========================================================================
// (1b) E2E — resolveModel wires the per-tool opt-in into the native Anthropic
// fetch, reads the ToolDefinition.eagerInputStreaming flag, and gates to the
// first-party path. Drives the real resolved model's fetch and captures the wire.
// ===========================================================================
describe('U020/w78 — resolveModel per-tool eager streaming wiring', () => {
  let realFetch: typeof globalThis.fetch;
  let captured: { body?: unknown };
  const ENV_KEYS = [
    'SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING',
    'CLAUDE_CODE_ENABLE_FINE_GRAINED_TOOL_STREAMING',
    // Force the NATIVE anthropic path (not anthropic-on-vertex), which is the only
    // provider that accepts eager_input_streaming. The host env may have these set.
    'CLAUDE_CODE_USE_VERTEX',
    'ANTHROPIC_VERTEX_PROJECT_ID',
  ] as const;
  const savedEnv: Record<string, string | undefined> = {};

  before(() => {
    realFetch = globalThis.fetch;
    for (const k of ENV_KEYS) savedEnv[k] = process.env[k];
  });
  after(() => {
    globalThis.fetch = realFetch;
    for (const k of ENV_KEYS) {
      if (savedEnv[k] === undefined) delete process.env[k];
      else process.env[k] = savedEnv[k];
    }
  });
  beforeEach(() => {
    captured = {};
    for (const k of ENV_KEYS) delete process.env[k];
    globalThis.fetch = (async (_input: unknown, init?: { body?: unknown }) => {
      captured.body = init?.body;
      return new Response('{}', { status: 200 });
    }) as typeof globalThis.fetch;
  });

  const cfg: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant-x', model: 'claude-opus-4-8' };
  const wireTools = JSON.stringify({
    model: 'claude-opus-4-8',
    tools: [{ name: 'eager_one' }, { name: 'plain_two' }],
  });

  async function outgoingTools(tools: ToolDefinition[] | undefined): Promise<Array<Record<string, unknown>>> {
    const m = resolveModel(cfg, tools) as unknown as { config?: { fetch?: typeof globalThis.fetch } };
    const f = m.config?.fetch;
    assert.ok(typeof f === 'function', 'resolved native anthropic model must expose config.fetch');
    await f!('https://api.anthropic.com/v1/messages', { method: 'POST', body: wireTools });
    return JSON.parse(captured.body as string).tools;
  }

  it('opts in ONLY the flagged tool when the global knob is off', async () => {
    const tools = await outgoingTools([tool('eager_one', true), tool('plain_two')]);
    assert.deepEqual(tools, [
      { name: 'eager_one', eager_input_streaming: true },
      { name: 'plain_two' },
    ]);
  });

  it('adds NO eager field when no tool opts in and the global knob is off', async () => {
    const tools = await outgoingTools([tool('eager_one'), tool('plain_two')]);
    assert.deepEqual(tools, [{ name: 'eager_one' }, { name: 'plain_two' }]);
  });

  it('the global knob still opts in EVERY tool (regression)', async () => {
    process.env.SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING = '1';
    const tools = await outgoingTools([tool('eager_one'), tool('plain_two')]);
    assert.deepEqual(tools, [
      { name: 'eager_one', eager_input_streaming: true },
      { name: 'plain_two', eager_input_streaming: true },
    ]);
  });
});

// ===========================================================================
// (2a) sleepWithRetryHeartbeat — unit
// ===========================================================================
describe('U020/w78 — sleepWithRetryHeartbeat', () => {
  it('fires NO heartbeat for a short wait (totalMs <= interval) — single sleep', async () => {
    const ticks: number[] = [];
    await sleepWithRetryHeartbeat(10, undefined, (r) => ticks.push(r), 50);
    assert.deepEqual(ticks, []);
  });

  it('fires a decrementing heartbeat per completed slice, never on the final one', async () => {
    const ticks: number[] = [];
    await sleepWithRetryHeartbeat(50, undefined, (r) => ticks.push(r), 10);
    // 5 slices of 10ms -> heartbeats at remaining 40,30,20,10; the last slice (->0)
    // resolves the wait and fires nothing.
    assert.deepEqual(ticks, [40, 30, 20, 10]);
  });

  it('handles a total that is not a multiple of the interval', async () => {
    const ticks: number[] = [];
    await sleepWithRetryHeartbeat(25, undefined, (r) => ticks.push(r), 10);
    // slices 10,10,5 -> heartbeats at 15,5 then the final 5ms slice resolves.
    assert.deepEqual(ticks, [15, 5]);
  });

  it('is a plain sleep when no onHeartbeat is supplied', async () => {
    await assert.doesNotReject(sleepWithRetryHeartbeat(30, undefined, undefined, 10));
  });

  it('falls back to the default interval when intervalMs <= 0', async () => {
    const ticks: number[] = [];
    // total 5ms < default 30_000ms -> single slice, no heartbeat; proves the
    // guard replaced the invalid 0 interval with RETRY_HEARTBEAT_INTERVAL_MS.
    assert.ok(RETRY_HEARTBEAT_INTERVAL_MS > 5);
    await sleepWithRetryHeartbeat(5, undefined, (r) => ticks.push(r), 0);
    assert.deepEqual(ticks, []);
  });

  it('rejects on abort (same contract as the base sleep)', async () => {
    const ac = new AbortController();
    const p = sleepWithRetryHeartbeat(1000, ac.signal, () => {}, 10);
    ac.abort();
    await assert.rejects(p, (e: unknown) => e instanceof DOMException && e.name === 'AbortError');
  });
});

// ===========================================================================
// (2b) Integration — a long retry backoff re-fires onRetry with a decrementing
// countdown (heartbeat). RED without the slicing: onRetry fires exactly once.
// ===========================================================================
describe('U020/w78 — retry-backoff heartbeat through streamChatCompletionWithRetry', () => {
  let server: http.Server;
  let port: number;
  let calls = 0;
  let savedRandom: () => number;

  before(async () => {
    savedRandom = retryHooks.random;
    retryHooks.random = () => 1; // pin jitter so the backoff delay is deterministic
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      calls++;
      if (calls === 1) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'service unavailable' } }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}\n\n' +
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n' +
        'data: [DONE]\n\n',
      );
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => { retryHooks.random = savedRandom; server.close(); });

  it('re-fires onRetry with a decrementing remaining time during a long backoff', async () => {
    calls = 0;
    const config = {
      provider: 'openai', apiKey: 'sk-key', model: 'test-model',
      baseUrl: `http://127.0.0.1:${port}/v1`,
    } as ProviderConfig;
    const ac = new AbortController();
    const delays: number[] = [];
    const statuses: number[] = [];
    let text = '';
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }] as Message[], [], ac.signal,
      {
        maxRetries: 3,
        baseDelayMs: 80,   // attempt-0 backoff = 80ms (jitter pinned to 1)
        maxDelayMs: 80,
        heartbeatIntervalMs: 20, // 80ms sliced into 20ms -> heartbeats at 60,40,20
        onRetry: (_a, _m, delayMs, status) => { delays.push(delayMs); statuses.push(status); },
      } as Parameters<typeof streamChatCompletionWithRetry>[4],
    )) {
      const chunk = c as StreamChunk;
      if (chunk.type === 'content_delta') text += chunk.text;
    }

    assert.equal(text, 'ok', 'stream recovers after the 503');
    // RED (single sleep, pre-fix): exactly one onRetry call.
    // GREEN (chunked heartbeat): the initial call + a heartbeat per slice.
    assert.ok(delays.length > 1, `expected multiple onRetry heartbeats, got ${delays.length}`);
    assert.equal(delays[0], 80, 'first onRetry reports the full backoff');
    // Every heartbeat after the first reports a strictly smaller remaining time.
    for (let i = 1; i < delays.length; i++) {
      assert.ok(delays[i] < delays[i - 1], `countdown must decrease: ${delays.join(',')}`);
    }
    // The 503 retry keeps its status on every heartbeat (not reset to 0).
    assert.ok(statuses.every((s) => s === 503), `heartbeat status must stay 503, got ${statuses.join(',')}`);
  });
});
