// ===========================================================================
// provider-betas.test.ts
//
// Parity unit U020 — Anthropic beta-header assembly (core/core/src/provider.ts).
// Covers the single canonical beta source (computeAnthropicBetas) and its wiring
// into the native + anthropic-on-vertex provider paths:
//   - interleaved-thinking beta (model-gated, env opt-out)      [gap: betas.ts L4-5]
//   - token-efficient-tools beta (explicit opt-in)              [gap: betas.ts L21-22]
//   - CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS kill switch         [gap: utils/betas.ts L215-220]
//
// ESM-coverage-safe: no source-text assertions, the only mocked global is
// globalThis.fetch (saved + restored), env is saved + restored per block.
// ===========================================================================
import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  computeAnthropicBetas,
  supportsInterleavedThinking,
  experimentalBetasDisabled,
  resolveModel,
  INTERLEAVED_THINKING_BETA,
  TOKEN_EFFICIENT_TOOLS_BETA,
  EXTENDED_CACHE_TTL_BETA,
  ONE_MILLION_CONTEXT_BETA,
} from '../src/provider';
import type { ProviderConfig } from '../src/types';

// Every env var that influences beta assembly or provider routing, so each test
// starts from a clean slate regardless of the ambient shell.
const BETA_ENV = [
  'ANTHROPIC_BETAS',
  'CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS',
  'SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS',
  'DISABLE_INTERLEAVED_THINKING',
  'SUPERINFERENCE_DISABLE_INTERLEAVED_THINKING',
  'SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS',
  'CLAUDE_CODE_TOKEN_EFFICIENT_TOOLS',
  'SUPERINFERENCE_DISABLE_1M_CONTEXT',
  'CLAUDE_CODE_USE_VERTEX',
  'ANTHROPIC_VERTEX_PROJECT_ID',
  'GOOGLE_VERTEX_PROJECT',
  'CLOUD_ML_REGION',
  'GOOGLE_VERTEX_LOCATION',
] as const;

function saveEnv(): Record<string, string | undefined> {
  const saved: Record<string, string | undefined> = {};
  for (const k of BETA_ENV) { saved[k] = process.env[k]; delete process.env[k]; }
  return saved;
}
function restoreEnv(saved: Record<string, string | undefined>): void {
  for (const k of BETA_ENV) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
}

// ---------------------------------------------------------------------------
// supportsInterleavedThinking — Claude 4+ Opus/Sonnet families only.
// ---------------------------------------------------------------------------
describe('supportsInterleavedThinking', () => {
  it('matches Claude 4+ Opus/Sonnet families', () => {
    assert.equal(supportsInterleavedThinking('claude-sonnet-4-6'), true);
    assert.equal(supportsInterleavedThinking('claude-opus-4-1'), true);
    assert.equal(supportsInterleavedThinking('claude-sonnet-45-beta'), true);
  });
  it('rejects pre-4 and non-Claude models', () => {
    assert.equal(supportsInterleavedThinking('claude-3-5-sonnet'), false);
    assert.equal(supportsInterleavedThinking('claude-haiku-4-5'), false); // haiku excluded
    assert.equal(supportsInterleavedThinking('gpt-5.6-luna'), false);
  });
  it('is robust to non-string / empty input', () => {
    assert.equal(supportsInterleavedThinking(''), false);
    assert.equal(supportsInterleavedThinking(undefined as unknown as string), false);
  });
});

// ---------------------------------------------------------------------------
// experimentalBetasDisabled — kill switch (Claude + AMI env names).
// ---------------------------------------------------------------------------
describe('experimentalBetasDisabled', () => {
  let saved: Record<string, string | undefined>;
  beforeEach(() => { saved = saveEnv(); });
  afterEach(() => { restoreEnv(saved); });

  it('is false by default', () => {
    assert.equal(experimentalBetasDisabled(), false);
  });
  it('honors the reference-parity env name', () => {
    process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = '1';
    assert.equal(experimentalBetasDisabled(), true);
  });
  it('honors the AMI-native env name', () => {
    process.env.SUPERINFERENCE_DISABLE_EXPERIMENTAL_BETAS = 'true';
    assert.equal(experimentalBetasDisabled(), true);
  });
});

// ---------------------------------------------------------------------------
// computeAnthropicBetas — the single canonical beta source.
// ---------------------------------------------------------------------------
describe('computeAnthropicBetas', () => {
  let saved: Record<string, string | undefined>;
  beforeEach(() => { saved = saveEnv(); });
  afterEach(() => { restoreEnv(saved); });

  it('adds interleaved-thinking for a supported model (native path, no 1h TTL)', () => {
    const betas = computeAnthropicBetas('claude-sonnet-4-6', { includeCacheTtlBeta: true });
    assert.ok(betas.includes(INTERLEAVED_THINKING_BETA));
    assert.ok(betas.includes(ONE_MILLION_CONTEXT_BETA)); // sonnet-4 serves 1M
    assert.ok(!betas.includes(EXTENDED_CACHE_TTL_BETA)); // default TTL, not 1h
  });

  it('adds the extended-cache beta on the native path for 1h TTL only', () => {
    assert.ok(
      computeAnthropicBetas('claude-opus-4-1', { cacheTtl: '1h', includeCacheTtlBeta: true })
        .includes(EXTENDED_CACHE_TTL_BETA),
    );
    assert.ok(
      !computeAnthropicBetas('claude-opus-4-1', { cacheTtl: '5m', includeCacheTtlBeta: true })
        .includes(EXTENDED_CACHE_TTL_BETA),
    );
  });

  it('omits the extended-cache beta on the vertex path even for 1h TTL', () => {
    // withVertexAnthropicCache injects that header separately.
    const betas = computeAnthropicBetas('claude-opus-4-1', { cacheTtl: '1h', includeCacheTtlBeta: false });
    assert.ok(!betas.includes(EXTENDED_CACHE_TTL_BETA));
  });

  it('omits interleaved-thinking for an unsupported model', () => {
    assert.ok(!computeAnthropicBetas('claude-3-5-sonnet').includes(INTERLEAVED_THINKING_BETA));
  });

  it('opt-out via DISABLE_INTERLEAVED_THINKING drops the header', () => {
    process.env.DISABLE_INTERLEAVED_THINKING = '1';
    assert.ok(!computeAnthropicBetas('claude-sonnet-4-6').includes(INTERLEAVED_THINKING_BETA));
  });

  it('token-efficient-tools is opt-in', () => {
    assert.ok(!computeAnthropicBetas('claude-sonnet-4-6').includes(TOKEN_EFFICIENT_TOOLS_BETA));
    process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS = '1';
    assert.ok(computeAnthropicBetas('claude-sonnet-4-6').includes(TOKEN_EFFICIENT_TOOLS_BETA));
  });

  it('the kill switch strips experimental betas but keeps 1M, cache-ttl and ANTHROPIC_BETAS', () => {
    process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS = '1';
    process.env.ANTHROPIC_BETAS = 'my-private-beta';
    process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = '1';
    const betas = computeAnthropicBetas('claude-sonnet-4-6', { cacheTtl: '1h', includeCacheTtlBeta: true });
    assert.ok(!betas.includes(INTERLEAVED_THINKING_BETA));
    assert.ok(!betas.includes(TOKEN_EFFICIENT_TOOLS_BETA));
    assert.ok(betas.includes(ONE_MILLION_CONTEXT_BETA));
    assert.ok(betas.includes(EXTENDED_CACHE_TTL_BETA));
    assert.ok(betas.includes('my-private-beta')); // escape hatch bypasses the kill switch
  });

  it('dedupes when ANTHROPIC_BETAS repeats a managed token', () => {
    process.env.ANTHROPIC_BETAS = `${INTERLEAVED_THINKING_BETA},custom-x`;
    const betas = computeAnthropicBetas('claude-sonnet-4-6');
    assert.equal(betas.filter(b => b === INTERLEAVED_THINKING_BETA).length, 1);
    assert.ok(betas.includes('custom-x'));
  });
});

// ---------------------------------------------------------------------------
// E2E — the header actually rides the wire on the anthropic-on-vertex path,
// assembled by resolveModel through computeAnthropicBetas + withAnthropicBetaHeader.
// We drive the real provider model's fetch and capture the outgoing header.
// ---------------------------------------------------------------------------
describe('resolveModel (anthropic-vertex) emits betas on the wire', () => {
  let saved: Record<string, string | undefined>;
  let realFetch: typeof globalThis.fetch;
  let calls: Array<{ input: unknown; init: any }>; // eslint-disable-line @typescript-eslint/no-explicit-any

  before(() => {
    saved = saveEnv();
    process.env.ANTHROPIC_VERTEX_PROJECT_ID = 'proj';
    process.env.CLOUD_ML_REGION = 'us-east5';
    realFetch = globalThis.fetch;
  });
  after(() => { globalThis.fetch = realFetch; restoreEnv(saved); });
  beforeEach(() => {
    calls = [];
    globalThis.fetch = (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
      calls.push({ input, init });
      return new Response('{}', { status: 200 });
    }) as typeof globalThis.fetch;
  });

  async function outgoingBeta(cfg: ProviderConfig): Promise<string | null> {
    const m = resolveModel(cfg) as unknown as { config: { fetch: typeof globalThis.fetch } };
    await m.config.fetch('https://vertex.example/v1/messages', { method: 'POST', headers: {} });
    const h = calls[0]?.init?.headers;
    return h instanceof Headers ? h.get('anthropic-beta') : (h?.['anthropic-beta'] ?? null);
  }

  it('sends interleaved-thinking + 1M for a supported model', async () => {
    const beta = await outgoingBeta({ provider: 'anthropic-vertex', apiKey: '', model: 'claude-sonnet-4-6' } as ProviderConfig);
    assert.ok(beta?.includes(INTERLEAVED_THINKING_BETA), `expected interleaved-thinking, got: ${beta}`);
    assert.ok(beta?.includes(ONE_MILLION_CONTEXT_BETA));
  });

  it('strips interleaved-thinking under the kill switch but keeps 1M', async () => {
    process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = '1';
    const beta = await outgoingBeta({ provider: 'anthropic-vertex', apiKey: '', model: 'claude-sonnet-4-6' } as ProviderConfig);
    assert.ok(!beta?.includes(INTERLEAVED_THINKING_BETA), `expected no interleaved-thinking, got: ${beta}`);
    assert.ok(beta?.includes(ONE_MILLION_CONTEXT_BETA));
    delete process.env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS;
  });

  it('sends token-efficient-tools when opted in', async () => {
    process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS = '1';
    const beta = await outgoingBeta({ provider: 'anthropic-vertex', apiKey: '', model: 'claude-sonnet-4-6' } as ProviderConfig);
    assert.ok(beta?.includes(TOKEN_EFFICIENT_TOOLS_BETA), `expected token-efficient-tools, got: ${beta}`);
    delete process.env.SUPERINFERENCE_TOKEN_EFFICIENT_TOOLS;
  });
});
