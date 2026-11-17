/**
 * U065 (w96) — model-registry parity additions:
 *  - Gap 5: cancellation/abort of in-flight model-list / connectivity network
 *    waits. httpGetJSON accepts an optional AbortSignal threaded through
 *    listModels / listModelsDiagnostic / validateModel / probeProviderConnectivity
 *    so a cancelled `/model` listing tears the in-flight request down immediately
 *    instead of running to the per-request timeout. A signal-bearing listing runs
 *    on its OWN request (never coalesced) so aborting it never tears down a
 *    listing shared with other concurrent callers.
 *    Ref services/api/withRetry.ts:284-295.
 *  - Gap 1: per-provider canonical model-id table — getBuiltinModelStrings(config)
 *    maps each logical short key to its canonical first-party id AND the concrete
 *    wire id for the detected provider (Bedrock region-scoped profile, else the
 *    canonical id), via the single central remap (toProviderModelId /
 *    resolveBedrockModelId). Ref utils/model/modelStrings.ts:L21-L31.
 *  - Gap 4: validateModel suggestion ranking — closest (shortest-delta) match
 *    first within a match tier instead of the provider's arbitrary list order.
 *    Ref utils/model/validateModel.ts:26-31.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-abort-wireids.test.ts
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  listModelsDiagnostic,
  validateModel,
  probeProviderConnectivity,
  getBuiltinModelStrings,
  rankClosestMatches,
  MODEL_SHORT_KEYS,
} from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

// Env keys that interfere with provider detection / Bedrock resolution.
const INTERFERING_ENV_VARS = [
  'ANTHROPIC_VERTEX_PROJECT_ID', 'CLAUDE_CODE_USE_VERTEX', 'CLAUDE_CODE_USE_BEDROCK',
  'SUPERINFERENCE_USE_BEDROCK', 'CLAUDE_CODE_USE_FOUNDRY', 'SUPERINFERENCE_USE_FOUNDRY',
  'AMI_BEDROCK_INFERENCE_PROFILES', 'AMI_BEDROCK_REGION', 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
  'HF_TOKEN', 'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY',
  'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY',
  'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY', 'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY',
  'LUMA_API_KEY', 'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
];

async function withCleanEnvAsync<T>(fn: () => Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {};
  for (const k of INTERFERING_ENV_VARS) { saved[k] = process.env[k]; delete process.env[k]; }
  try { return await fn(); }
  finally {
    for (const k of INTERFERING_ENV_VARS) {
      if (saved[k] !== undefined) process.env[k] = saved[k]; else delete process.env[k];
    }
  }
}

function cfg(overrides: Partial<ProviderConfig> = {}): ProviderConfig {
  return { baseUrl: '', apiKey: '', model: '', ...overrides };
}

// ---------------------------------------------------------------------------
// Gap 5 — AbortSignal cancellation of in-flight model-list network waits.
// ---------------------------------------------------------------------------
describe('U065 — AbortSignal cancellation (Gap 5)', () => {
  let server: http.Server;
  let port = 0;
  let requestCount = 0;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      requestCount++;
      if (req.url === '/fast/models') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }] }));
        return;
      }
      // /models (default): HOLD the response open so only an abort/timeout settles it.
      // (never calls res.end)
    });
    await new Promise<void>(r => { s.listen(0, '127.0.0.1', () => r()); });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('tears down a held request the instant the signal fires (well under the timeout)', async () => {
    await withCleanEnvAsync(async () => {
      process.env.SI_MODEL_LIST_TIMEOUT_MS = '600000'; // 10min — only abort can win
      try {
        const ac = new AbortController();
        const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k' });
        const started = Date.now();
        setTimeout(() => ac.abort(), 30);
        const res = await listModelsDiagnostic(config, ac.signal);
        const elapsed = Date.now() - started;
        // The held request cannot answer; abort makes the fetch reject fast, so
        // listModelsDiagnostic falls back to its (empty for openai) list + diagnostic.
        assert.ok(elapsed < 2000, `aborted listing must settle fast, took ${elapsed}ms`);
        assert.equal(res.models.length, 0);
        assert.ok(res.diagnostic, 'an aborted listing surfaces a diagnostic');
      } finally {
        delete process.env.SI_MODEL_LIST_TIMEOUT_MS;
      }
    });
  });

  it('rejects synchronously when the signal is already aborted (no socket opened)', async () => {
    await withCleanEnvAsync(async () => {
      const before = requestCount;
      const ac = new AbortController();
      ac.abort();
      const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k' });
      const res = await listModelsDiagnostic(config, ac.signal);
      assert.equal(requestCount, before, 'an already-aborted signal must not open a socket');
      assert.ok(res.diagnostic);
    });
  });

  it('validateModel honors an already-aborted signal (fails open with a reason)', async () => {
    await withCleanEnvAsync(async () => {
      const ac = new AbortController();
      ac.abort();
      const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k' });
      const res = await validateModel(config, 'gpt-4o', ac.signal);
      // No models retrieved (aborted) → soft/transient failure fails open.
      assert.equal(res.valid, true);
    });
  });

  it('probeProviderConnectivity reports unreachable fast when aborted', async () => {
    await withCleanEnvAsync(async () => {
      process.env.SI_MODEL_LIST_TIMEOUT_MS = '600000';
      try {
        const ac = new AbortController();
        const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k' });
        const started = Date.now();
        setTimeout(() => ac.abort(), 30);
        const r = await probeProviderConnectivity(config, ac.signal);
        const elapsed = Date.now() - started;
        assert.ok(elapsed < 2000, `aborted probe must settle fast, took ${elapsed}ms`);
        assert.equal(r.reachable, false);
        assert.equal(r.status, 'error');
      } finally {
        delete process.env.SI_MODEL_LIST_TIMEOUT_MS;
      }
    });
  });

  it('a signal-bearing listing is NOT coalesced (own request per caller)', async () => {
    await withCleanEnvAsync(async () => {
      const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}/fast`, apiKey: 'k' });
      const before = requestCount;
      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const [a, b] = await Promise.all([
        listModelsDiagnostic(config, ac1.signal),
        listModelsDiagnostic(config, ac2.signal),
      ]);
      // Two cancellable callers must each get their OWN request so one abort cannot
      // tear down the other's socket — two requests, never one coalesced walk.
      assert.equal(requestCount - before, 2, 'cancellable listings must not coalesce');
      assert.deepEqual(a.models.map(m => m.id), ['gpt-4o']);
      assert.deepEqual(b.models.map(m => m.id), ['gpt-4o']);
    });
  });

  it('uncancellable concurrent listings still coalesce into one request', async () => {
    await withCleanEnvAsync(async () => {
      const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}/fast`, apiKey: 'coalesce-key' });
      const before = requestCount;
      const [a, b] = await Promise.all([
        listModelsDiagnostic(config),
        listModelsDiagnostic(config),
      ]);
      assert.equal(requestCount - before, 1, 'uncancellable concurrent listings must coalesce');
      assert.deepEqual(a.models.map(m => m.id), b.models.map(m => m.id));
    });
  });
});

// ---------------------------------------------------------------------------
// Gap 1 — per-provider canonical model-id table (getBuiltinModelStrings).
// ---------------------------------------------------------------------------
describe('U065 — getBuiltinModelStrings (Gap 1)', () => {
  it('maps every short key to its canonical id, wireId==canonical on a first-party provider', () => {
    withCleanEnvAsyncSync(() => {
      const rows = getBuiltinModelStrings(cfg({ provider: 'anthropic' }));
      const keys = rows.map(r => r.key);
      assert.deepEqual(keys, Object.keys(MODEL_SHORT_KEYS));
      for (const row of rows) {
        assert.equal(row.firstParty, MODEL_SHORT_KEYS[row.key]);
        // First-party endpoints send the canonical id unchanged (AMI single-remap).
        assert.equal(row.wireId, row.firstParty);
      }
    });
  });

  it('resolves wire ids to the matching Bedrock inference profile when routing to Bedrock', () => {
    const saved: Record<string, string | undefined> = {};
    const set = (k: string, v: string) => { saved[k] = process.env[k]; process.env[k] = v; };
    set('CLAUDE_CODE_USE_BEDROCK', '1');
    set('AMI_BEDROCK_INFERENCE_PROFILES', 'us.anthropic.claude-opus-4-6-v1:0,eu.anthropic.claude-sonnet-4-5-v1:0');
    try {
      const rows = getBuiltinModelStrings(cfg());
      const opus46 = rows.find(r => r.key === 'opus46');
      assert.ok(opus46);
      assert.equal(opus46!.firstParty, 'claude-opus-4-6');
      // The canonical id resolves to the account's region-scoped Bedrock profile.
      assert.equal(opus46!.wireId, 'us.anthropic.claude-opus-4-6-v1:0');
      const sonnet45 = rows.find(r => r.key === 'sonnet45');
      assert.equal(sonnet45!.wireId, 'eu.anthropic.claude-sonnet-4-5-v1:0');
      // A key with no matching profile falls back to the canonical id unchanged.
      const haiku35 = rows.find(r => r.key === 'haiku35');
      assert.equal(haiku35!.wireId, 'claude-3-5-haiku');
    } finally {
      for (const [k, v] of Object.entries(saved)) {
        if (v !== undefined) process.env[k] = v; else delete process.env[k];
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Gap 4 — closest-first suggestion ranking (rankClosestMatches + validateModel).
// ---------------------------------------------------------------------------
describe('U065 — rankClosestMatches + closest-first suggestion (Gap 4)', () => {
  it('orders candidates by smallest length delta, then shortest, then lexicographic', () => {
    const ranked = rankClosestMatches(['gpt-4o-mini', 'gpt-4o', 'gpt-4o-2024'], 'gpt-4');
    assert.equal(ranked[0], 'gpt-4o'); // closest in length to 'gpt-4'
    // Does not mutate the input.
    const input = ['bbbb', 'aa', 'cc'];
    const out = rankClosestMatches(input, 'xx');
    assert.deepEqual(input, ['bbbb', 'aa', 'cc']);
    // Equal length delta → lexicographic tie-break ('aa' before 'cc').
    assert.deepEqual(out.slice(0, 2), ['aa', 'cc']);
  });

  let server: http.Server;
  let port = 0;
  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      // Decoy longer match listed FIRST so arbitrary list order would pick it.
      res.end(JSON.stringify({ data: [
        { id: 'gpt-4o-realtime-preview' },
        { id: 'gpt-4o' },
      ] }));
    });
    await new Promise<void>(r => { s.listen(0, '127.0.0.1', () => r()); });
    server = s;
    port = (s.address() as { port: number }).port;
  });
  after(() => { server.close(); });

  it('suggests the CLOSEST (shortest-delta) match, not the provider list order', async () => {
    await withCleanEnvAsync(async () => {
      const res = await validateModel(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k' }), 'gpt-4');
      assert.equal(res.valid, false);
      // Without closest-first ranking this would be 'gpt-4o-realtime-preview' (first listed).
      assert.equal(res.suggestion, 'gpt-4o');
    });
  });
});

// Synchronous clean-env wrapper for the non-async Gap 1 case.
function withCleanEnvAsyncSync(fn: () => void): void {
  const saved: Record<string, string | undefined> = {};
  for (const k of INTERFERING_ENV_VARS) { saved[k] = process.env[k]; delete process.env[k]; }
  try { fn(); }
  finally {
    for (const k of INTERFERING_ENV_VARS) {
      if (saved[k] !== undefined) process.env[k] = saved[k]; else delete process.env[k];
    }
  }
}
