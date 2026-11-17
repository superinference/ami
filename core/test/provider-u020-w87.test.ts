// ===========================================================================
// provider-u020-w87.test.ts
//
// Parity unit U020 (wave 87) — additive provider-layer capabilities in
// core/core/src/provider.ts (+ core/core/src/utils/aws.ts):
//
//  - Gap (configs.ts): cross-provider canonical model-ID equivalence/translation
//    (CANONICAL_MODEL_EQUIVALENTS / CANONICAL_ID_TO_KEY / resolveModelForProvider)
//    wired into resolveModel so a model chosen under one provider is translated to
//    the active provider's required wire id.
//  - Gap (configs.ts): Azure AI Foundry as a Claude-model provider target
//    ('azure-foundry' provider: union + registries + resolveModel route).
//  - Gap (aws.ts): clearAwsIniCache + fromIni({ ignoreCache }) parity — a
//    dependency-free ~/.aws/credentials reader with cache invalidation, wired into
//    resolveBedrockGateway as a credential fallback.
//  - Gap (constants/betas.ts): VERTEX_COUNT_TOKENS_ALLOWED_BETAS proactive
//    allowlist + filterCountTokensBetas endpoint-scoped filter.
//  - Gap (tokenEstimation.ts): accurate provider token-count path
//    (countTokensWithAPI) with thinking-budget / beta knobs.
//  - Gap (apiMicrocompact.ts): provider-native (server-side) context editing
//    (buildAnthropicContextManagement) computing clear_at_least = trigger - keep,
//    wired into the native Anthropic request body + beta header.
//
// ESM-coverage-safe: env snapshotted/restored; temp files via mkdtemp; mock HTTP
// via http.createServer on port 0.
// ===========================================================================
import { describe, it, afterEach, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  resolveModel,
  resolveModelForProvider,
  CANONICAL_MODEL_EQUIVALENTS,
  CANONICAL_ID_TO_KEY,
  describeProviders,
  inferProviderFromEnv,
  resolveBedrockGateway,
  VERTEX_COUNT_TOKENS_ALLOWED_BETAS,
  filterCountTokensBetas,
  countTokensWithAPI,
  buildAnthropicContextManagement,
  withExtraBody,
  CONTEXT_MANAGEMENT_BETA,
  INTERLEAVED_THINKING_BETA,
  CLAUDE_CODE_BETA,
  ONE_MILLION_CONTEXT_BETA,
  TOKEN_COUNT_THINKING_BUDGET,
  DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS,
  DEFAULT_CONTEXT_EDIT_TARGET_INPUT_TOKENS,
} from '../src/provider';
import {
  readAwsIniCredentials,
  clearAwsIniCache,
  resolveAwsCredentialsFilePath,
  AWS_INI_IGNORE_CACHE_ENV,
} from '../src/utils/aws';

const ENV_KEYS = [
  'AWS_SHARED_CREDENTIALS_FILE', 'AWS_PROFILE', 'AWS_BEARER_TOKEN_BEDROCK',
  'AWS_ACCESS_KEY_ID', 'AWS_SESSION_TOKEN', AWS_INI_IGNORE_CACHE_ENV,
  'AZURE_AI_FOUNDRY_API_KEY', 'AZURE_AI_FOUNDRY_BASE_URL',
  'CLAUDE_CODE_USE_VERTEX', 'ANTHROPIC_VERTEX_PROJECT_ID',
  'AMI_API_CONTEXT_EDIT', 'AMI_API_CONTEXT_EDIT_MAX_INPUT_TOKENS',
  'AMI_API_CONTEXT_EDIT_TARGET_INPUT_TOKENS', 'ANTHROPIC_BETAS',
];

function snapshotEnv(): Record<string, string | undefined> {
  const snap: Record<string, string | undefined> = {};
  for (const k of ENV_KEYS) snap[k] = process.env[k];
  return snap;
}
function restoreEnv(snap: Record<string, string | undefined>): void {
  for (const k of ENV_KEYS) {
    if (snap[k] === undefined) delete process.env[k];
    else process.env[k] = snap[k];
  }
}

// ---------------------------------------------------------------------------
// Cross-provider canonical model-ID equivalence / translation
// ---------------------------------------------------------------------------
describe('canonical model equivalence / translation', () => {
  it('CANONICAL_ID_TO_KEY is comprehensive: every entry resolves every provider field', () => {
    for (const [key, config] of Object.entries(CANONICAL_MODEL_EQUIVALENTS)) {
      for (const providerField of ['anthropic', 'anthropic-vertex', 'amazon-bedrock', 'azure-foundry'] as const) {
        const wireId = config[providerField];
        assert.ok(typeof wireId === 'string' && wireId.length > 0,
          `${key}.${providerField} must be a non-empty wire id`);
        // Reverse index maps each wire id back to its canonical key.
        assert.equal(CANONICAL_ID_TO_KEY[wireId], key,
          `CANONICAL_ID_TO_KEY['${wireId}'] should map to '${key}'`);
      }
      // The canonical key itself maps to itself.
      assert.equal(CANONICAL_ID_TO_KEY[key], key);
    }
  });

  it('translates a model across providers', () => {
    assert.equal(resolveModelForProvider('claude-opus-4-6', 'amazon-bedrock'), 'anthropic.claude-opus-4-6-20250514');
    assert.equal(resolveModelForProvider('claude-opus-4-6', 'anthropic'), 'claude-opus-4-6');
    assert.equal(resolveModelForProvider('claude-opus-4-6', 'azure-foundry'), 'claude-opus-4-6');
    // bedrock id → anthropic id (reverse direction)
    assert.equal(resolveModelForProvider('anthropic.claude-sonnet-4-6-20250514', 'anthropic'), 'claude-sonnet-4-6');
  });

  it('is idempotent and passes unknown / non-mapped through unchanged', () => {
    // already in the target provider's naming
    assert.equal(resolveModelForProvider('anthropic.claude-opus-4-6-20250514', 'amazon-bedrock'), 'anthropic.claude-opus-4-6-20250514');
    // unknown model id → unchanged
    assert.equal(resolveModelForProvider('gpt-4o', 'amazon-bedrock'), 'gpt-4o');
    // provider the model has no mapping for → unchanged (never translate to a missing field)
    assert.equal(resolveModelForProvider('claude-opus-4-6', 'openai'), 'claude-opus-4-6');
    // undefined provider → unchanged
    assert.equal(resolveModelForProvider('claude-opus-4-6', undefined), 'claude-opus-4-6');
  });

  it('resolveModel translates a bedrock-style id to the active anthropic id (end-to-end)', () => {
    const snap = snapshotEnv();
    delete process.env.CLAUDE_CODE_USE_VERTEX;
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    try {
      const m = resolveModel({ provider: 'anthropic', apiKey: 'sk-ant-x', model: 'anthropic.claude-opus-4-6-20250514' } as never);
      assert.ok(m.provider.startsWith('anthropic'));
      assert.equal(m.modelId, 'claude-opus-4-6');
    } finally {
      restoreEnv(snap);
    }
  });
});

// ---------------------------------------------------------------------------
// Azure AI Foundry provider
// ---------------------------------------------------------------------------
describe('Azure AI Foundry provider', () => {
  it('is registered as a known provider with its models', () => {
    const foundry = describeProviders().find((p) => p.id === 'azure-foundry');
    assert.ok(foundry, 'azure-foundry should be in describeProviders()');
    assert.ok(foundry!.models.includes('claude-opus-4-6'));
    assert.ok(foundry!.envVars.includes('AZURE_AI_FOUNDRY_API_KEY'));
  });

  it('is inferable from AZURE_AI_FOUNDRY_API_KEY', () => {
    const snap = snapshotEnv();
    for (const k of ENV_KEYS) delete process.env[k];
    process.env.AZURE_AI_FOUNDRY_API_KEY = 'foundry-key';
    try {
      const inferred = inferProviderFromEnv();
      assert.equal(inferred?.provider, 'azure-foundry');
    } finally {
      restoreEnv(snap);
    }
  });

  it('routes a Foundry-hosted claude model through the OpenAI-compatible endpoint (not api.anthropic.com)', () => {
    const snap = snapshotEnv();
    delete process.env.CLAUDE_CODE_USE_VERTEX;
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    try {
      const m = resolveModel({
        provider: 'azure-foundry',
        apiKey: 'foundry-key',
        baseUrl: 'https://myres.services.ai.azure.com/models',
        model: 'claude-opus-4-6',
      } as never);
      assert.ok(m.provider.startsWith('openai'), `expected openai-compatible provider, got ${m.provider}`);
      assert.equal(m.modelId, 'claude-opus-4-6');
    } finally {
      restoreEnv(snap);
    }
  });
});

// ---------------------------------------------------------------------------
// AWS INI credentials (clearAwsIniCache / ignoreCache)
// ---------------------------------------------------------------------------
describe('AWS shared-credentials INI reader', () => {
  let tmpDir: string;
  let credFile: string;
  const INI_A = '[default]\naws_access_key_id = AKIADEFAULT\naws_secret_access_key = secretA\n\n[dev]\naws_access_key_id = AKIADEV\n';

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'aws-ini-'));
    credFile = path.join(tmpDir, 'credentials');
  });
  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
  afterEach(() => {
    clearAwsIniCache();
  });

  it('resolveAwsCredentialsFilePath honors AWS_SHARED_CREDENTIALS_FILE then ~/.aws/credentials', () => {
    assert.equal(resolveAwsCredentialsFilePath({ AWS_SHARED_CREDENTIALS_FILE: '/x/creds' } as never), '/x/creds');
    assert.equal(resolveAwsCredentialsFilePath({} as never), path.join(os.homedir(), '.aws', 'credentials'));
  });

  it('reads the default and a named profile', () => {
    fs.writeFileSync(credFile, INI_A);
    const env = { AWS_SHARED_CREDENTIALS_FILE: credFile } as never;
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIADEFAULT');
    assert.equal(readAwsIniCredentials({ env }).secretAccessKey, 'secretA');
    assert.equal(readAwsIniCredentials({ profile: 'dev', env }).accessKeyId, 'AKIADEV');
  });

  it('honors AWS_PROFILE and [profile x] section headers + comments', () => {
    fs.writeFileSync(credFile, '# a comment\n[profile prod]\naws_access_key_id = AKIAPROD ; inline comment\n');
    const env = { AWS_SHARED_CREDENTIALS_FILE: credFile, AWS_PROFILE: 'prod' } as never;
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIAPROD');
  });

  it('returns {} for a missing file or unknown profile (no throw)', () => {
    assert.deepEqual(readAwsIniCredentials({ env: { AWS_SHARED_CREDENTIALS_FILE: path.join(tmpDir, 'nope') } as never }), {});
    fs.writeFileSync(credFile, INI_A);
    assert.deepEqual(readAwsIniCredentials({ profile: 'ghost', env: { AWS_SHARED_CREDENTIALS_FILE: credFile } as never }), {});
  });

  it('caches the parse and clearAwsIniCache / ignoreCache force a fresh read', () => {
    fs.writeFileSync(credFile, INI_A);
    const env = { AWS_SHARED_CREDENTIALS_FILE: credFile } as never;
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIADEFAULT');
    // Edit the file; the cached value must persist until invalidated.
    fs.writeFileSync(credFile, '[default]\naws_access_key_id = AKIAROTATED\n');
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIADEFAULT');
    // ignoreCache picks up the rotation.
    assert.equal(readAwsIniCredentials({ env, ignoreCache: true }).accessKeyId, 'AKIAROTATED');
    // clearAwsIniCache also invalidates.
    fs.writeFileSync(credFile, '[default]\naws_access_key_id = AKIAAGAIN\n');
    clearAwsIniCache();
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIAAGAIN');
  });

  it('AMI_AWS_INI_IGNORE_CACHE env knob forces a fresh read every call', () => {
    fs.writeFileSync(credFile, INI_A);
    const env = { AWS_SHARED_CREDENTIALS_FILE: credFile, [AWS_INI_IGNORE_CACHE_ENV]: '1' } as never;
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIADEFAULT');
    fs.writeFileSync(credFile, '[default]\naws_access_key_id = AKIAFRESH\n');
    assert.equal(readAwsIniCredentials({ env }).accessKeyId, 'AKIAFRESH');
  });

  it('resolveBedrockGateway falls back to the ini access key, but bearer and config.apiKey win', () => {
    fs.writeFileSync(credFile, '[default]\naws_access_key_id = AKIAINIFALLBACK\n');
    const base = { AWS_SHARED_CREDENTIALS_FILE: credFile };
    clearAwsIniCache();
    // No bearer, no config key → ini fallback.
    assert.equal(resolveBedrockGateway({ apiKey: '', baseUrl: '' }, { ...base } as never).apiKey, 'AKIAINIFALLBACK');
    // Explicit config.apiKey wins.
    assert.equal(resolveBedrockGateway({ apiKey: 'cfg-key', baseUrl: '' }, { ...base } as never).apiKey, 'cfg-key');
    // Bearer token wins over everything.
    assert.equal(
      resolveBedrockGateway({ apiKey: 'cfg-key', baseUrl: '' }, { ...base, AWS_BEARER_TOKEN_BEDROCK: 'bearer-tok' } as never).apiKey,
      'bearer-tok',
    );
  });
});

// ---------------------------------------------------------------------------
// Vertex countTokens beta allowlist
// ---------------------------------------------------------------------------
describe('Vertex countTokens beta allowlist', () => {
  it('allowlist contains exactly the Vertex-safe betas', () => {
    assert.ok(VERTEX_COUNT_TOKENS_ALLOWED_BETAS.has(CLAUDE_CODE_BETA));
    assert.ok(VERTEX_COUNT_TOKENS_ALLOWED_BETAS.has(INTERLEAVED_THINKING_BETA));
    assert.ok(VERTEX_COUNT_TOKENS_ALLOWED_BETAS.has(CONTEXT_MANAGEMENT_BETA));
    assert.ok(!VERTEX_COUNT_TOKENS_ALLOWED_BETAS.has(ONE_MILLION_CONTEXT_BETA));
  });

  it('strips disallowed betas on vertex, passes all through elsewhere', () => {
    const betas = [INTERLEAVED_THINKING_BETA, ONE_MILLION_CONTEXT_BETA, 'extended-cache-ttl-2025-04-11'];
    assert.deepEqual(filterCountTokensBetas(betas, 'anthropic-vertex'), [INTERLEAVED_THINKING_BETA]);
    assert.deepEqual(filterCountTokensBetas(betas, 'google-vertex'), [INTERLEAVED_THINKING_BETA]);
    assert.deepEqual(filterCountTokensBetas(betas, 'anthropic'), betas);
    assert.deepEqual(filterCountTokensBetas(betas, undefined), betas);
  });
});

// ---------------------------------------------------------------------------
// Accurate provider token-count path (countTokensWithAPI)
// ---------------------------------------------------------------------------
describe('countTokensWithAPI', () => {
  let server: http.Server;
  let baseUrl: string;
  let lastReq: { headers: http.IncomingHttpHeaders; body: unknown; url: string } | null = null;
  let respond: () => { status: number; json: unknown } = () => ({ status: 200, json: { input_tokens: 42 } });

  before(async () => {
    server = http.createServer((req, res) => {
      let data = '';
      req.on('data', (c) => (data += c));
      req.on('end', () => {
        lastReq = { headers: req.headers, body: data ? JSON.parse(data) : null, url: req.url || '' };
        const r = respond();
        res.writeHead(r.status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(r.json));
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });
  after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  afterEach(() => {
    respond = () => ({ status: 200, json: { input_tokens: 42 } });
    lastReq = null;
  });

  it('returns input_tokens and POSTs to /v1/messages/count_tokens', async () => {
    const n = await countTokensWithAPI(
      { apiKey: 'sk-ant-x', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic' } as never,
      { messages: [{ role: 'user', content: 'hi' }] },
    );
    assert.equal(n, 42);
    assert.equal(lastReq!.url, '/v1/messages/count_tokens');
    assert.equal(lastReq!.headers['x-api-key'], 'sk-ant-x');
    assert.deepEqual((lastReq!.body as { messages: unknown }).messages, [{ role: 'user', content: 'hi' }]);
  });

  it('injects a dummy user message when messages is empty (for tool token counts)', async () => {
    await countTokensWithAPI(
      { apiKey: 'k', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic' } as never,
      { messages: [], tools: [{ name: 't', input_schema: {} }] },
    );
    assert.deepEqual((lastReq!.body as { messages: unknown }).messages, [{ role: 'user', content: 'foo' }]);
    assert.ok((lastReq!.body as { tools?: unknown[] }).tools);
  });

  it('enables thinking budget when requested', async () => {
    await countTokensWithAPI(
      { apiKey: 'k', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic' } as never,
      { messages: [{ role: 'user', content: 'x' }], thinking: true },
    );
    assert.deepEqual((lastReq!.body as { thinking?: unknown }).thinking, { type: 'enabled', budget_tokens: TOKEN_COUNT_THINKING_BUDGET });
  });

  it('narrows the beta header on the vertex path', async () => {
    const snap = snapshotEnv();
    process.env.ANTHROPIC_BETAS = ONE_MILLION_CONTEXT_BETA; // disallowed on vertex countTokens
    try {
      await countTokensWithAPI(
        { apiKey: 'k', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic-vertex' } as never,
        { messages: [{ role: 'user', content: 'x' }] },
      );
      const hdr = (lastReq!.headers['anthropic-beta'] as string | undefined) ?? '';
      assert.ok(!hdr.split(',').includes(ONE_MILLION_CONTEXT_BETA), `vertex beta header must not carry ${ONE_MILLION_CONTEXT_BETA}: got "${hdr}"`);
    } finally {
      restoreEnv(snap);
    }
  });

  it('returns null on a non-OK response, a missing field, an empty model, or a transport error', async () => {
    respond = () => ({ status: 500, json: { error: 'x' } });
    assert.equal(await countTokensWithAPI({ apiKey: 'k', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic' } as never, { messages: [] }), null);
    respond = () => ({ status: 200, json: { not_input_tokens: 1 } });
    assert.equal(await countTokensWithAPI({ apiKey: 'k', baseUrl, model: 'claude-opus-4-6', provider: 'anthropic' } as never, { messages: [] }), null);
    // empty model
    assert.equal(await countTokensWithAPI({ apiKey: 'k', baseUrl, model: '', provider: 'anthropic' } as never, { messages: [] }), null);
    // transport error (unroutable base)
    assert.equal(await countTokensWithAPI({ apiKey: 'k', baseUrl: 'http://127.0.0.1:1', model: 'claude-opus-4-6', provider: 'anthropic' } as never, { messages: [] }), null);
  });
});

// ---------------------------------------------------------------------------
// Provider-native (server-side) context editing
// ---------------------------------------------------------------------------
describe('buildAnthropicContextManagement', () => {
  it('is OFF by default (returns undefined)', () => {
    assert.equal(buildAnthropicContextManagement({} as never), undefined);
  });

  it('builds a clear_tool_uses edit with clear_at_least = trigger - keepTarget', () => {
    const cm = buildAnthropicContextManagement({ AMI_API_CONTEXT_EDIT: '1' } as never);
    assert.ok(cm);
    const edit = cm!.edits[0] as Record<string, Record<string, number> & { type: string; clear_tool_inputs: boolean }>;
    assert.equal(edit.type, 'clear_tool_uses_20250919');
    assert.equal((edit.trigger as { value: number }).value, DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS);
    assert.equal((edit.clear_at_least as { value: number }).value, DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS - DEFAULT_CONTEXT_EDIT_TARGET_INPUT_TOKENS);
    assert.equal(edit.clear_tool_inputs, true);
  });

  it('honors trigger / keepTarget overrides and clamps clear_at_least at 0', () => {
    const cm = buildAnthropicContextManagement({
      AMI_API_CONTEXT_EDIT: '1',
      AMI_API_CONTEXT_EDIT_MAX_INPUT_TOKENS: '100000',
      AMI_API_CONTEXT_EDIT_TARGET_INPUT_TOKENS: '30000',
    } as never);
    assert.equal(((cm!.edits[0] as Record<string, { value: number }>).clear_at_least).value, 70000);
    // keepTarget larger than trigger → clamped to 0, never negative.
    const cm2 = buildAnthropicContextManagement({
      AMI_API_CONTEXT_EDIT: '1',
      AMI_API_CONTEXT_EDIT_MAX_INPUT_TOKENS: '1000',
      AMI_API_CONTEXT_EDIT_TARGET_INPUT_TOKENS: '9999',
    } as never);
    assert.equal(((cm2!.edits[0] as Record<string, { value: number }>).clear_at_least).value, 0);
  });

  it('is injected into the request body by withExtraBody (the resolveModel composition)', () => {
    const cm = buildAnthropicContextManagement({ AMI_API_CONTEXT_EDIT: '1' } as never)!;
    let sentBody: unknown = null;
    const stub = (async (_input: unknown, init?: { body?: string }) => {
      sentBody = init?.body ? JSON.parse(init.body) : null;
      return new Response('{}');
    }) as unknown as typeof globalThis.fetch;
    const wrapped = withExtraBody({ context_management: cm }, stub)!;
    void wrapped('https://api.anthropic.com/v1/messages', { body: JSON.stringify({ model: 'claude-opus-4-6', messages: [] }) } as never);
    assert.deepEqual((sentBody as { context_management: unknown }).context_management, cm);
  });

  it('resolveModel wires the context edit + beta when AMI_API_CONTEXT_EDIT is set (smoke)', () => {
    const snap = snapshotEnv();
    delete process.env.CLAUDE_CODE_USE_VERTEX;
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    process.env.AMI_API_CONTEXT_EDIT = '1';
    try {
      const m = resolveModel({ provider: 'anthropic', apiKey: 'sk-ant-x', model: 'claude-opus-4-6' } as never);
      assert.ok(m.provider.startsWith('anthropic'));
      assert.equal(m.modelId, 'claude-opus-4-6');
    } finally {
      restoreEnv(snap);
    }
  });
});
