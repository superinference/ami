import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  buildThinkingOptions,
  resolveContextWindow,
  inferProviderFromApiKey,
  describeProviders,
  mergeBetaTokens,
  ONE_MILLION_CONTEXT_BETA,
  MODEL_PREFERENCE,
  PROVIDER_BASE_URLS,
} from '../src/provider';
import type { ThinkingConfig, ProviderConfig } from '../src/types';
import type { ThinkingLevel } from '../src/model-capabilities';

// ---------------------------------------------------------------------------
// POV: PURE provider helpers NOT owned by round1's Claude thinking matrix.
// Everything here is computed by calling the real code (no .includes on source).
// ---------------------------------------------------------------------------

const think = (level: ThinkingLevel, extra: Partial<ThinkingConfig> = {}): ThinkingConfig => ({
  enabled: true,
  level,
  ...extra,
});

const cfg = (over: Partial<ProviderConfig>): ProviderConfig => ({
  baseUrl: '',
  apiKey: '',
  model: '',
  ...over,
});

describe('buildThinkingOptions — Gemini (thinkingConfig/thinkingBudget) branch', () => {
  it('gemini-2.5-pro high uses the model max budget (32768) + includeThoughts', () => {
    const opts = buildThinkingOptions('gemini-2.5-pro', think('high'));
    assert.deepEqual(opts, {
      providerOptions: {
        google: { thinkingConfig: { thinkingBudget: 32768, includeThoughts: true } },
      },
    });
  });

  it('gemini-2.5-flash low maps to the low budget (4096)', () => {
    const opts = buildThinkingOptions('gemini-2.5-flash', think('low')) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 4096);
    assert.equal(opts.providerOptions.google.thinkingConfig.includeThoughts, true);
  });

  it('gemini-2.5-pro medium maps to the medium budget (10240)', () => {
    const opts = buildThinkingOptions('gemini-2.5-pro', think('medium')) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 10240);
  });

  it('gemini-2.5-pro max derives from caps.maxThinkingBudget (32768 - 8192 = 24576)', () => {
    const opts = buildThinkingOptions('gemini-2.5-pro', think('max')) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 24576);
  });

  it('explicit budgetTokens overrides the level-derived budget', () => {
    const opts = buildThinkingOptions('gemini-2.5-pro', think('high', { budgetTokens: 777 })) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 777);
  });

  it('gemini-3 family (no capability entry) still routes to google with the flat budget', () => {
    const opts = buildThinkingOptions('gemini-3-pro-preview', think('high')) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 32768);
    assert.equal(opts.providerOptions.google.thinkingConfig.includeThoughts, true);
  });

  it('gemini-3 max with no caps falls back to the 128000 default ceiling (128000 - 8192 = 119808)', () => {
    const opts = buildThinkingOptions('gemini-3-pro-preview', think('max')) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.thinkingBudget, 119808);
  });
});

describe('buildThinkingOptions — OpenAI o-series / GPT-5+ (reasoningEffort) branch', () => {
  it('o3-mini high -> reasoningEffort high', () => {
    const opts = buildThinkingOptions('o3-mini', think('high'));
    assert.deepEqual(opts, { providerOptions: { openai: { reasoningEffort: 'high' } } });
  });

  it('max maps to high (effort scale has no "max")', () => {
    const opts = buildThinkingOptions('o3-mini', think('max')) as any;
    assert.equal(opts.providerOptions.openai.reasoningEffort, 'high');
  });

  it('low and medium map 1:1', () => {
    assert.equal(
      (buildThinkingOptions('o1-mini', think('low')) as any).providerOptions.openai.reasoningEffort,
      'low',
    );
    assert.equal(
      (buildThinkingOptions('o1', think('medium')) as any).providerOptions.openai.reasoningEffort,
      'medium',
    );
  });

  it('gpt-6 (GPT-5+ reasoning family) also uses reasoningEffort', () => {
    const opts = buildThinkingOptions('gpt-6-astra', think('medium')) as any;
    assert.equal(opts.providerOptions.openai.reasoningEffort, 'medium');
  });

  it('o-series is chosen before any gemini check (model id cannot mislead it)', () => {
    // o4-mini is temperatureMustBeUnset -> openai branch, never google.
    const opts = buildThinkingOptions('o4-mini', think('high')) as any;
    assert.ok(opts.providerOptions.openai);
    assert.equal(opts.providerOptions.google, undefined);
  });
});

describe('buildThinkingOptions — DeepSeek / other (empty) branch', () => {
  it('deepseek-reasoner yields no provider options (inline tags)', () => {
    assert.deepEqual(buildThinkingOptions('deepseek-reasoner', think('high')), {});
  });

  it('deepseek-chat (non-reasoning) yields no provider options', () => {
    assert.deepEqual(buildThinkingOptions('deepseek-chat', think('high')), {});
  });

  it('a plain non-reasoning chat model yields no provider options even with thinking on', () => {
    assert.deepEqual(buildThinkingOptions('gpt-4o', think('high')), {});
  });

  it('an entirely unknown model id yields no provider options', () => {
    assert.deepEqual(buildThinkingOptions('some-random-model', think('max')), {});
  });
});

describe('buildThinkingOptions — disabled / absent thinking short-circuits to {}', () => {
  it('undefined thinking -> {}', () => {
    assert.deepEqual(buildThinkingOptions('gemini-2.5-pro', undefined), {});
  });

  it('enabled:false -> {} even on a thinking-capable model', () => {
    assert.deepEqual(buildThinkingOptions('o3-mini', { enabled: false, level: 'high' }), {});
  });
});

describe('resolveContextWindow — explicit override, table lookup, and [1m] marker', () => {
  it('an explicit positive contextWindow wins over everything', async () => {
    assert.equal(await resolveContextWindow(cfg({ contextWindow: 512345, model: 'gpt-4o' })), 512345);
  });

  it('a non-positive contextWindow is ignored and falls through to the table', async () => {
    assert.equal(await resolveContextWindow(cfg({ contextWindow: 0, model: 'gpt-4o' })), 128000);
  });

  it('empty model with no override returns the default window (128000)', async () => {
    assert.equal(await resolveContextWindow(cfg({})), 128000);
  });

  it('gemini-2.5-pro resolves from the static table (1048576)', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'gemini-2.5-pro' })), 1048576);
  });

  it('claude-opus-5 resolves to the 1M key', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'claude-opus-5' })), 1_000_000);
  });

  it('claude-opus-4 is the 200k key', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'claude-opus-4-20250101' })), 200000);
  });

  it('a [1m] marker promotes a 200k base id to 1M', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'claude-opus-4[1m]' })), 1_000_000);
  });

  it('a trailing -1m marker on an unknown id promotes the default to 1M', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'mystery-model-1m' })), 1_000_000);
  });

  it('an unknown model with no marker gets the default window', async () => {
    assert.equal(await resolveContextWindow(cfg({ model: 'totally-unknown-xyz' })), 128000);
  });
});

describe('inferProviderFromApiKey — prefix dispatch', () => {
  const expectProvider = (key: string, provider: string) => {
    const inferred = inferProviderFromApiKey(key);
    assert.ok(inferred, `expected a provider for key ${key}`);
    assert.equal(inferred!.provider, provider);
    assert.equal(inferred!.defaultModel, MODEL_PREFERENCE[provider][0]);
    assert.equal(inferred!.defaultBaseUrl, (PROVIDER_BASE_URLS as any)[provider] || '');
  };

  it('sk-ant- -> anthropic', () => expectProvider('sk-ant-abc123', 'anthropic'));
  it('AIzaSy -> google', () => expectProvider('AIzaSyFakeKey', 'google'));
  it('sk-or- -> openrouter (checked before generic sk-)', () => expectProvider('sk-or-v1-xyz', 'openrouter'));
  it('sk-di- -> deepinfra', () => expectProvider('sk-di-abc', 'deepinfra'));
  it('gsk_ -> groq', () => expectProvider('gsk_abc', 'groq'));
  it('xai- -> xai', () => expectProvider('xai-abc', 'xai'));
  it('nvapi- -> nvidia', () => expectProvider('nvapi-abc', 'nvidia'));
  it('pplx- -> perplexity', () => expectProvider('pplx-abc', 'perplexity'));
  it('hf_ -> huggingface', () => expectProvider('hf_abc', 'huggingface'));
  it('cohere_ -> cohere', () => expectProvider('cohere_abc', 'cohere'));
  it('bare sk- -> openai (generic fallback)', () => expectProvider('sk-plainsecret', 'openai'));

  it('an unrecognized prefix returns null', () => {
    assert.equal(inferProviderFromApiKey('random-key-no-prefix'), null);
  });

  it('an empty key returns null', () => {
    assert.equal(inferProviderFromApiKey(''), null);
  });

  it('order matters: sk-or- is not swallowed by sk-', () => {
    assert.notEqual(inferProviderFromApiKey('sk-or-xxx')!.provider, 'openai');
  });
});

describe('describeProviders — descriptor shape + cross-field linkage', () => {
  const providers = describeProviders();
  const byId = new Map(providers.map(p => [p.id, p]));

  it('has one descriptor per MODEL_PREFERENCE entry', () => {
    assert.equal(providers.length, Object.keys(MODEL_PREFERENCE).length);
  });

  it('every descriptor carries the required fields with correct types', () => {
    for (const p of providers) {
      assert.equal(typeof p.id, 'string');
      assert.ok(Array.isArray(p.envVars));
      assert.equal(typeof p.baseUrl, 'string');
      assert.ok(Array.isArray(p.models));
      assert.equal(typeof p.nativeSdk, 'boolean');
      assert.ok(Array.isArray(p.keyPrefixes));
    }
  });

  it('models array is the exact MODEL_PREFERENCE list for each provider', () => {
    for (const p of providers) {
      assert.deepEqual(p.models, MODEL_PREFERENCE[p.id]);
    }
  });

  it('anthropic is a native SDK provider with the sk-ant- prefix and an env var', () => {
    const a = byId.get('anthropic')!;
    assert.equal(a.nativeSdk, true);
    assert.ok(a.keyPrefixes.includes('sk-ant-'));
    assert.ok(a.envVars.includes('ANTHROPIC_API_KEY'));
  });

  it('openai is NOT a native SDK provider and exposes the generic sk- prefix', () => {
    const o = byId.get('openai')!;
    assert.equal(o.nativeSdk, false);
    assert.deepEqual(o.keyPrefixes, ['sk-']);
    assert.ok(o.envVars.includes('OPENAI_API_KEY'));
  });

  it('every inferable key prefix maps back to a descriptor', () => {
    for (const p of providers) {
      for (const prefix of p.keyPrefixes) {
        const inferred = inferProviderFromApiKey(`${prefix}sample`);
        assert.ok(inferred, `prefix ${prefix} should be inferable`);
      }
    }
  });
});

describe('mergeBetaTokens — dedup + comma-join', () => {
  it('null existing yields just the new token', () => {
    assert.equal(mergeBetaTokens(null, ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
  });

  it('undefined existing yields just the new token', () => {
    assert.equal(mergeBetaTokens(undefined, 'beta-x'), 'beta-x');
  });

  it('empty-string existing yields just the new token', () => {
    assert.equal(mergeBetaTokens('', 'beta-x'), 'beta-x');
  });

  it('appends a distinct token, comma-joined', () => {
    assert.equal(
      mergeBetaTokens('extended-cache-ttl-2025-04-11', ONE_MILLION_CONTEXT_BETA),
      `extended-cache-ttl-2025-04-11,${ONE_MILLION_CONTEXT_BETA}`,
    );
  });

  it('does not duplicate an already-present token', () => {
    assert.equal(mergeBetaTokens('a,b', 'b'), 'a,b');
  });

  it('is idempotent when the only token already matches', () => {
    assert.equal(mergeBetaTokens(ONE_MILLION_CONTEXT_BETA, ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
  });

  it('trims whitespace around existing tokens', () => {
    assert.equal(mergeBetaTokens(' a , b ', 'c'), 'a,b,c');
  });

  it('drops empty segments from the existing header', () => {
    assert.equal(mergeBetaTokens('a,,b', 'c'), 'a,b,c');
  });

  it('a header of only separators/whitespace collapses to just the new token', () => {
    assert.equal(mergeBetaTokens('  ,  , ', 'solo'), 'solo');
  });
});
