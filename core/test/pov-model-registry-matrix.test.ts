import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  detectProvider,
  listProviders,
  listModels,
  validateModel,
  formatModelList,
} from '../src/model-registry';
import {
  getContextWindow,
  fitOutputTokens,
  capThinkingBudget,
  discoverFromModelList,
  registerModelCapabilities,
  clearDynamicCapabilities,
  getModelCapabilities,
  DEFAULT_CONTEXT_WINDOW,
  DEFAULT_MAX_OUTPUT_TOKENS,
  OUTPUT_FIT_SAFETY_TOKENS,
  MIN_FITTED_OUTPUT_TOKENS,
  MIN_THINKING_BUDGET,
  THINKING_HEADROOM_MARGIN,
  type ModelCapabilities,
} from '../src/model-capabilities';
import { MODEL_PREFERENCE } from '../src/provider';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// POV: core model registry + model-capabilities math.
//
// Every expectation is computed from the published spec (the invariants stated
// in each function's doc comment / the static tables that ARE the spec), never
// copied from the implementation body, and then checked against the REAL
// function's observed return value. No source-string scraping.
// ---------------------------------------------------------------------------

function cfg(partial: Partial<ProviderConfig>): ProviderConfig {
  return { baseUrl: '', apiKey: '', model: '', ...partial };
}

// Credential prefixes are detection markers, not live secrets; split anyway so
// no contiguous key-shaped literal appears in source (push-protection safe).
const ANT = 'sk-' + 'ant-' + 'synthetic00';
const GOOGLE_KEY = 'AIza' + 'SyntheticGoogleKey000';
const OPENROUTER_KEY = 'sk-' + 'or-' + 'synthetic00';
const GROQ_KEY = 'gsk_' + 'synthetic00';
const XAI_KEY = 'xai-' + 'synthetic00';
const PPLX_KEY = 'pplx-' + 'synthetic00';
const OPENAI_KEY = 'sk-' + 'synthetic00';

describe('detectProvider — explicit provider always wins', () => {
  it('returns config.provider verbatim regardless of key/url shape', () => {
    assert.equal(detectProvider(cfg({ provider: 'my-gateway', apiKey: ANT })), 'my-gateway');
    assert.equal(detectProvider(cfg({ provider: 'ollama', baseUrl: 'https://api.anthropic.com' })), 'ollama');
  });
});

describe('detectProvider — vertex env detection precedes key patterns', () => {
  const saved = {
    proj: process.env.ANTHROPIC_VERTEX_PROJECT_ID,
    use: process.env.CLAUDE_CODE_USE_VERTEX,
  };
  beforeEach(() => {
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    delete process.env.CLAUDE_CODE_USE_VERTEX;
  });
  afterEach(() => {
    if (saved.proj === undefined) delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    else process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved.proj;
    if (saved.use === undefined) delete process.env.CLAUDE_CODE_USE_VERTEX;
    else process.env.CLAUDE_CODE_USE_VERTEX = saved.use;
  });

  it('ANTHROPIC_VERTEX_PROJECT_ID overrides an sk-ant- key', () => {
    process.env.ANTHROPIC_VERTEX_PROJECT_ID = 'proj-123';
    assert.equal(detectProvider(cfg({ apiKey: ANT })), 'anthropic-vertex');
  });
  it('CLAUDE_CODE_USE_VERTEX overrides an sk-ant- key', () => {
    process.env.CLAUDE_CODE_USE_VERTEX = '1';
    assert.equal(detectProvider(cfg({ apiKey: ANT })), 'anthropic-vertex');
  });
  it('without the env vars an sk-ant- key resolves to anthropic', () => {
    assert.equal(detectProvider(cfg({ apiKey: ANT })), 'anthropic');
  });
});

describe('detectProvider — API key prefix patterns', () => {
  const saved = {
    proj: process.env.ANTHROPIC_VERTEX_PROJECT_ID,
    use: process.env.CLAUDE_CODE_USE_VERTEX,
  };
  beforeEach(() => {
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    delete process.env.CLAUDE_CODE_USE_VERTEX;
  });
  afterEach(() => {
    if (saved.proj !== undefined) process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved.proj;
    if (saved.use !== undefined) process.env.CLAUDE_CODE_USE_VERTEX = saved.use;
  });

  const cases: Array<[string, string]> = [
    [ANT, 'anthropic'],
    [GOOGLE_KEY, 'google'],
    [OPENROUTER_KEY, 'openrouter'],
    [GROQ_KEY, 'groq'],
    [XAI_KEY, 'xai'],
    [PPLX_KEY, 'perplexity'],
    [OPENAI_KEY, 'openai'],
  ];
  for (const [key, expected] of cases) {
    it(`${expected} detected from its key prefix`, () => {
      assert.equal(detectProvider(cfg({ apiKey: key })), expected);
    });
  }

  it('sk-or- is distinguished from the bare sk- OpenAI fallback', () => {
    // sk-or- must win over the sk- fallback that runs last.
    assert.equal(detectProvider(cfg({ apiKey: OPENROUTER_KEY })), 'openrouter');
    assert.notEqual(detectProvider(cfg({ apiKey: OPENROUTER_KEY })), 'openai');
  });
});

describe('detectProvider — base URL patterns and default', () => {
  const saved = {
    proj: process.env.ANTHROPIC_VERTEX_PROJECT_ID,
    use: process.env.CLAUDE_CODE_USE_VERTEX,
  };
  beforeEach(() => {
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    delete process.env.CLAUDE_CODE_USE_VERTEX;
  });
  afterEach(() => {
    if (saved.proj !== undefined) process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved.proj;
    if (saved.use !== undefined) process.env.CLAUDE_CODE_USE_VERTEX = saved.use;
  });

  const cases: Array<[string, string]> = [
    ['https://generativelanguage.googleapis.com/v1beta', 'google'],
    ['https://us-central1-aiplatform.googleapis.com', 'google-vertex'],
    ['https://api.anthropic.com', 'anthropic'],
    ['https://openrouter.ai/api/v1', 'openrouter'],
    ['https://api.groq.com/openai/v1', 'groq'],
    ['https://api.mistral.ai/v1', 'mistral'],
    ['https://api.x.ai/v1', 'xai'],
    ['https://api.deepseek.com/v1', 'deepseek'],
    ['https://api.together.xyz/v1', 'togetherai'],
    ['http://localhost:11434/v1', 'ollama'],
    ['http://127.0.0.1:11434/v1', 'ollama'],
    ['https://api.openai.com/v1', 'openai'],
  ];
  for (const [url, expected] of cases) {
    it(`${expected} detected from base URL ${url}`, () => {
      assert.equal(detectProvider(cfg({ baseUrl: url })), expected);
    });
  }

  it('an empty/unknown config falls back to openai', () => {
    assert.equal(detectProvider(cfg({})), 'openai');
    assert.equal(detectProvider(cfg({ baseUrl: 'https://unknown.example.com', apiKey: 'nope' })), 'openai');
  });
});

describe('listProviders — mirrors the MODEL_PREFERENCE table', () => {
  it('returns one entry per configured provider with its model list', () => {
    const got = listProviders();
    const expected = Object.entries(MODEL_PREFERENCE).map(([id, models]) => ({ id, models }));
    assert.deepEqual(got, expected);
  });
  it('anthropic entry carries the static preference list', () => {
    const anthropic = listProviders().find(p => p.id === 'anthropic');
    assert.ok(anthropic);
    assert.deepEqual(anthropic!.models, MODEL_PREFERENCE.anthropic);
  });
});

describe('listModels — static providers map the preference table (no network)', () => {
  it('anthropic returns {id,name} for each preferred model', async () => {
    const models = await listModels(cfg({ provider: 'anthropic' }));
    assert.deepEqual(
      models,
      MODEL_PREFERENCE.anthropic.map(id => ({ id, name: id })),
    );
  });
  it('groq static list is returned verbatim', async () => {
    const models = await listModels(cfg({ provider: 'groq' }));
    assert.deepEqual(models.map(m => m.id), MODEL_PREFERENCE.groq);
  });
  it('a non-static provider with no base URL falls back to [] when it has no static list', async () => {
    // ollama is OpenAI-compatible (not static) and has no MODEL_PREFERENCE entry;
    // listOpenAIModels throws before any network call because baseUrl is empty.
    const models = await listModels(cfg({ provider: 'ollama', baseUrl: '' }));
    assert.deepEqual(models, []);
  });
});

describe('validateModel — behavioral (driven off the static anthropic list)', () => {
  it('an exact model id is valid and echoes the available list', async () => {
    const known = MODEL_PREFERENCE.anthropic[0];
    const res = await validateModel(cfg({ provider: 'anthropic' }), known);
    assert.equal(res.valid, true);
    assert.deepEqual(res.available, MODEL_PREFERENCE.anthropic);
  });
  it('a totally unknown id is invalid with no substring suggestion', async () => {
    const res = await validateModel(cfg({ provider: 'anthropic' }), 'totally-made-up-zzz');
    assert.equal(res.valid, false);
    assert.equal(res.suggestion, undefined);
    assert.deepEqual(res.available, MODEL_PREFERENCE.anthropic);
  });
  it('a substring query is invalid but offers a suggestion', async () => {
    const res = await validateModel(cfg({ provider: 'anthropic' }), 'claude-sonnet');
    assert.equal(res.valid, false);
    assert.ok(res.suggestion);
    assert.ok(res.suggestion!.includes('claude-sonnet'));
  });
  it('when the list cannot be retrieved it declines to invalidate', async () => {
    const res = await validateModel(cfg({ provider: 'ollama', baseUrl: '' }), 'anything');
    assert.deepEqual(res, { valid: true, available: [] });
  });
});

describe('formatModelList', () => {
  it('renders a header with count + provider and one indented line per model', () => {
    const out = formatModelList(['a', 'b'], 'anthropic');
    assert.equal(out, 'Available models (anthropic, 2):\n  a\n  b');
  });
  it('explains an empty list', () => {
    assert.equal(formatModelList([], 'openai'), 'Could not retrieve model list from provider.');
  });
});

describe('getContextWindow — static table + prefix precedence', () => {
  it('returns the exact published window for table entries', () => {
    assert.equal(getContextWindow('claude-opus-5'), 1_000_000);
    assert.equal(getContextWindow('gpt-6'), 1_050_000);
    assert.equal(getContextWindow('gemini-2.5-pro'), 1_048_576);
    assert.equal(getContextWindow('deepseek-chat'), 64_000);
  });
  it('a longer key wins over a shorter prefix (grok-4.20 vs grok-4.7)', () => {
    assert.equal(getContextWindow('grok-4.20'), 1_000_000);
    assert.equal(getContextWindow('grok-4.7'), 500_000);
  });
  it('an unknown model id falls back to the default window', () => {
    assert.equal(getContextWindow('totally-unknown-model-xyz'), DEFAULT_CONTEXT_WINDOW);
  });
  it('a gateway-prefixed id still matches the base key (unanchored)', () => {
    assert.equal(getContextWindow('openrouter/anthropic/claude-opus-5'), 1_000_000);
  });
});

describe('getContextWindow — [1m] / -1m marker bumps the window to >=1M', () => {
  it('bracketed [1m] on a 200k base id bumps to 1M', () => {
    // claude-haiku-4 base = 200k; the 1m beta marker forces at least 1M.
    assert.equal(getContextWindow('claude-haiku-4-5[1m]'), 1_000_000);
  });
  it('trailing -1m bumps a base window up to 1M', () => {
    assert.equal(getContextWindow('claude-sonnet-4-5-1m'), 1_000_000);
  });
  it('the marker on an unknown id bumps the default up to 1M', () => {
    assert.equal(getContextWindow('mystery-model-1m'), 1_000_000);
  });
  it('the marker never shrinks an already-larger window', () => {
    // gpt-6 is 1.05M; the marker must not pull it down to 1M.
    assert.equal(getContextWindow('gpt-6-1m'), 1_050_000);
  });
});

describe('getContextWindow — short-key boundary guard', () => {
  it('a <=3-char key preceded by an alnum char is NOT matched', () => {
    // 'o1' only appears mid-token (after an alnum), so it must be rejected and
    // the id falls through to the default window.
    assert.equal(getContextWindow('abco1def'), DEFAULT_CONTEXT_WINDOW);
  });
  it('a short key at the start of the id IS matched', () => {
    // 'o1' key = 200k (distinct from the longer 'o1-mini' key = 128k).
    assert.equal(getContextWindow('o1'), 200_000);
    assert.equal(getContextWindow('o1-preview'), 200_000);
    assert.equal(getContextWindow('o1-mini'), 128_000);
  });
});

describe('fitOutputTokens — fits under window, floors, and degenerate inputs', () => {
  it('returns the request when it fits beside the prompt', () => {
    assert.equal(fitOutputTokens(1000, 200_000, 1000), 1000);
  });
  it('clamps to window - prompt - safety when the ask is too large', () => {
    const window = 32_000;
    const prompt = 1000;
    const expected = window - prompt - OUTPUT_FIT_SAFETY_TOKENS;
    assert.equal(fitOutputTokens(256_000, window, prompt), expected);
  });
  it('floors to MIN_FITTED_OUTPUT_TOKENS when the window is nearly full', () => {
    assert.equal(fitOutputTokens(1000, 1000, 900), MIN_FITTED_OUTPUT_TOKENS);
  });
  it('a non-positive / non-finite request uses the default output budget', () => {
    assert.equal(fitOutputTokens(0, 200_000, 0), DEFAULT_MAX_OUTPUT_TOKENS);
    assert.equal(fitOutputTokens(NaN, 200_000, 0), DEFAULT_MAX_OUTPUT_TOKENS);
    assert.equal(fitOutputTokens(-5, 200_000, 0), DEFAULT_MAX_OUTPUT_TOKENS);
  });
  it('a non-positive / non-finite window returns the (floored) ask unchanged', () => {
    assert.equal(fitOutputTokens(5000, 0, 100), 5000);
    assert.equal(fitOutputTokens(5000.9, Infinity, 100), 5000);
  });
  it('a non-finite prompt is treated as zero', () => {
    assert.equal(fitOutputTokens(500, 200_000, NaN), 500);
  });
});

describe('capThinkingBudget — clamps to window - prompt - output headroom', () => {
  it('a non-positive / non-finite request yields 0', () => {
    assert.equal(capThinkingBudget(0, 200_000, 1000, 1000), 0);
    assert.equal(capThinkingBudget(NaN, 200_000, 1000, 1000), 0);
    assert.equal(capThinkingBudget(-100, 200_000, 1000, 1000), 0);
  });
  it('a non-positive window returns the floored request (no window to clamp to)', () => {
    assert.equal(capThinkingBudget(5000.7, 0, 100, 100), 5000);
  });
  it('returns the request unchanged when it already fits', () => {
    assert.equal(capThinkingBudget(10_000, 200_000, 1000, 1000), 10_000);
  });
  it('shrinks to usable - margin when the request overflows but margin leaves room', () => {
    const window = 40_000, prompt = 1000, output = 1000;
    const usable = window - prompt - output - OUTPUT_FIT_SAFETY_TOKENS;
    const expected = usable - THINKING_HEADROOM_MARGIN;
    assert.ok(expected >= MIN_THINKING_BUDGET);
    assert.equal(capThinkingBudget(50_000, window, prompt, output), expected);
  });
  it('falls back to the MIN budget when usable room is tiny but >= floor', () => {
    // usable = 6000-1000-1000-1024 = 2976; withMargin < floor, but usable >= floor.
    assert.equal(capThinkingBudget(50_000, 6000, 1000, 1000), MIN_THINKING_BUDGET);
  });
  it('returns 0 when even the floor would overflow the window', () => {
    assert.equal(capThinkingBudget(50_000, 1500, 1000, 1000), 0);
  });
});

describe('discoverFromModelList + dynamic capability registry round-trip', () => {
  beforeEach(() => clearDynamicCapabilities());
  afterEach(() => clearDynamicCapabilities());

  it('registers advertised windows so getContextWindow resolves them', () => {
    const id = 'vendor/custom-served-model-aaa';
    const res = discoverFromModelList([{ id, context_length: 50_000 }]);
    assert.equal(res.contextWindows[id], 50_000);
    assert.ok(res.registered.includes(id));
    // The window is now readable through the public accessor.
    assert.equal(getContextWindow(id), 50_000);
  });

  it('classifies a gpt>=5 discovered id as a reasoning model', () => {
    const id = 'gpt-7-experimental-bbb';
    discoverFromModelList([{ id, context_length: 60_000 }]);
    const caps = getModelCapabilities(id);
    assert.ok(caps);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('classifies a plain chat id as non-reasoning (null) but still registered', () => {
    const id = 'plain-chat-model-ccc';
    const res = discoverFromModelList([{ id, max_model_len: 8000 }]);
    assert.ok(res.registered.includes(id));
    assert.equal(getModelCapabilities(id), null);
  });

  it('does not re-register an id already present in the dynamic map', () => {
    const id = 'plain-chat-model-ddd';
    discoverFromModelList([{ id, context_length: 9000 }]);
    const second = discoverFromModelList([{ id, context_length: 9000 }]);
    assert.ok(!second.registered.includes(id));
  });

  it('skips records with no id and no name', () => {
    const res = discoverFromModelList([{ context_length: 123 } as Record<string, unknown>]);
    assert.deepEqual(res.registered, []);
    assert.deepEqual(res.contextWindows, {});
  });

  it('records without an advertised window register caps but no window entry', () => {
    const id = 'windowless-model-eee';
    const res = discoverFromModelList([{ id }]);
    assert.ok(res.registered.includes(id));
    assert.equal(res.contextWindows[id], undefined);
  });
});

describe('registerModelCapabilities / clearDynamicCapabilities round-trip', () => {
  beforeEach(() => clearDynamicCapabilities());
  afterEach(() => clearDynamicCapabilities());

  const caps: ModelCapabilities = {
    reasoning: true,
    defaultThinkingLevel: 'high',
    supportsAdaptiveThinking: true,
    requiresTemperatureOne: false,
    temperatureMustBeUnset: false,
    maxThinkingBudget: 64_000,
  };

  it('a registered capability is returned (case-insensitive lookup)', () => {
    registerModelCapabilities('My-Custom-Model', caps);
    assert.deepEqual(getModelCapabilities('my-custom-model'), caps);
    assert.deepEqual(getModelCapabilities('MY-CUSTOM-MODEL'), caps);
  });

  it('an explicit null override beats the pattern table (forces non-reasoning)', () => {
    // 'o1-foo' would otherwise classify as a reasoning model via the pattern table.
    assert.ok(getModelCapabilities('o1-foo'));
    registerModelCapabilities('o1-foo', null);
    assert.equal(getModelCapabilities('o1-foo'), null);
  });

  it('clearing the registry restores pattern-table classification', () => {
    registerModelCapabilities('o1-foo', null);
    assert.equal(getModelCapabilities('o1-foo'), null);
    clearDynamicCapabilities();
    assert.ok(getModelCapabilities('o1-foo'));
  });

  it('non-string input is rejected by getModelCapabilities', () => {
    assert.equal(getModelCapabilities(undefined as unknown as string), null);
  });
});
