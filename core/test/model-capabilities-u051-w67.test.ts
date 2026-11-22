import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  normalizeModelStringForAPI,
  getSmallFastModel,
  SMALL_FAST_MODEL_ENV_VARS,
  getContextWindow,
} from '../src/model-capabilities';
import { resolveModel } from '../src/provider';

// ---------------------------------------------------------------------------
// U051-w67 — normalizeModelStringForAPI (strip the synthetic [1m]/[2m] marker
// before the id reaches the provider API) + getSmallFastModel (aux-call knob).
// ---------------------------------------------------------------------------

describe('normalizeModelStringForAPI', () => {
  it('strips the [1m] bracket marker (the reference parity)', () => {
    assert.equal(normalizeModelStringForAPI('claude-opus-5[1m]'), 'claude-opus-5');
    assert.equal(normalizeModelStringForAPI('claude-sonnet-4-5[1m]'), 'claude-sonnet-4-5');
  });

  it('strips the [2m] bracket marker', () => {
    assert.equal(normalizeModelStringForAPI('claude-opus-5[2m]'), 'claude-opus-5');
  });

  it('is case-insensitive and strips every occurrence (Claude models)', () => {
    assert.equal(normalizeModelStringForAPI('claude-opus-5[1M]'), 'claude-opus-5');
    // `.replace(/.../gi)` global — both markers go even if the input repeats them.
    assert.equal(normalizeModelStringForAPI('claude-opus-5[1m][2m]'), 'claude-opus-5');
  });

  it('strips the AMI -1m/_1m suffix forms the local detector also recognizes', () => {
    assert.equal(normalizeModelStringForAPI('claude-opus-4-1m'), 'claude-opus-4');
    assert.equal(normalizeModelStringForAPI('claude-opus-4_1m'), 'claude-opus-4');
  });

  it('leaves a bare id (no marker) untouched', () => {
    assert.equal(normalizeModelStringForAPI('claude-opus-5'), 'claude-opus-5');
    assert.equal(normalizeModelStringForAPI('gpt-5.6-luna'), 'gpt-5.6-luna');
  });

  it('does not strip a legit mid-id "1m" that is not a boundary marker', () => {
    // `\b` means `-1m` only matches at a word boundary end; an embedded digit-run
    // like `...1mini` must survive.
    assert.equal(normalizeModelStringForAPI('model-1mini'), 'model-1mini');
  });

  it('passes a non-string through unchanged (defensive)', () => {
    assert.equal(normalizeModelStringForAPI(undefined as unknown as string), undefined as unknown as string);
  });

  it('keeps the context budget sized from the marker while the API id is bare (wiring)', () => {
    // The marker still sizes the local window (config.model keeps it elsewhere)…
    assert.equal(getContextWindow('claude-opus-4[1m]'), 1_000_000);
    // …but the id the SDK receives is normalized.
    const m = resolveModel({ apiKey: 'sk-ant-test', provider: 'anthropic', model: 'claude-opus-5[1m]' });
    assert.equal((m as { modelId?: string }).modelId, 'claude-opus-5');
  });

  it('passes a third-party model id through verbatim (the [1m] may be part of its real name)', () => {
    assert.equal(normalizeModelStringForAPI('deepseek-v4-pro[1m]'), 'deepseek-v4-pro[1m]');
    assert.equal(normalizeModelStringForAPI('gpt-5.6-luna[1m]'), 'gpt-5.6-luna[1m]');
    const m = resolveModel({ baseUrl: 'https://api.deepseek.com/anthropic', apiKey: 'sk-test', provider: 'anthropic', model: 'deepseek-v4-pro[1m]' });
    assert.equal((m as { modelId?: string }).modelId, 'deepseek-v4-pro[1m]');
  });
});

describe('getSmallFastModel', () => {
  const SAVED: Record<string, string | undefined> = {};
  afterEach(() => {
    for (const key of SMALL_FAST_MODEL_ENV_VARS) {
      if (SAVED[key] === undefined) delete process.env[key];
      else process.env[key] = SAVED[key];
      delete SAVED[key];
    }
  });
  const stash = (key: string) => { SAVED[key] = process.env[key]; };

  it('returns the fallback when no env var is set (aux calls reuse the main model)', () => {
    const env = {} as NodeJS.ProcessEnv;
    assert.equal(getSmallFastModel('claude-opus-5', env), 'claude-opus-5');
    assert.equal(getSmallFastModel(undefined, env), undefined);
  });

  it('honors ANTHROPIC_SMALL_FAST_MODEL over the fallback (the reference parity)', () => {
    const env = { ANTHROPIC_SMALL_FAST_MODEL: 'claude-haiku-4' } as NodeJS.ProcessEnv;
    assert.equal(getSmallFastModel('claude-opus-5', env), 'claude-haiku-4');
  });

  it('honors the provider-neutral AMI_SMALL_FAST_MODEL', () => {
    const env = { AMI_SMALL_FAST_MODEL: 'gpt-4o-mini' } as NodeJS.ProcessEnv;
    assert.equal(getSmallFastModel('claude-opus-5', env), 'gpt-4o-mini');
  });

  it('prefers ANTHROPIC_SMALL_FAST_MODEL when both are set (declared order)', () => {
    const env = { ANTHROPIC_SMALL_FAST_MODEL: 'claude-haiku-4', AMI_SMALL_FAST_MODEL: 'gpt-4o-mini' } as NodeJS.ProcessEnv;
    assert.equal(getSmallFastModel('x', env), 'claude-haiku-4');
  });

  it('trims surrounding whitespace and ignores an empty/blank value', () => {
    assert.equal(getSmallFastModel('fb', { ANTHROPIC_SMALL_FAST_MODEL: '  claude-haiku-4  ' } as NodeJS.ProcessEnv), 'claude-haiku-4');
    assert.equal(getSmallFastModel('fb', { ANTHROPIC_SMALL_FAST_MODEL: '   ' } as NodeJS.ProcessEnv), 'fb');
  });

  it('reads process.env at call time by default', () => {
    stash('ANTHROPIC_SMALL_FAST_MODEL');
    stash('AMI_SMALL_FAST_MODEL');
    delete process.env.AMI_SMALL_FAST_MODEL;
    process.env.ANTHROPIC_SMALL_FAST_MODEL = 'claude-haiku-4';
    assert.equal(getSmallFastModel('main'), 'claude-haiku-4');
    delete process.env.ANTHROPIC_SMALL_FAST_MODEL;
    assert.equal(getSmallFastModel('main'), 'main');
  });
});
