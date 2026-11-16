import { describe, it, afterEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  claudeReasoningCapabilities,
  getModelCapabilities,
  modelAlwaysOnThinking,
  clearDynamicCapabilities,
  getContextWindow,
  getMaxOutputTokens,
  modelCapabilitiesCacheDir,
  modelCapabilitiesCachePath,
  readModelCapabilitiesCache,
  writeModelCapabilitiesCache,
  warmModelCapabilitiesFromCache,
  discoveryToCacheEntries,
  MODEL_CAPABILITIES_CACHE_FILE,
} from '../src/model-capabilities';
import { buildThinkingOptions } from '../src/provider';
import { listModels } from '../src/model-registry';

// ---------------------------------------------------------------------------
// U051-w68 — (1) per-model alwaysOnThinking capability (the 5.x Claude line
// rejects a disabled-thinking request) and (2) a persistent on-disk cache of
// provider-advertised model capabilities (context/output windows).
// ---------------------------------------------------------------------------

const CACHE_ENV = 'SUPERINFERENCE_MODEL_CACHE_DIR';
const tmpDirs: string[] = [];

function mkCacheDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-modelcache-'));
  tmpDirs.push(dir);
  return dir;
}

afterEach(() => {
  delete process.env[CACHE_ENV];
  clearDynamicCapabilities();
});

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

describe('alwaysOnThinking capability (U051-w68)', () => {
  it('flags the 5.x Claude line as always-on (rejects disabled thinking)', () => {
    assert.equal(claudeReasoningCapabilities('claude-opus-5-5')?.alwaysOnThinking, true);
    assert.equal(claudeReasoningCapabilities('claude-sonnet-5')?.alwaysOnThinking, true);
    // A dated snapshot of a 5.x id still resolves to always-on.
    assert.equal(claudeReasoningCapabilities('claude-opus-5-20260101')?.alwaysOnThinking, true);
  });

  it('does NOT flag the 4.x family (including adaptive 4.6) as always-on', () => {
    assert.equal(claudeReasoningCapabilities('claude-opus-4-6')?.alwaysOnThinking, false);
    assert.equal(claudeReasoningCapabilities('claude-sonnet-4-6')?.alwaysOnThinking, false);
    assert.equal(claudeReasoningCapabilities('claude-opus-4-5')?.alwaysOnThinking, false);
  });

  it('modelAlwaysOnThinking resolves through getModelCapabilities', () => {
    assert.equal(modelAlwaysOnThinking('claude-opus-5-5'), true);
    assert.equal(modelAlwaysOnThinking('claude-sonnet-5'), true);
    assert.equal(modelAlwaysOnThinking('claude-opus-4-5'), false);
    assert.equal(modelAlwaysOnThinking('gpt-6'), false);
    assert.equal(modelAlwaysOnThinking(''), false);
    // @ts-expect-error — non-string id is defended
    assert.equal(modelAlwaysOnThinking(undefined), false);
  });

  it('getModelCapabilities carries the flag for callers', () => {
    assert.equal(getModelCapabilities('claude-opus-5-5')?.alwaysOnThinking, true);
    assert.equal(getModelCapabilities('claude-opus-4-5')?.alwaysOnThinking, false);
  });
});

describe('buildThinkingOptions honors alwaysOnThinking (U051-w68)', () => {
  // RED on revert: without the always-on branch, an always-on model with thinking
  // OFF returns {} (no thinking sent) — this asserts the adaptive-low floor instead.
  it('sends adaptive low even when thinking is off for an always-on model', () => {
    assert.deepEqual(
      buildThinkingOptions('claude-opus-5-5', { enabled: false, level: 'off' }),
      { providerOptions: { anthropic: { thinking: { type: 'adaptive' }, effort: 'low' } } },
    );
  });

  it('still omits thinking options when off for a NON-always-on model', () => {
    assert.deepEqual(buildThinkingOptions('claude-opus-4-5', { enabled: false, level: 'off' }), {});
    assert.deepEqual(buildThinkingOptions('gpt-6', { enabled: false, level: 'off' }), {});
  });

  it('leaves the enabled-thinking path unchanged for an always-on model', () => {
    const opts = buildThinkingOptions('claude-opus-5-5', { enabled: true, level: 'high' }) as {
      providerOptions: { anthropic: { thinking: { type: string }; effort: string } };
    };
    assert.equal(opts.providerOptions.anthropic.thinking.type, 'adaptive');
    assert.equal(opts.providerOptions.anthropic.effort, 'high');
  });
});

describe('model-capabilities cache dir/path resolution (U051-w68)', () => {
  it('returns undefined when the env var is unset or blank', () => {
    assert.equal(modelCapabilitiesCacheDir({}), undefined);
    assert.equal(modelCapabilitiesCacheDir({ [CACHE_ENV]: '   ' }), undefined);
  });

  it('trims and returns the configured directory', () => {
    assert.equal(modelCapabilitiesCacheDir({ [CACHE_ENV]: ' /tmp/x ' }), '/tmp/x');
  });

  it('builds the cache file path under the directory', () => {
    assert.equal(modelCapabilitiesCachePath('/tmp/x'), path.join('/tmp/x', MODEL_CAPABILITIES_CACHE_FILE));
  });
});

describe('model-capabilities cache write/read round-trip (U051-w68)', () => {
  it('writes a sorted, timestamped file with 0600 perms and reads it back', () => {
    const dir = mkCacheDir();
    const written = writeModelCapabilitiesCache(dir, [
      { id: 'zeta-model', contextWindow: 300000 },
      { id: 'alpha-model', contextWindow: 100000, maxOutputTokens: 8192 },
    ], 1234);
    assert.equal(written, true);

    const file = modelCapabilitiesCachePath(dir);
    const mode = fs.statSync(file).mode & 0o777;
    assert.equal(mode, 0o600);

    const cache = readModelCapabilitiesCache(dir);
    assert.ok(cache);
    assert.equal(cache!.timestamp, 1234);
    // Entries are sorted by id for a deterministic file.
    assert.deepEqual(cache!.models.map(m => m.id), ['alpha-model', 'zeta-model']);
    assert.equal(cache!.models[0].maxOutputTokens, 8192);
  });

  it('skips the write (no mtime churn) when the model set is unchanged', () => {
    const dir = mkCacheDir();
    assert.equal(writeModelCapabilitiesCache(dir, [{ id: 'm', contextWindow: 200000 }], 1), true);
    // Same models, different timestamp → skipped.
    assert.equal(writeModelCapabilitiesCache(dir, [{ id: 'm', contextWindow: 200000 }], 999), false);
    // The on-disk timestamp is still the original (nothing rewritten).
    assert.equal(readModelCapabilitiesCache(dir)!.timestamp, 1);
    // A genuine change rewrites.
    assert.equal(writeModelCapabilitiesCache(dir, [{ id: 'm', contextWindow: 500000 }], 2), true);
    assert.equal(readModelCapabilitiesCache(dir)!.models[0].contextWindow, 500000);
  });

  it('returns false when caching is off (no dir)', () => {
    assert.equal(writeModelCapabilitiesCache(undefined, [{ id: 'm', contextWindow: 1 }]), false);
    assert.equal(readModelCapabilitiesCache(undefined), null);
    assert.equal(warmModelCapabilitiesFromCache(undefined), 0);
  });

  it('returns null for a missing or malformed cache file', () => {
    const dir = mkCacheDir();
    // Missing file.
    assert.equal(readModelCapabilitiesCache(dir), null);
    clearDynamicCapabilities();
    // Malformed JSON.
    fs.writeFileSync(modelCapabilitiesCachePath(dir), '{not json');
    assert.equal(readModelCapabilitiesCache(dir), null);
    clearDynamicCapabilities();
    // Valid JSON, wrong shape.
    fs.writeFileSync(modelCapabilitiesCachePath(dir), JSON.stringify({ models: 'nope' }));
    assert.equal(readModelCapabilitiesCache(dir), null);
  });

  it('drops malformed entries but keeps the valid ones', () => {
    const dir = mkCacheDir();
    fs.writeFileSync(modelCapabilitiesCachePath(dir), JSON.stringify({
      timestamp: 5,
      models: [
        { id: 'good', contextWindow: 100 },
        { id: '', contextWindow: 1 },          // empty id dropped
        { notId: true },                        // no id dropped
        { id: 'neg', contextWindow: -4 },       // non-positive window ignored (id kept, no window)
      ],
    }));
    const cache = readModelCapabilitiesCache(dir)!;
    assert.deepEqual(cache.models.map(m => m.id), ['good', 'neg']);
    assert.equal(cache.models[1].contextWindow, undefined);
  });

  it('memoizes reads until a write or clearDynamicCapabilities', () => {
    const dir = mkCacheDir();
    writeModelCapabilitiesCache(dir, [{ id: 'm', contextWindow: 1 }], 1);
    // External mutation is not seen (memoized).
    fs.writeFileSync(modelCapabilitiesCachePath(dir), JSON.stringify({ timestamp: 2, models: [{ id: 'm', contextWindow: 9 }] }));
    assert.equal(readModelCapabilitiesCache(dir)!.models[0].contextWindow, 1);
    // Clearing invalidates the memo.
    clearDynamicCapabilities();
    assert.equal(readModelCapabilitiesCache(dir)!.models[0].contextWindow, 9);
  });
});

describe('warmModelCapabilitiesFromCache seeds the runtime tables (U051-w68)', () => {
  it('warms context windows and output caps from the cache', () => {
    const dir = mkCacheDir();
    writeModelCapabilitiesCache(dir, [
      { id: 'si-warm-ctx', contextWindow: 654321 },
      { id: 'si-warm-out', maxOutputTokens: 44444 },
    ], 1);
    const warmed = warmModelCapabilitiesFromCache(dir);
    assert.equal(warmed, 2);
    assert.equal(getContextWindow('si-warm-ctx'), 654321);
    assert.equal(getMaxOutputTokens('si-warm-out'), 44444);
  });

  it('listModels warms the cache before returning a static provider list (end-to-end)', async () => {
    const dir = mkCacheDir();
    writeModelCapabilitiesCache(dir, [{ id: 'si-e2e-warm', contextWindow: 321000 }], 1);
    clearDynamicCapabilities();
    process.env[CACHE_ENV] = dir;
    // anthropic is a static provider → listModels returns the offline list, but the
    // warm step runs first, so the cached window is now live.
    const models = await listModels({ provider: 'anthropic' });
    assert.ok(Array.isArray(models));
    assert.equal(getContextWindow('si-e2e-warm'), 321000);
  });
});

describe('discoveryToCacheEntries merges windows + output caps (U051-w68)', () => {
  it('merges per-id context windows and output caps', () => {
    const entries = discoveryToCacheEntries({
      contextWindows: { a: 100, b: 200 },
      outputCaps: { a: 8, c: 9 },
    });
    const byId = Object.fromEntries(entries.map(e => [e.id, e]));
    assert.deepEqual(byId.a, { id: 'a', contextWindow: 100, maxOutputTokens: 8 });
    assert.deepEqual(byId.b, { id: 'b', contextWindow: 200 });
    assert.deepEqual(byId.c, { id: 'c', maxOutputTokens: 9 });
  });
});
