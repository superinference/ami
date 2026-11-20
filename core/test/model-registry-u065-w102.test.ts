/**
 * U065 (w102) — model-registry: model-string initialization await-barrier.
 *
 * Parity gap (modelStrings.ts:L147-166): the reference exposes
 * `ensureModelStringsInitialized()` — an explicit async barrier called before
 * generating model-selection options so the displayed/region strings are correct
 * rather than interim defaults. For Bedrock it awaits the inference-profile fetch;
 * for every other provider it resolves synchronously; an already-initialized state
 * returns immediately (sequential-guarded, single fetch per concurrent burst).
 *
 * AMI's `listModels` previously returned the static Bedrock list immediately with no
 * await-until-ready barrier for region/profile-correct strings. This suite proves the
 * new {@link ensureModelStringsInitialized} barrier:
 *   1. non-Bedrock resolves immediately and yields canonical (passthrough) wire ids;
 *   2. Bedrock awaits the injected async profile resolver before returning, and the
 *      snapshot carries region/profile-correct wire ids;
 *   3. an already-initialized signature returns the cached snapshot WITHOUT
 *      re-invoking the resolver (idempotent fast path);
 *   4. concurrent callers for one signature share a single resolver invocation;
 *   5. `getBuiltinModelStrings` serves the warmed snapshot after the barrier ran;
 *   6. `listModels` awaits the barrier on the Bedrock static path (end-to-end wiring).
 *
 * RED before the fix: `ensureModelStringsInitialized` does not exist, and
 * `listModels` never awaits a profile resolver — cases 2/3/4/6 fail to import/observe.
 *
 * Run from core/core: node --import tsx --test test/model-registry-u065-w102.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  ensureModelStringsInitialized,
  getBuiltinModelStrings,
  listModels,
  modelStringsInitHooks,
  MODEL_SHORT_KEYS,
} from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

const BEDROCK_ENV = [
  'CLAUDE_CODE_USE_BEDROCK',
  'SUPERINFERENCE_USE_BEDROCK',
  'AMI_BEDROCK_INFERENCE_PROFILES',
  'AMI_BEDROCK_REGION',
  'CLAUDE_CODE_USE_VERTEX',
  'ANTHROPIC_VERTEX_PROJECT_ID',
  'CLAUDE_CODE_USE_FOUNDRY',
  'SUPERINFERENCE_USE_FOUNDRY',
  'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
] as const;

const defaultResolver = modelStringsInitHooks.resolveBedrockProfiles;
let saved: Array<[string, string | undefined]> = [];

beforeEach(() => {
  saved = [];
  for (const k of BEDROCK_ENV) { saved.push([k, process.env[k]]); delete process.env[k]; }
});
afterEach(() => {
  for (const [k, v] of saved) { if (v !== undefined) process.env[k] = v; else delete process.env[k]; }
  modelStringsInitHooks.resolveBedrockProfiles = defaultResolver;
});

const cfg = (over: Partial<ProviderConfig> = {}): ProviderConfig => ({ provider: 'anthropic', ...over });
// Explicit Bedrock provider: detectProvider returns config.provider verbatim when
// set, so a Bedrock-class config must declare it (the env toggle only applies when
// provider is unset). Profiles still come from AMI_BEDROCK_INFERENCE_PROFILES / hook.
const bed = (): ProviderConfig => ({ provider: 'amazon-bedrock' });

describe('U065 (w102) — ensureModelStringsInitialized barrier', () => {
  it('non-Bedrock resolves immediately with canonical (passthrough) wire ids', async () => {
    let called = 0;
    modelStringsInitHooks.resolveBedrockProfiles = async () => { called++; return []; };
    const rows = await ensureModelStringsInitialized(cfg({ provider: 'anthropic' }));
    // No Bedrock → the resolver is never consulted.
    assert.equal(called, 0);
    assert.deepEqual(rows.map(r => r.key), Object.keys(MODEL_SHORT_KEYS));
    for (const row of rows) assert.equal(row.wireId, row.firstParty);
  });

  it('Bedrock awaits the async profile resolver before returning region-correct strings', async () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-opus-4-6-v1:0';
    let resolved = false;
    let release!: () => void;
    const gate = new Promise<void>(r => { release = r; });
    modelStringsInitHooks.resolveBedrockProfiles = async () => {
      await gate;
      resolved = true;
      return ['us.anthropic.claude-opus-4-6-v1:0'];
    };
    const p = ensureModelStringsInitialized(bed());
    // The barrier must still be pending until the profile fetch completes.
    await Promise.resolve();
    assert.equal(resolved, false, 'barrier resolved before the profile fetch completed');
    release();
    const rows = await p;
    assert.equal(resolved, true);
    const opus46 = rows.find(r => r.key === 'opus46');
    assert.ok(opus46);
    assert.equal(opus46!.wireId, 'us.anthropic.claude-opus-4-6-v1:0');
  });

  it('already-initialized signature returns the cached snapshot without re-resolving', async () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-sonnet-4-6-idem';
    let called = 0;
    modelStringsInitHooks.resolveBedrockProfiles = async () => { called++; return ['us.anthropic.claude-sonnet-4-6-idem']; };
    const first = await ensureModelStringsInitialized(bed());
    const second = await ensureModelStringsInitialized(bed());
    assert.equal(called, 1, 'resolver must run once for an already-initialized signature');
    assert.equal(first, second, 'cached snapshot reference is reused');
  });

  it('concurrent callers for one signature share a single resolver invocation', async () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-opus-4-6-conc';
    let called = 0;
    let release!: () => void;
    const gate = new Promise<void>(r => { release = r; });
    modelStringsInitHooks.resolveBedrockProfiles = async () => { called++; await gate; return ['us.anthropic.claude-opus-4-6-conc']; };
    const a = ensureModelStringsInitialized(bed());
    const b = ensureModelStringsInitialized(bed());
    release();
    const [ra, rb] = await Promise.all([a, b]);
    assert.equal(called, 1, 'a concurrent burst shares one resolution');
    assert.equal(ra, rb);
  });

  it('getBuiltinModelStrings serves the warmed snapshot after the barrier ran', async () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-opus-4-6-warm';
    modelStringsInitHooks.resolveBedrockProfiles = async () => ['us.anthropic.claude-opus-4-6-warm'];
    const warmed = await ensureModelStringsInitialized(bed());
    const read = getBuiltinModelStrings(bed());
    assert.equal(read, warmed, 'getBuiltinModelStrings returns the cached snapshot');
    assert.equal(read.find(r => r.key === 'opus46')!.wireId, 'us.anthropic.claude-opus-4-6-warm');
  });

  it('getBuiltinModelStrings computes interim defaults when not yet initialized', () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-opus-4-6-interim';
    // No barrier call for this signature → inline (synchronous) interim build.
    const rows = getBuiltinModelStrings(bed());
    assert.equal(rows.find(r => r.key === 'opus46')!.wireId, 'us.anthropic.claude-opus-4-6-interim');
  });

  it('listModels awaits the barrier on the Bedrock static path (end-to-end)', async () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    process.env.AMI_BEDROCK_INFERENCE_PROFILES = 'us.anthropic.claude-sonnet-4-6-e2e';
    let called = 0;
    modelStringsInitHooks.resolveBedrockProfiles = async () => { called++; return ['us.anthropic.claude-sonnet-4-6-e2e']; };
    const models = await listModels(bed());
    assert.ok(called >= 1, 'listModels awaited the model-string init barrier for Bedrock');
    // Still returns the provider's static model list.
    assert.ok(models.length > 0);
    // The barrier warmed the snapshot, so a subsequent build is region-correct.
    const sonnet46 = getBuiltinModelStrings(bed()).find(r => r.key === 'sonnet46');
    assert.equal(sonnet46!.wireId, 'us.anthropic.claude-sonnet-4-6-e2e');
  });

  it('listModels does NOT invoke the Bedrock resolver for a non-Bedrock provider', async () => {
    let called = 0;
    modelStringsInitHooks.resolveBedrockProfiles = async () => { called++; return []; };
    // anthropic is a static provider but not Bedrock-class → no barrier await.
    await listModels(cfg({ provider: 'anthropic' }));
    assert.equal(called, 0);
  });
});
