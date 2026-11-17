/**
 * U065 (w70) — model-registry parity additions:
 *  - detectProvider honors the explicit Bedrock opt-in toggle
 *    CLAUDE_CODE_USE_BEDROCK / SUPERINFERENCE_USE_BEDROCK, independent of AWS
 *    credential sniffing. Ref utils/model/providers.ts:L6-L14.
 *  - isLegacyModelRemapDisabled + CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP opt-out
 *    suppresses the deprecated→successor remap (both the advisory suggestion AND
 *    the startup settings auto-migration). Ref
 *    hooks/notifs/useModelMigrationNotifications.tsx:L24-L29.
 *  - suggestFamilyVersionFallback: too-new family id stepped DOWN to the prior
 *    generation (opus-4-6 → opus-4-1, …). Ref services/api/errors.ts:L936-L959.
 *  - getNewerVersionHint + getModelMetadata.upgrade: a pinned OLDER family build
 *    gets a "Newer version available · select <Alias> for <CurrentVersion>" hint.
 *    Ref utils/model/modelOptions.ts:L385-L459.
 *  - MODEL_ENV_TOGGLES: introspection surface for the new env knobs.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-w70.test.ts
 */
import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  detectProvider,
  isLegacyModelRemapDisabled,
  suggestDeprecatedReplacement,
  suggestFamilyVersionFallback,
  getNewerVersionHint,
  getModelMetadata,
  getModelDisplayName,
  validateModel,
  MODEL_ENV_TOGGLES,
} from '../src/model-registry';
import { migrateDeprecatedModelAlias } from '../src/reference-journal';
import type { ProviderConfig } from '../src/types';

// Env keys this suite toggles — snapshot + restore so no test leaks state.
const ENV_KEYS = [
  'CLAUDE_CODE_USE_BEDROCK',
  'SUPERINFERENCE_USE_BEDROCK',
  'CLAUDE_CODE_USE_VERTEX',
  'ANTHROPIC_VERTEX_PROJECT_ID',
  'CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP',
];
afterEach(() => {
  for (const k of ENV_KEYS) delete process.env[k];
});

describe('detectProvider — explicit Bedrock opt-in toggle', () => {
  it('routes to amazon-bedrock from CLAUDE_CODE_USE_BEDROCK regardless of credentials', () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    // An OpenAI-looking key would normally be sniffed as openai; the toggle wins.
    assert.equal(detectProvider({ apiKey: 'sk-openai', baseUrl: '' } as ProviderConfig), 'amazon-bedrock');
  });
  it('routes to amazon-bedrock from the SUPERINFERENCE_USE_BEDROCK alias', () => {
    process.env.SUPERINFERENCE_USE_BEDROCK = 'true';
    assert.equal(detectProvider({ apiKey: '', baseUrl: '' } as ProviderConfig), 'amazon-bedrock');
  });
  it('does NOT route to bedrock without the toggle (credential sniffing stands)', () => {
    assert.equal(detectProvider({ apiKey: 'sk-ant-xyz', baseUrl: '' } as ProviderConfig), 'anthropic');
    assert.equal(detectProvider({ apiKey: 'sk-openai', baseUrl: '' } as ProviderConfig), 'openai');
  });
  it('an explicit config.provider still wins over the toggle', () => {
    process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    assert.equal(detectProvider({ provider: 'anthropic', apiKey: '', baseUrl: '' } as ProviderConfig), 'anthropic');
  });
});

describe('isLegacyModelRemapDisabled — CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP opt-out', () => {
  it('is false when unset', () => {
    assert.equal(isLegacyModelRemapDisabled(), false);
  });
  it('accepts the usual truthy spellings (case/space tolerant)', () => {
    for (const v of ['1', 'true', 'TRUE', 'yes', ' on ']) {
      process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP = v;
      assert.equal(isLegacyModelRemapDisabled(), true, `expected truthy for ${JSON.stringify(v)}`);
    }
  });
  it('is false for a non-truthy value', () => {
    process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP = '0';
    assert.equal(isLegacyModelRemapDisabled(), false);
    process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP = 'off';
    assert.equal(isLegacyModelRemapDisabled(), false);
  });
  it('suppresses suggestDeprecatedReplacement when opted out', () => {
    // Without the opt-out, a legacy id maps to its successor.
    assert.equal(suggestDeprecatedReplacement('claude-3-5-sonnet', []), 'claude-sonnet-4-5');
    process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP = '1';
    assert.equal(suggestDeprecatedReplacement('claude-3-5-sonnet', []), undefined);
  });
});

describe('migrateDeprecatedModelAlias — honors the remap opt-out (end-to-end)', () => {
  const tmpdirs: string[] = [];
  function cfgPath(obj: Record<string, unknown>): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u065w70-mig-'));
    tmpdirs.push(dir);
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify(obj));
    return p;
  }
  after(() => { for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });

  it('does NOT rewrite a deprecated pinned model when the opt-out is set, but DOES when unset', async () => {
    // Opted out → the user's explicit (legacy) choice stands, no rewrite.
    process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP = '1';
    const pOut = cfgPath({ model: 'claude-3-5-sonnet' });
    assert.equal(await migrateDeprecatedModelAlias(pOut), null);
    assert.equal(JSON.parse(fs.readFileSync(pOut, 'utf8')).model, 'claude-3-5-sonnet');

    // Opt-out cleared → the migration rewrites to the successor.
    delete process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP;
    const pIn = cfgPath({ model: 'claude-3-5-sonnet' });
    const out = await migrateDeprecatedModelAlias(pIn);
    assert.ok(out, 'migration should run when not opted out');
    assert.equal(JSON.parse(fs.readFileSync(pIn, 'utf8')).model, 'claude-sonnet-4-5');
  });
});

describe('suggestFamilyVersionFallback — step a too-new id DOWN to the prior generation', () => {
  it('maps a newer family version to the prior one', () => {
    assert.equal(suggestFamilyVersionFallback('claude-opus-4-6'), 'claude-opus-4-1');
    assert.equal(suggestFamilyVersionFallback('claude-sonnet-4-6'), 'claude-sonnet-4-5');
    assert.equal(suggestFamilyVersionFallback('claude-sonnet-4-5'), 'claude-sonnet-4');
    assert.equal(suggestFamilyVersionFallback('claude-haiku-4-5'), 'claude-3-5-haiku');
  });
  it('prefers a prior version the provider actually lists', () => {
    const listed = ['openrouter/anthropic/claude-opus-4-1-20250805'];
    assert.equal(suggestFamilyVersionFallback('claude-opus-4-6', listed), listed[0]);
  });
  it('returns undefined for an unrecognized id', () => {
    assert.equal(suggestFamilyVersionFallback('gpt-4o'), undefined);
    assert.equal(suggestFamilyVersionFallback('claude-sonnet-4'), undefined);
  });
});

describe('getNewerVersionHint / getModelMetadata.upgrade', () => {
  it('surfaces an upgrade hint for a pinned OLDER family build', () => {
    const hint = getNewerVersionHint('claude-opus-4-1');
    assert.ok(hint, 'opus 4.1 should have a newer-version hint');
    assert.match(hint!, /^Newer version available · select Opus for /);
    assert.ok(hint!.includes(getModelDisplayName('claude-opus-4-6')), 'names the current version');
  });
  it('maps the family alias label correctly per family', () => {
    assert.match(getNewerVersionHint('claude-sonnet-4-5')!, /select Sonnet for /);
    assert.match(getNewerVersionHint('claude-3-5-haiku')!, /select Haiku for /);
  });
  it('returns undefined when the pinned model IS the current version', () => {
    assert.equal(getNewerVersionHint('claude-opus-4-6'), undefined);
    assert.equal(getNewerVersionHint('claude-sonnet-4-6'), undefined);
    assert.equal(getNewerVersionHint('claude-haiku-4-5'), undefined);
  });
  it('returns undefined for unknown / non-Claude ids', () => {
    assert.equal(getNewerVersionHint('gpt-4o'), undefined);
    assert.equal(getNewerVersionHint('some-custom-model'), undefined);
    assert.equal(getNewerVersionHint(''), undefined);
  });
  it('getModelMetadata carries the upgrade field alongside displayName/description', () => {
    const meta = getModelMetadata('claude-opus-4-1');
    assert.equal(meta.displayName, 'Opus 4.1');
    assert.ok(meta.description);
    assert.ok(meta.upgrade && meta.upgrade.includes('Newer version available'));
    // Current version → no upgrade field.
    assert.equal(getModelMetadata('claude-opus-4-6').upgrade, undefined);
  });
});

describe('MODEL_ENV_TOGGLES — introspection surface', () => {
  it('declares the Bedrock + remap env knobs with affects + description', () => {
    const envs = MODEL_ENV_TOGGLES.map(t => t.env);
    assert.ok(envs.includes('CLAUDE_CODE_USE_BEDROCK'));
    assert.ok(envs.includes('SUPERINFERENCE_USE_BEDROCK'));
    assert.ok(envs.includes('CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP'));
    for (const t of MODEL_ENV_TOGGLES) {
      assert.ok(t.affects && t.description, `${t.env} needs affects + description`);
    }
  });
});

describe('validateModel — family-version fallback suggestion (integration via mock /models)', () => {
  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.setHeader('content-type', 'application/json');
      // The endpoint carries the PRIOR generation but NOT the requested 4.6.
      res.end(JSON.stringify({ data: [{ id: 'claude-opus-4-1-20250805' }, { id: 'gpt-4o' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('suggests the prior family version when the requested newer model is absent', async () => {
    const res = await validateModel(
      { provider: 'openai', apiKey: 'k', baseUrl: `http://127.0.0.1:${port}/v1`, model: 'm' } as ProviderConfig,
      'claude-opus-4-6',
    );
    assert.equal(res.valid, false);
    assert.ok(res.suggestion, 'should produce a fallback suggestion');
    assert.ok(
      res.suggestion!.includes('claude-opus-4-1'),
      `expected an opus-4-1 fallback, got ${res.suggestion}`,
    );
  });
});
