import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  AWS_BEARER_TOKEN_BEDROCK_ENV,
  ANTHROPIC_BEDROCK_BASE_URL_ENV,
  BEDROCK_INFERENCE_PROFILES_ENV,
  BEDROCK_REGION_PREFIX_ENV,
  BEDROCK_AUTH_AFFORDANCES,
  bedrockProvidersKeyHint,
  type BedrockAuthAffordance,
} from '../src/bedrock';
// The bearer-token env name must stay a single canonical constant re-exported by
// provider.ts (its existing importers), not a second literal.
import { AWS_BEARER_TOKEN_BEDROCK_ENV as REEXPORTED_BEARER } from '../src/provider';

describe('Bedrock auth env-var constants (U665)', () => {
  it('declares the canonical bearer-token and base-URL env names', () => {
    assert.equal(AWS_BEARER_TOKEN_BEDROCK_ENV, 'AWS_BEARER_TOKEN_BEDROCK');
    assert.equal(ANTHROPIC_BEDROCK_BASE_URL_ENV, 'ANTHROPIC_BEDROCK_BASE_URL');
  });

  it('provider.ts re-exports the SAME bearer-token constant (one canonical home)', () => {
    assert.equal(REEXPORTED_BEARER, AWS_BEARER_TOKEN_BEDROCK_ENV);
  });
});

describe('BEDROCK_AUTH_AFFORDANCES (U665)', () => {
  const byEnv = (env: string): BedrockAuthAffordance | undefined =>
    BEDROCK_AUTH_AFFORDANCES.find((a) => a.env === env);

  it('surfaces the bearer-token auth affordance', () => {
    const bearer = byEnv(AWS_BEARER_TOKEN_BEDROCK_ENV);
    assert.ok(bearer, 'bearer-token affordance present');
    assert.equal(bearer!.affects, 'auth');
    assert.match(bearer!.description, /bearer-token/i);
  });

  it('surfaces the endpoint-override affordance', () => {
    const endpoint = byEnv(ANTHROPIC_BEDROCK_BASE_URL_ENV);
    assert.ok(endpoint, 'endpoint affordance present');
    assert.equal(endpoint!.affects, 'endpoint');
  });

  it('surfaces the region, inference-profile, and cross-region-prefix knobs', () => {
    assert.equal(byEnv('AWS_REGION')?.affects, 'region');
    assert.equal(byEnv(BEDROCK_INFERENCE_PROFILES_ENV)?.affects, 'model');
    assert.equal(byEnv(BEDROCK_REGION_PREFIX_ENV)?.affects, 'region');
  });

  it('every affordance has a non-empty env name, kind, and description', () => {
    for (const a of BEDROCK_AUTH_AFFORDANCES) {
      assert.ok(a.env.length > 0, `env set for ${JSON.stringify(a)}`);
      assert.ok(['auth', 'endpoint', 'region', 'model'].includes(a.affects));
      assert.ok(a.description.length > 0, `description set for ${a.env}`);
    }
  });

  it('has no duplicate env entries', () => {
    const envs = BEDROCK_AUTH_AFFORDANCES.map((a) => a.env);
    assert.equal(new Set(envs).size, envs.length);
  });
});

describe('bedrockProvidersKeyHint (U665)', () => {
  it('composes the hint from the affordances (SSO default + bearer + endpoint)', () => {
    const hint = bedrockProvidersKeyHint();
    assert.match(hint, /AWS_SESSION_TOKEN/);
    assert.match(hint, new RegExp(AWS_BEARER_TOKEN_BEDROCK_ENV));
    assert.match(hint, new RegExp(ANTHROPIC_BEDROCK_BASE_URL_ENV));
  });
});
