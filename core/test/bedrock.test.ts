import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getBedrockInferenceProfiles,
  resolveBedrockModelId,
  extractModelIdFromArn,
  BEDROCK_INFERENCE_PROFILES_ENV,
  BEDROCK_REGION_PREFIX_ENV,
  BEDROCK_REGION_PREFIXES,
  extractBedrockRegionPrefix,
  applyBedrockRegionPrefix,
  getBedrockRegionPrefix,
} from '../src/bedrock';

describe('getBedrockInferenceProfiles', () => {
  it('returns [] when the env var is unset', () => {
    assert.deepEqual(getBedrockInferenceProfiles({} as NodeJS.ProcessEnv), []);
  });

  it('comma-splits, trims, and drops empties', () => {
    const env = {
      [BEDROCK_INFERENCE_PROFILES_ENV]: ' eu.anthropic.claude-opus-4-6-v1:0 , , us.anthropic.claude-sonnet-4-5-v1:0 ,',
    } as unknown as NodeJS.ProcessEnv;
    assert.deepEqual(getBedrockInferenceProfiles(env), [
      'eu.anthropic.claude-opus-4-6-v1:0',
      'us.anthropic.claude-sonnet-4-5-v1:0',
    ]);
  });
});

describe('resolveBedrockModelId', () => {
  const profiles = [
    'eu.anthropic.claude-opus-4-6-v1:0',
    'us.anthropic.claude-sonnet-4-5-v1:0',
  ];

  it('maps a canonical id to the region-scoped profile that contains it', () => {
    assert.equal(resolveBedrockModelId('claude-opus-4-6', profiles), 'eu.anthropic.claude-opus-4-6-v1:0');
    assert.equal(resolveBedrockModelId('claude-sonnet-4-5', profiles), 'us.anthropic.claude-sonnet-4-5-v1:0');
  });

  it('is case-insensitive', () => {
    assert.equal(resolveBedrockModelId('CLAUDE-OPUS-4-6', profiles), 'eu.anthropic.claude-opus-4-6-v1:0');
  });

  it('falls back to the canonical id when no profile matches', () => {
    assert.equal(resolveBedrockModelId('claude-haiku-4-5', profiles), 'claude-haiku-4-5');
  });

  it('falls back when there are no profiles', () => {
    assert.equal(resolveBedrockModelId('claude-opus-4-6', []), 'claude-opus-4-6');
  });

  it('returns an empty canonical id unchanged', () => {
    assert.equal(resolveBedrockModelId('', profiles), '');
  });

  it('normalizes a full Bedrock ARN before matching a profile', () => {
    assert.equal(
      resolveBedrockModelId(
        'arn:aws:bedrock:eu-west-1:123456789012:inference-profile/eu.anthropic.claude-opus-4-6-v1:0',
        profiles,
      ),
      'eu.anthropic.claude-opus-4-6-v1:0',
    );
  });

  it('falls back to the ARN-extracted id (not the raw ARN) when no profile matches', () => {
    assert.equal(
      resolveBedrockModelId(
        'arn:aws:bedrock:us-east-1:123456789012:foundation-model/anthropic.claude-haiku-4-5-20251001-v1:0',
        profiles,
      ),
      'anthropic.claude-haiku-4-5-20251001-v1:0',
    );
  });
});

describe('extractModelIdFromArn', () => {
  it('strips an inference-profile ARN to its trailing id', () => {
    assert.equal(
      extractModelIdFromArn(
        'arn:aws:bedrock:eu-west-1:123456789012:inference-profile/eu.anthropic.claude-opus-4-6-v1:0',
      ),
      'eu.anthropic.claude-opus-4-6-v1:0',
    );
  });

  it('strips an application-inference-profile ARN to its trailing id', () => {
    assert.equal(
      extractModelIdFromArn(
        'arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/my-profile-abc123',
      ),
      'my-profile-abc123',
    );
  });

  it('strips a foundation-model ARN to its trailing id', () => {
    assert.equal(
      extractModelIdFromArn(
        'arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-sonnet-4-5-v1:0',
      ),
      'anthropic.claude-sonnet-4-5-v1:0',
    );
  });

  it('returns a non-ARN id unchanged', () => {
    assert.equal(extractModelIdFromArn('claude-opus-4-6'), 'claude-opus-4-6');
    assert.equal(extractModelIdFromArn('anthropic.claude-sonnet-4-5-v1:0'), 'anthropic.claude-sonnet-4-5-v1:0');
    assert.equal(extractModelIdFromArn(''), '');
  });

  it('returns an ARN without a slash unchanged', () => {
    assert.equal(extractModelIdFromArn('arn:aws:bedrock:us-east-1:123456789012'), 'arn:aws:bedrock:us-east-1:123456789012');
  });
});

describe('extractBedrockRegionPrefix', () => {
  it('reads the leading region segment of a region-scoped profile id', () => {
    assert.equal(extractBedrockRegionPrefix('us.anthropic.claude-opus-4-6-v1:0'), 'us');
    assert.equal(extractBedrockRegionPrefix('eu.anthropic.claude-sonnet-4-5-v1:0'), 'eu');
    assert.equal(extractBedrockRegionPrefix('apac.anthropic.claude-haiku-4-5-v1:0'), 'apac');
    assert.equal(extractBedrockRegionPrefix('global.anthropic.claude-opus-4-6-v1:0'), 'global');
  });

  it('is case-insensitive', () => {
    assert.equal(extractBedrockRegionPrefix('US.anthropic.claude-opus-4-6-v1:0'), 'us');
  });

  it('returns undefined for a provider-namespaced id with no region prefix', () => {
    assert.equal(extractBedrockRegionPrefix('anthropic.claude-sonnet-4-5-v1:0'), undefined);
  });

  it('returns undefined for a bare id with no dot', () => {
    assert.equal(extractBedrockRegionPrefix('claude-opus-4-6'), undefined);
  });

  it('returns undefined when the leading segment is not a known region', () => {
    assert.equal(extractBedrockRegionPrefix('xx.anthropic.claude-opus-4-6-v1:0'), undefined);
  });
});

describe('applyBedrockRegionPrefix', () => {
  it('prepends the region when the id has no existing region prefix', () => {
    assert.equal(
      applyBedrockRegionPrefix('anthropic.claude-sonnet-4-5-v1:0', 'eu'),
      'eu.anthropic.claude-sonnet-4-5-v1:0',
    );
  });

  it('replaces an existing region prefix', () => {
    assert.equal(
      applyBedrockRegionPrefix('us.anthropic.claude-opus-4-6-v1:0', 'eu'),
      'eu.anthropic.claude-opus-4-6-v1:0',
    );
  });

  it('normalizes a full ARN before applying the prefix', () => {
    assert.equal(
      applyBedrockRegionPrefix(
        'arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.anthropic.claude-opus-4-6-v1:0',
        'apac',
      ),
      'apac.anthropic.claude-opus-4-6-v1:0',
    );
  });

  it('covers every documented region prefix', () => {
    for (const region of BEDROCK_REGION_PREFIXES) {
      assert.equal(
        applyBedrockRegionPrefix('anthropic.claude-opus-4-6-v1:0', region),
        `${region}.anthropic.claude-opus-4-6-v1:0`,
      );
    }
  });
});

describe('getBedrockRegionPrefix', () => {
  it('returns undefined when unset', () => {
    assert.equal(getBedrockRegionPrefix({} as NodeJS.ProcessEnv), undefined);
  });

  it('reads and normalizes a recognized region', () => {
    assert.equal(
      getBedrockRegionPrefix({ [BEDROCK_REGION_PREFIX_ENV]: ' EU ' } as unknown as NodeJS.ProcessEnv),
      'eu',
    );
  });

  it('ignores an unrecognized region value', () => {
    assert.equal(
      getBedrockRegionPrefix({ [BEDROCK_REGION_PREFIX_ENV]: 'mars' } as unknown as NodeJS.ProcessEnv),
      undefined,
    );
  });
});

describe('resolveBedrockModelId — cross-region prefix fallback', () => {
  const env = (region?: string): NodeJS.ProcessEnv =>
    (region ? { [BEDROCK_REGION_PREFIX_ENV]: region } : {}) as unknown as NodeJS.ProcessEnv;

  it('applies the configured region to a namespaced id when no profile matches', () => {
    assert.equal(
      resolveBedrockModelId('anthropic.claude-haiku-4-5-v1:0', [], env('eu')),
      'eu.anthropic.claude-haiku-4-5-v1:0',
    );
  });

  it('replaces an existing region prefix on the id with the configured one', () => {
    assert.equal(
      resolveBedrockModelId('us.anthropic.claude-haiku-4-5-v1:0', [], env('apac')),
      'apac.anthropic.claude-haiku-4-5-v1:0',
    );
  });

  it('leaves a bare canonical id (no namespace) unchanged even with a region set', () => {
    assert.equal(resolveBedrockModelId('claude-haiku-4-5', [], env('eu')), 'claude-haiku-4-5');
  });

  it('does not apply a region when none is configured', () => {
    assert.equal(
      resolveBedrockModelId('anthropic.claude-haiku-4-5-v1:0', [], env()),
      'anthropic.claude-haiku-4-5-v1:0',
    );
  });

  it('prefers an explicit matching profile over the region fallback', () => {
    assert.equal(
      resolveBedrockModelId(
        'claude-opus-4-6',
        ['eu.anthropic.claude-opus-4-6-v1:0'],
        env('us'),
      ),
      'eu.anthropic.claude-opus-4-6-v1:0',
    );
  });
});
