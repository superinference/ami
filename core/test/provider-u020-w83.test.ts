import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  isValidAwsStsOutput,
  readStsEnvCredentials,
  assertValidStsEnvCredentials,
  AwsStsCredentialError,
} from '../src/utils/aws';
import { resolveModel, AWS_BEARER_TOKEN_BEDROCK_ENV } from '../src/provider';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w83 — AWS short-term (STS) credential boundary validation for the
// amazon-bedrock path.
//
// Reference: references/claude/claude-code-main/src/utils/aws.ts:L24-L47
//   `isValidAwsStsOutput` — a defensive typeguard that rejects a non-object,
//   a missing/non-object `Credentials`, and any AccessKeyId/SecretAccessKey/
//   SessionToken that is not a present, NON-EMPTY string, before the secret
//   material is trusted.
//
// AMI gap (ami-missing): AMI had no structural validation of externally-sourced
// AWS credential input on the Bedrock path — a malformed/partially-populated
// credential blob exported into the env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
// / AWS_SESSION_TOKEN) was fed straight through, surfacing later as an opaque 403.
//
// RED on revert: removing the `assertValidStsEnvCredentials(process.env)` call in
// the amazon-bedrock branch of resolveModel() makes the "throws on malformed STS
// env creds" E2E test below pass through (no throw) instead of failing fast.
// ---------------------------------------------------------------------------

const AWS_KEYS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN'] as const;

function snapshotAwsEnv(): Record<string, string | undefined> {
  const snap: Record<string, string | undefined> = {};
  for (const k of AWS_KEYS) snap[k] = process.env[k];
  snap[AWS_BEARER_TOKEN_BEDROCK_ENV] = process.env[AWS_BEARER_TOKEN_BEDROCK_ENV];
  return snap;
}
function restoreAwsEnv(snap: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(snap)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

afterEach(() => {
  for (const k of AWS_KEYS) delete process.env[k];
  delete process.env[AWS_BEARER_TOKEN_BEDROCK_ENV];
});

describe('isValidAwsStsOutput', () => {
  const valid = {
    Credentials: { AccessKeyId: 'AKIA123', SecretAccessKey: 'secret123', SessionToken: 'tok123' },
  };

  it('accepts a well-formed STS output', () => {
    assert.equal(isValidAwsStsOutput(valid), true);
    // Expiration is optional and not validated.
    assert.equal(isValidAwsStsOutput({ Credentials: { ...valid.Credentials, Expiration: '2026-01-01T00:00:00Z' } }), true);
  });

  it('rejects non-objects', () => {
    assert.equal(isValidAwsStsOutput(null), false);
    assert.equal(isValidAwsStsOutput(undefined), false);
    assert.equal(isValidAwsStsOutput('x'), false);
    assert.equal(isValidAwsStsOutput(42), false);
  });

  it('rejects a missing or non-object Credentials', () => {
    assert.equal(isValidAwsStsOutput({}), false);
    assert.equal(isValidAwsStsOutput({ Credentials: null }), false);
    assert.equal(isValidAwsStsOutput({ Credentials: 'nope' }), false);
  });

  it('rejects empty-string credential fields (the explicit .length>0 guard)', () => {
    assert.equal(isValidAwsStsOutput({ Credentials: { AccessKeyId: '', SecretAccessKey: 's', SessionToken: 't' } }), false);
    assert.equal(isValidAwsStsOutput({ Credentials: { AccessKeyId: 'a', SecretAccessKey: '', SessionToken: 't' } }), false);
    assert.equal(isValidAwsStsOutput({ Credentials: { AccessKeyId: 'a', SecretAccessKey: 's', SessionToken: '' } }), false);
  });

  it('rejects non-string credential fields', () => {
    assert.equal(isValidAwsStsOutput({ Credentials: { AccessKeyId: 1, SecretAccessKey: 's', SessionToken: 't' } }), false);
    assert.equal(isValidAwsStsOutput({ Credentials: { AccessKeyId: 'a', SecretAccessKey: 's' } }), false);
  });
});

describe('readStsEnvCredentials', () => {
  it('returns null when no session token is present (not short-term creds)', () => {
    process.env.AWS_ACCESS_KEY_ID = 'AKIAlongterm';
    process.env.AWS_SECRET_ACCESS_KEY = 'secret';
    assert.equal(readStsEnvCredentials(process.env), null);
  });

  it('builds a trimmed STS candidate when a session token is present', () => {
    const cand = readStsEnvCredentials({
      AWS_ACCESS_KEY_ID: '  AKIA1  ',
      AWS_SECRET_ACCESS_KEY: ' sec ',
      AWS_SESSION_TOKEN: ' tok ',
    } as NodeJS.ProcessEnv);
    assert.deepEqual(cand, { Credentials: { AccessKeyId: 'AKIA1', SecretAccessKey: 'sec', SessionToken: 'tok' } });
  });

  it('treats a whitespace-only session token as absent', () => {
    assert.equal(readStsEnvCredentials({ AWS_SESSION_TOKEN: '   ' } as NodeJS.ProcessEnv), null);
  });
});

describe('assertValidStsEnvCredentials', () => {
  it('no-ops when no session token is set', () => {
    assert.doesNotThrow(() => assertValidStsEnvCredentials({ AWS_ACCESS_KEY_ID: 'a' } as NodeJS.ProcessEnv));
  });

  it('no-ops for a valid short-term credential triple', () => {
    assert.doesNotThrow(() => assertValidStsEnvCredentials({
      AWS_ACCESS_KEY_ID: 'AKIA1', AWS_SECRET_ACCESS_KEY: 'sec', AWS_SESSION_TOKEN: 'tok',
    } as NodeJS.ProcessEnv));
  });

  it('throws naming AWS_ACCESS_KEY_ID when it is empty but a session token is set', () => {
    assert.throws(
      () => assertValidStsEnvCredentials({ AWS_SECRET_ACCESS_KEY: 'sec', AWS_SESSION_TOKEN: 'tok' } as NodeJS.ProcessEnv),
      (e: unknown) => e instanceof AwsStsCredentialError && /AWS_ACCESS_KEY_ID/.test((e as Error).message) && / is empty/.test((e as Error).message),
    );
  });

  it('throws naming both fields when both are empty', () => {
    assert.throws(
      () => assertValidStsEnvCredentials({ AWS_SESSION_TOKEN: 'tok' } as NodeJS.ProcessEnv),
      (e: unknown) =>
        e instanceof AwsStsCredentialError &&
        /AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY/.test((e as Error).message) &&
        / are empty/.test((e as Error).message),
    );
  });
});

describe('E2E: resolveModel amazon-bedrock STS credential boundary', () => {
  const bedrockConfig: ProviderConfig = {
    provider: 'amazon-bedrock',
    model: 'anthropic.claude-sonnet-4-6',
    apiKey: '',
    baseUrl: 'https://gw.example/v1',
  };

  it('throws AwsStsCredentialError when short-term env creds are malformed', () => {
    const snap = snapshotAwsEnv();
    try {
      process.env.AWS_SESSION_TOKEN = 'tok';
      // AWS_ACCESS_KEY_ID intentionally absent → malformed STS triple.
      process.env.AWS_SECRET_ACCESS_KEY = 'sec';
      assert.throws(() => resolveModel(bedrockConfig), AwsStsCredentialError);
    } finally {
      restoreAwsEnv(snap);
    }
  });

  it('constructs the model when short-term env creds are well-formed', () => {
    const snap = snapshotAwsEnv();
    try {
      process.env.AWS_ACCESS_KEY_ID = 'AKIA1';
      process.env.AWS_SECRET_ACCESS_KEY = 'sec';
      process.env.AWS_SESSION_TOKEN = 'tok';
      const model = resolveModel(bedrockConfig);
      assert.ok(String(model.provider).includes('openai'), `provider was ${model.provider}`);
      assert.equal(model.modelId, 'anthropic.claude-sonnet-4-6');
    } finally {
      restoreAwsEnv(snap);
    }
  });

  it('skips STS validation under bearer-token auth even with a malformed triple', () => {
    const snap = snapshotAwsEnv();
    try {
      process.env[AWS_BEARER_TOKEN_BEDROCK_ENV] = 'bearer-123';
      process.env.AWS_SESSION_TOKEN = 'tok'; // malformed (no access key) — but bearer wins
      assert.doesNotThrow(() => resolveModel(bedrockConfig));
    } finally {
      restoreAwsEnv(snap);
    }
  });
});
