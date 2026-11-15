import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { inspect } from 'node:util';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  REDACT_PLACEHOLDER,
  REDACT_MIN_LENGTH,
  maskSecretValue,
  RedactedSecret,
  redactPaths,
} from '../src/credential-guard';

// ===========================================================================
// FUNCTIONAL: prefixed UPPER_SNAKE env-var secret coverage.
//
// The lowercase SECRET_ASSIGN_RE cannot match `AWS_SECRET_ACCESS_KEY=…` etc.
// because its key lookbehind rejects a leading `PREFIX_`, so these secrets
// leaked verbatim before. The env rule redacts them UNCONDITIONALLY (even a
// short / low-entropy value cannot slip through under an explicitly secret name).
// ===========================================================================
describe('prefixed env-var secret coverage (unconditional)', () => {
  // The canonical AWS secret access key (NOT the AKIA id) has no distinctive
  // prefix of its own and only leaks via its env-var NAME.
  const AWS_SECRET = 'wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY';

  it('detects + redacts AWS_SECRET_ACCESS_KEY the AKIA rule alone would miss', () => {
    const text = `AWS_SECRET_ACCESS_KEY=${AWS_SECRET}`;
    assert.equal(containsCredential(text), true);
    const red = redactCredentials(text);
    assert.ok(!red.includes(AWS_SECRET));
    assert.equal(red, 'AWS_SECRET_ACCESS_KEY=[REDACTED]');
  });

  it('redacts low-entropy/short values UNCONDITIONALLY under a prefixed secret name', () => {
    const cases: Array<[string, string]> = [
      ['GH_TOKEN', 'xyz_placeholderlowentropyvalue'],
      ['DB_PASSWORD', 'short1'],
      ['MY_SERVICE_SECRET', 'tiny'],
      ['FOO_SECRET_KEY', 'aaaa'],
      ['APP_PRIVATE_KEY', 'bbbb'],
      ['SVC_CREDENTIALS', 'cccc'],
      ['OKTA_API_KEY', 'dddd'],
      ['S3_ACCESS_KEY', 'eeee'],
      ['SSH_PASSPHRASE', 'ffff'],
      ['BASIC_AUTH', 'gggg'],
    ];
    for (const [name, val] of cases) {
      const text = `${name}=${val}`;
      assert.equal(containsCredential(text), true, `containsCredential failed for ${name}`);
      assert.equal(redactCredentials(text), `${name}=[REDACTED]`, `redact failed for ${name}`);
    }
  });

  it('handles the `export NAME="value"` quoted form, keeping surrounding context', () => {
    const red = redactCredentials('export GOOGLE_API_KEY="abcDEFghiJKL012" # comment');
    assert.equal(red, 'export GOOGLE_API_KEY="[REDACTED]" # comment');
  });

  it('findCredentials reports an env_secret match over the VALUE span only', () => {
    const text = `AWS_SECRET_ACCESS_KEY=${AWS_SECRET}`;
    const found = findCredentials(text);
    const env = found.filter(f => f.type === 'env_secret');
    assert.equal(env.length, 1);
    assert.equal(env[0].label, 'Env Secret');
    assert.equal(text.slice(env[0].start, env[0].start + env[0].length), AWS_SECRET);
  });

  it('does NOT double-count: a prefixed name yields ONE match, not env+lowercase', () => {
    // `client_secret` (lowercase) stays a single lowercase match.
    assert.equal(findCredentials('client_secret=xyz987').length, 1);
    // A bare uppercase `TOKEN=` (no prefix) is handled ONLY by the lowercase
    // unconditional rule — the env rule requires a prefix, so no duplicate.
    const bare = findCredentials('TOKEN=aaaaaaaaaaaaaaaaaaaa');
    assert.deepEqual(bare.map(f => f.type), ['generic_secret']);
  });

  it('does NOT flag ordinary non-secret UPPER_SNAKE env vars (no false positives)', () => {
    for (const text of [
      'CONTENT_TYPE=application/json',
      'LOG_LEVEL=debug',
      'NODE_ENV=production',
      'MAX_TOKENS=100',        // TOKENS, not TOKEN — must not match
      'ACCESS_LEVEL=admin',    // ACCESS_LEVEL, not ACCESS_KEY
      'HTTP_PORT=8080',
    ]) {
      assert.equal(containsCredential(text), false, `false positive on ${text}`);
    }
  });
});

// ===========================================================================
// FUNCTIONAL: Authorization header (non-Bearer) value redaction.
// Entropy-gated to avoid redacting `authorization` used as a code flag.
// ===========================================================================
describe('Authorization header (non-Bearer) redaction', () => {
  it('redacts a high-entropy Authorization value lacking the Bearer prefix', () => {
    const text = 'Authorization: 2b9d8f0e1a2b3c4d5e6f7a8b9c0d1e2f';
    assert.equal(containsCredential(text), true);
    const red = redactCredentials(text);
    assert.ok(!red.includes('2b9d8f0e1a2b3c4d5e6f7a8b9c0d1e2f'));
    assert.equal(red, 'Authorization: [REDACTED]');
  });

  it('does NOT flag a low-entropy authorization boolean/flag assignment', () => {
    assert.equal(containsCredential('authorization: true'), false);
    assert.equal(redactCredentials('authorization: false'), 'authorization: false');
  });
});

// ===========================================================================
// FUNCTIONAL: partial-reveal (fingerprint) redaction mode.
// ===========================================================================
describe('maskSecretValue / partial redaction mode', () => {
  it('exports REDACT_MIN_LENGTH=16', () => {
    assert.equal(REDACT_MIN_LENGTH, 16);
  });

  it('defaults to full [REDACTED] (no options, or partial off)', () => {
    assert.equal(maskSecretValue('abcdefghijklmnop1234'), REDACT_PLACEHOLDER);
    assert.equal(maskSecretValue('abcdefghijklmnop1234', { partial: false }), REDACT_PLACEHOLDER);
  });

  it('partial reveals first 8 + last 4 for values at/above the threshold', () => {
    assert.equal(maskSecretValue('abcdefghijklmnop1234', { partial: true }), `abcdefgh${REDACT_PLACEHOLDER}1234`);
    // exactly minLength (16) is revealed (>= boundary)
    assert.equal(maskSecretValue('abcdefghijkl3456', { partial: true }), `abcdefgh${REDACT_PLACEHOLDER}3456`);
  });

  it('partial fully masks values below the threshold', () => {
    assert.equal(maskSecretValue('tooshort', { partial: true }), REDACT_PLACEHOLDER);
    // one char below the custom threshold
    assert.equal(maskSecretValue('abcdefghij', { partial: true, minLength: 11 }), REDACT_PLACEHOLDER);
  });

  it('honours a custom minLength knob', () => {
    assert.equal(maskSecretValue('abcdefghij', { partial: true, minLength: 10 }), `abcdefgh${REDACT_PLACEHOLDER}ghij`);
  });

  it('redactCredentials partial mode fingerprints pattern, env, and assign matches', () => {
    const key = 'sk-' + 'a'.repeat(48);
    const patt = redactCredentials(`key=${key}`, { partial: true });
    assert.ok(!patt.includes(key));
    assert.ok(patt.includes(REDACT_PLACEHOLDER));
    assert.ok(patt.startsWith('key=sk-aaaaa'));   // first 8 of the key kept

    const env = redactCredentials('AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY', { partial: true });
    assert.ok(env.startsWith('AWS_SECRET_ACCESS_KEY=wJalrXUt'));
    assert.ok(env.includes(REDACT_PLACEHOLDER));
    assert.ok(!env.includes('NFEMIK7MDENGbPxRfiCYEXAMPLE'));

    const assign = redactCredentials('token="aZ3kP9xQ2mL7vB4nR8tW"', { partial: true });
    assert.ok(assign.includes(REDACT_PLACEHOLDER));
    assert.ok(!assign.includes('xQ2mL7vB4n'));
  });
});

// ===========================================================================
// FUNCTIONAL: redact-by-construction secret wrapper.
// ===========================================================================
describe('RedactedSecret', () => {
  it('redacts under every implicit stringification path but reveals explicitly', () => {
    const s = new RedactedSecret('super-secret-123');
    assert.equal(String(s), REDACT_PLACEHOLDER);
    assert.equal(`${s}`, REDACT_PLACEHOLDER);
    assert.equal(s.toString(), REDACT_PLACEHOLDER);
    assert.equal(s.toJSON(), REDACT_PLACEHOLDER);
    assert.equal(JSON.stringify({ apiKey: s }), `{"apiKey":"${REDACT_PLACEHOLDER}"}`);
    assert.equal(inspect(s), REDACT_PLACEHOLDER);
    // The ONLY way to get the raw value.
    assert.equal(s.reveal(), 'super-secret-123');
  });

  it('isEmpty reports emptiness without revealing the value', () => {
    assert.equal(new RedactedSecret('x').isEmpty(), false);
    assert.equal(new RedactedSecret('').isEmpty(), true);
  });
});

// ===========================================================================
// FUNCTIONAL: local-path / PII normalization.
// ===========================================================================
describe('redactPaths', () => {
  it('normalizes cwd and home to placeholders', () => {
    const out = redactPaths('see /home/alice/proj/a.ts and /home/alice/b.ts', {
      cwd: '/home/alice/proj',
      home: '/home/alice',
    });
    assert.equal(out, 'see [CWD]/a.ts and [HOME]/b.ts');
  });

  it('substitutes cwd before home so a nested cwd collapses to [CWD]', () => {
    // cwd is UNDER home; cwd must win for paths inside it.
    const out = redactPaths('/home/alice/proj/x /home/alice/y', {
      cwd: '/home/alice/proj',
      home: '/home/alice',
    });
    assert.equal(out, '[CWD]/x [HOME]/y');
  });

  it('returns empty input unchanged', () => {
    assert.equal(redactPaths(''), '');
  });

  it('never uses root "/" as a needle (would rewrite every separator)', () => {
    const out = redactPaths('/a/b/c', { cwd: '/', home: '/' });
    assert.equal(out, '/a/b/c');
  });

  it('falls back to process cwd/home when no options are given', () => {
    const marker = `${process.cwd()}/deep/file.ts`;
    const out = redactPaths(marker);
    assert.ok(out.startsWith('[CWD]/deep/file.ts'));
    assert.ok(!out.includes(process.cwd()));
  });
});
