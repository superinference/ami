import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { SENSITIVE_ENV_RE, redactEnvVarsInOutput } from '../src/tools/bash-security';

describe('SENSITIVE_ENV_RE', () => {
  it('matches API_KEY', () => assert.ok(SENSITIVE_ENV_RE.test('API_KEY')));
  it('matches SECRET_TOKEN', () => assert.ok(SENSITIVE_ENV_RE.test('SECRET_TOKEN')));
  it('matches GITHUB_TOKEN', () => assert.ok(SENSITIVE_ENV_RE.test('GITHUB_TOKEN')));
  it('matches AWS_SECRET_ACCESS_KEY', () => assert.ok(SENSITIVE_ENV_RE.test('AWS_SECRET_ACCESS_KEY')));
  it('matches OPENAI_KEY', () => assert.ok(SENSITIVE_ENV_RE.test('OPENAI_KEY')));
  it('matches ANTHROPIC_KEY', () => assert.ok(SENSITIVE_ENV_RE.test('ANTHROPIC_KEY')));
  it('matches DATABASE_URL', () => assert.ok(SENSITIVE_ENV_RE.test('DATABASE_URL')));
  it('matches DB_PASSWORD', () => assert.ok(SENSITIVE_ENV_RE.test('DB_PASSWORD')));
  it('matches PRIVATE_KEY', () => assert.ok(SENSITIVE_ENV_RE.test('PRIVATE_KEY')));
  it('does NOT match PATH', () => assert.ok(!SENSITIVE_ENV_RE.test('PATH')));
  it('does NOT match HOME', () => assert.ok(!SENSITIVE_ENV_RE.test('HOME')));
  it('does NOT match LANG', () => assert.ok(!SENSITIVE_ENV_RE.test('LANG')));
  it('does NOT match SHELL', () => assert.ok(!SENSITIVE_ENV_RE.test('SHELL')));
  it('does NOT match USER', () => assert.ok(!SENSITIVE_ENV_RE.test('USER')));
});

describe('redactEnvVarsInOutput', () => {
  it('redacts API_KEY=value', () => {
    assert.equal(redactEnvVarsInOutput('API_KEY=sk-abc123xyz'), 'API_KEY=[REDACTED]');
  });

  it('redacts export SECRET_TOKEN=value', () => {
    assert.equal(
      redactEnvVarsInOutput('export SECRET_TOKEN=mysecret'),
      'export SECRET_TOKEN=[REDACTED]',
    );
  });

  it('redacts declare -x AWS_SECRET_ACCESS_KEY="value"', () => {
    assert.equal(
      redactEnvVarsInOutput('declare -x AWS_SECRET_ACCESS_KEY="myawskey"'),
      'declare -x AWS_SECRET_ACCESS_KEY="[REDACTED]"',
    );
  });

  it('redacts single-quoted values', () => {
    assert.equal(
      redactEnvVarsInOutput("GITHUB_TOKEN='ghp_xxx123'"),
      "GITHUB_TOKEN='[REDACTED]'",
    );
  });

  it('leaves PATH untouched', () => {
    const input = 'PATH=/usr/bin:/usr/local/bin';
    assert.equal(redactEnvVarsInOutput(input), input);
  });

  it('leaves HOME untouched', () => {
    const input = 'HOME=/home/user';
    assert.equal(redactEnvVarsInOutput(input), input);
  });

  it('handles mixed output correctly', () => {
    const input = [
      'PATH=/usr/bin',
      'API_KEY=secret123',
      'HOME=/home/user',
      'DB_PASSWORD=pass456',
      'SHELL=/bin/bash',
    ].join('\n');
    const expected = [
      'PATH=/usr/bin',
      'API_KEY=[REDACTED]',
      'HOME=/home/user',
      'DB_PASSWORD=[REDACTED]',
      'SHELL=/bin/bash',
    ].join('\n');
    assert.equal(redactEnvVarsInOutput(input), expected);
  });

  it('output without env vars is unchanged', () => {
    const input = 'Hello world\nThis is normal output\nNo env vars here';
    assert.equal(redactEnvVarsInOutput(input), input);
  });

  it('empty output returns empty', () => {
    assert.equal(redactEnvVarsInOutput(''), '');
  });

  it('redacts OPENAI_KEY', () => {
    assert.equal(
      redactEnvVarsInOutput('OPENAI_KEY=sk-proj-abc'),
      'OPENAI_KEY=[REDACTED]',
    );
  });

  it('redacts ANTHROPIC_KEY', () => {
    assert.equal(
      redactEnvVarsInOutput('ANTHROPIC_KEY=sk-ant-xyz'),
      'ANTHROPIC_KEY=[REDACTED]',
    );
  });

  it('handles multiple sensitive vars in multiline output', () => {
    const input = 'API_KEY=a\nSECRET_TOKEN=b\nPRIVATE_KEY=c';
    const result = redactEnvVarsInOutput(input);
    assert.ok(!result.includes('=a'));
    assert.ok(!result.includes('=b'));
    assert.ok(!result.includes('=c'));
    assert.ok(result.includes('[REDACTED]'));
  });
});
