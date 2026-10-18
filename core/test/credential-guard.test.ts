import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  REDACT_PLACEHOLDER,
  CredentialGuard,
} from '../src/credential-guard';

// ---------------------------------------------------------------------------
// Test key fixtures (fake but pattern-matching)
// ---------------------------------------------------------------------------
const OPENAI_KEY = 'sk-' + 'a'.repeat(48);
const ANTHROPIC_KEY = 'sk-ant-' + 'b'.repeat(30);
const GOOGLE_KEY = 'AIza' + 'C'.repeat(35);
const GITHUB_PAT = 'ghp_' + 'd'.repeat(36);
const GITHUB_OAUTH = 'gho_' + 'e'.repeat(36);
const GITHUB_FINE = 'github_pat_' + 'f'.repeat(22);
const GITLAB_PAT = 'glpat-' + 'g'.repeat(20);
const SLACK_BOT = 'xoxb-123456-abcdefghij';
const SLACK_USER = 'xoxp-123456-abcdefghij';
const AWS_KEY = 'AKIA' + 'H'.repeat(16);
const JWT = 'eyJ' + 'i'.repeat(25) + '.' + 'j'.repeat(25) + '.' + 'k'.repeat(25);

describe('containsCredential', () => {
  it('returns false for empty string', () => {
    assert.equal(containsCredential(''), false);
  });

  it('returns false for short string', () => {
    assert.equal(containsCredential('hello'), false);
  });

  it('detects OpenAI key', () => {
    assert.equal(containsCredential(`key is ${OPENAI_KEY}`), true);
  });

  it('detects Anthropic key', () => {
    assert.equal(containsCredential(`key is ${ANTHROPIC_KEY}`), true);
  });

  it('detects Google API key', () => {
    assert.equal(containsCredential(`key is ${GOOGLE_KEY}`), true);
  });

  it('detects GitHub PAT', () => {
    assert.equal(containsCredential(`token ${GITHUB_PAT}`), true);
  });

  it('detects GitHub OAuth token', () => {
    assert.equal(containsCredential(`token ${GITHUB_OAUTH}`), true);
  });

  it('detects GitHub fine-grained PAT', () => {
    assert.equal(containsCredential(`token ${GITHUB_FINE}`), true);
  });

  it('detects GitLab PAT', () => {
    assert.equal(containsCredential(`token ${GITLAB_PAT}`), true);
  });

  it('detects Slack bot token', () => {
    assert.equal(containsCredential(`token ${SLACK_BOT}`), true);
  });

  it('detects Slack user token', () => {
    assert.equal(containsCredential(`token ${SLACK_USER}`), true);
  });

  it('detects AWS access key', () => {
    assert.equal(containsCredential(`key ${AWS_KEY}`), true);
  });

  it('detects JWT', () => {
    assert.equal(containsCredential(`bearer ${JWT}`), true);
  });

  it('returns false for normal text', () => {
    assert.equal(containsCredential('The quick brown fox jumps over the lazy dog'), false);
  });

  it('returns false for code that looks vaguely like a key but is not', () => {
    assert.equal(containsCredential('const sk = getSecret()'), false);
  });
});

describe('redactCredentials', () => {
  it('replaces OpenAI key', () => {
    const text = `export OPENAI_API_KEY=${OPENAI_KEY}`;
    const result = redactCredentials(text);
    assert.ok(!result.includes(OPENAI_KEY));
    assert.ok(result.includes(REDACT_PLACEHOLDER));
  });

  it('replaces multiple credentials', () => {
    const text = `keys: ${OPENAI_KEY} and ${GITHUB_PAT}`;
    const result = redactCredentials(text);
    assert.ok(!result.includes(OPENAI_KEY));
    assert.ok(!result.includes(GITHUB_PAT));
  });

  it('preserves non-credential text', () => {
    const text = 'no secrets here, just normal code';
    assert.equal(redactCredentials(text), text);
  });

  it('handles empty string', () => {
    assert.equal(redactCredentials(''), '');
  });

  it('handles short string', () => {
    assert.equal(redactCredentials('hi'), 'hi');
  });
});

describe('findCredentials', () => {
  it('finds OpenAI key with correct type', () => {
    const creds = findCredentials(`key=${OPENAI_KEY}`);
    assert.ok(creds.length >= 1);
    assert.equal(creds[0].type, 'openai_key');
  });

  it('finds Anthropic key with correct type', () => {
    const creds = findCredentials(`key=${ANTHROPIC_KEY}`);
    assert.ok(creds.some(c => c.type === 'anthropic_key'));
  });

  it('finds multiple credentials', () => {
    const text = `first=${OPENAI_KEY} second=${AWS_KEY}`;
    const creds = findCredentials(text);
    assert.ok(creds.length >= 2);
  });

  it('returns empty for clean text', () => {
    assert.deepEqual(findCredentials('just some normal text here'), []);
  });

  it('returns correct start positions', () => {
    const prefix = 'key=';
    const text = prefix + OPENAI_KEY;
    const creds = findCredentials(text);
    assert.ok(creds.length >= 1);
    assert.equal(creds[0].start, prefix.length);
  });

  it('returns empty for empty string', () => {
    assert.deepEqual(findCredentials(''), []);
  });

  it('returns empty for short string', () => {
    assert.deepEqual(findCredentials('short'), []);
  });
});

describe('REDACT_PLACEHOLDER', () => {
  it('has value [REDACTED]', () => {
    assert.equal(REDACT_PLACEHOLDER, '[REDACTED]');
  });
});

describe('CredentialGuard', () => {
  it('scan detects credentials', () => {
    const guard = new CredentialGuard();
    assert.equal(guard.scan(`key=${OPENAI_KEY}`, 'bash'), true);
  });

  it('scan returns false for clean text', () => {
    const guard = new CredentialGuard();
    assert.equal(guard.scan('no secrets here at all', 'bash'), false);
  });

  it('redact replaces credentials', () => {
    const guard = new CredentialGuard();
    const result = guard.redact(`key=${OPENAI_KEY}`);
    assert.ok(!result.includes(OPENAI_KEY));
    assert.ok(result.includes(REDACT_PLACEHOLDER));
  });

  it('getDetections tracks finds', () => {
    const guard = new CredentialGuard();
    guard.scan(`key=${OPENAI_KEY}`, 'bash');
    const dets = guard.getDetections();
    assert.ok(dets.length >= 1);
    assert.equal(dets[0].source, 'bash');
    assert.ok(dets[0].timestamp > 0);
  });

  it('getDetectionCount returns correct count', () => {
    const guard = new CredentialGuard();
    guard.scan(`key=${OPENAI_KEY}`, 'bash');
    guard.scan(`token=${GITHUB_PAT}`, 'tool_output');
    assert.ok(guard.getDetectionCount() >= 2);
  });

  it('clear resets', () => {
    const guard = new CredentialGuard();
    guard.scan(`key=${OPENAI_KEY}`, 'bash');
    guard.clear();
    assert.equal(guard.getDetectionCount(), 0);
    assert.equal(guard.getDetections().length, 0);
  });

  it('capacity is enforced', () => {
    const guard = new CredentialGuard();
    // _maxDetections is 1000; push 1001 via scan
    for (let i = 0; i < 1100; i++) {
      guard.scan(`key=${OPENAI_KEY}`, `source_${i}`);
    }
    assert.equal(guard.getDetectionCount(), 1000);
  });
});
