import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { redactSecrets } from '../src/tools/tool-utils';

// ---------------------------------------------------------------------------
// Edge-case secret redaction: URL credentials, auth headers, private keys,
// connection strings
// ---------------------------------------------------------------------------

describe('redactSecrets — URL credentials', () => {
  it('redacts password in https:// URL', () => {
    const input = 'https://admin:s3cretP4ss@db.example.com/path';
    const result = redactSecrets(input);
    assert.ok(!result.includes('s3cretP4ss'), 'password should be redacted');
    assert.ok(result.includes('admin:'), 'username should be preserved');
    assert.ok(result.includes('[REDACTED-SECRET]@'), 'placeholder should appear before @');
  });

  it('redacts password in http:// URL', () => {
    const input = 'http://user:longpassword123@host.internal:8080/db';
    const result = redactSecrets(input);
    assert.ok(!result.includes('longpassword123'), 'password should be redacted');
    assert.ok(result.includes('[REDACTED-SECRET]@'));
  });

  it('does NOT redact URLs without credentials', () => {
    const input = 'https://example.com/path?query=1';
    assert.equal(redactSecrets(input), input);
  });

  it('does NOT redact URLs with short passwords (< 4 chars)', () => {
    const input = 'https://u:abc@host.com';
    assert.equal(redactSecrets(input), input);
  });
});

describe('redactSecrets — Bearer / Basic auth headers', () => {
  it('redacts Bearer token (long base64-like string)', () => {
    const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0';
    const input = `Authorization: Bearer ${token}`;
    const result = redactSecrets(input);
    assert.ok(!result.includes(token), 'token should be redacted');
    assert.ok(result.includes('Bearer [REDACTED-SECRET]'));
  });

  it('redacts Basic auth (long base64 string)', () => {
    const creds = 'YWRtaW46c3VwZXJzZWNyZXRwYXNz';  // 30 chars
    const input = `Authorization: Basic ${creds}`;
    const result = redactSecrets(input);
    assert.ok(!result.includes(creds), 'credentials should be redacted');
    assert.ok(result.includes('Basic [REDACTED-SECRET]'));
  });

  it('does NOT redact short tokens after Bearer (< 20 chars)', () => {
    const input = 'Authorization: Bearer shortTok';
    // "shortTok" is only 8 chars, should not be redacted
    assert.equal(redactSecrets(input), input);
  });
});

describe('redactSecrets — private key blocks', () => {
  it('redacts RSA private key block', () => {
    const key = [
      '-----BEGIN RSA PRIVATE KEY-----',
      'MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWMrP8TG3mTh+Q9LAg1CA==',
      '-----END RSA PRIVATE KEY-----',
    ].join('\n');
    const input = `config:\n${key}\nmore text`;
    const result = redactSecrets(input);
    assert.ok(!result.includes('MIIEpAIBAAK'), 'key material should be redacted');
    assert.ok(result.includes('[REDACTED-PRIVATE-KEY]'));
  });

  it('redacts EC private key block', () => {
    const key = [
      '-----BEGIN EC PRIVATE KEY-----',
      'MHQCAQEEIBkg6s3XZ8ygVfOhv1HXGXGX',
      '-----END EC PRIVATE KEY-----',
    ].join('\n');
    const result = redactSecrets(key);
    assert.ok(!result.includes('MHQCAQEEIBkg'), 'key material should be redacted');
    assert.ok(result.includes('[REDACTED-PRIVATE-KEY]'));
  });

  it('redacts generic PRIVATE KEY block', () => {
    const key = [
      '-----BEGIN PRIVATE KEY-----',
      'MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQ==',
      '-----END PRIVATE KEY-----',
    ].join('\n');
    const result = redactSecrets(key);
    assert.ok(result.includes('[REDACTED-PRIVATE-KEY]'));
  });
});

describe('redactSecrets — database connection strings', () => {
  it('redacts postgres:// connection string', () => {
    const input = 'postgres://dbuser:sup3rS3cret@db.example.com:5432/mydb';
    const result = redactSecrets(input);
    assert.ok(!result.includes('sup3rS3cret'), 'password should be redacted');
    assert.ok(result.includes('dbuser:'));
    assert.ok(result.includes('[REDACTED-SECRET]@'));
  });

  it('redacts postgresql:// connection string', () => {
    const input = 'postgresql://admin:p4ssw0rd!!@localhost/testdb';
    const result = redactSecrets(input);
    assert.ok(!result.includes('p4ssw0rd!!'), 'password should be redacted');
  });

  it('redacts mysql:// connection string', () => {
    const input = 'mysql://root:mysqlPass99@127.0.0.1:3306/appdb';
    const result = redactSecrets(input);
    assert.ok(!result.includes('mysqlPass99'));
  });

  it('redacts mongodb:// connection string', () => {
    const input = 'mongodb://mongoUser:m0ng0secret@cluster.example.com/admin';
    const result = redactSecrets(input);
    assert.ok(!result.includes('m0ng0secret'));
  });

  it('redacts redis:// connection string', () => {
    const input = 'redis://default:redisP4ssword@cache.example.com:6379';
    const result = redactSecrets(input);
    assert.ok(!result.includes('redisP4ssword'));
  });

  it('redacts amqp:// connection string', () => {
    const input = 'amqp://guest:rabbitmqSecret@mq.example.com:5672/vhost';
    const result = redactSecrets(input);
    assert.ok(!result.includes('rabbitmqSecret'));
  });
});

describe('redactSecrets — existing patterns still work', () => {
  it('redacts sk- OpenAI key', () => {
    const input = 'key: sk-abcdefghijklmnopqrstuvwx';
    const result = redactSecrets(input);
    assert.ok(!result.includes('sk-abcdefghijklmnopqrstuvwx'));
  });

  it('redacts ghp_ GitHub token', () => {
    const input = 'token=ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij';
    const result = redactSecrets(input);
    assert.ok(!result.includes('ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij'));
  });

  it('redacts AKIA AWS key', () => {
    const input = 'AKIAIOSFODNN7EXAMPLE';
    const result = redactSecrets(input);
    assert.ok(!result.includes('AKIAIOSFODNN7EXAMPLE'));
  });
});

describe('redactSecrets — normal text unchanged', () => {
  it('does not redact plain text', () => {
    const input = 'This is a normal log message with no secrets.';
    assert.equal(redactSecrets(input), input);
  });

  it('does not redact URLs without credentials', () => {
    const input = 'Visit https://docs.example.com/api/v2/users for more info.';
    assert.equal(redactSecrets(input), input);
  });
});

describe('redactSecrets — mixed content with multiple secret types', () => {
  it('redacts all secret types in one string', () => {
    const input = [
      'DB_URL=postgres://admin:dbp4ss99@db.local/app',
      'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxxxxxxxxxxx',
      '-----BEGIN RSA PRIVATE KEY-----',
      'MIIBogIBAAJBALFM/k12345==',
      '-----END RSA PRIVATE KEY-----',
      'OPENAI_KEY=sk-projABCDEFGHIJKLMNOPQR',
    ].join('\n');
    const result = redactSecrets(input);
    assert.ok(!result.includes('dbp4ss99'), 'db password redacted');
    assert.ok(!result.includes('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9'), 'bearer token redacted');
    assert.ok(!result.includes('MIIBogIBAAJBALFM'), 'private key material redacted');
    assert.ok(!result.includes('sk-projABCDEFGHIJKLMNOPQR'), 'openai key redacted');
    assert.ok(result.includes('[REDACTED-PRIVATE-KEY]'), 'private key placeholder present');
  });
});
