import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveBackgroundTimeoutMs,
  applyCaBundleFanout,
} from '../src/tools/bash';

const ENV_KEY = 'SUPERINFERENCE_BACKGROUND_COMMAND_TIMEOUT_MS';
const DEFAULT_BG = 30 * 60 * 1000;
const MAX_BG = 2 * 60 * 60 * 1000;

describe('U030 w69: resolveBackgroundTimeoutMs — reject non-positive env values', () => {
  const saved = process.env[ENV_KEY];
  afterEach(() => {
    if (saved === undefined) delete process.env[ENV_KEY];
    else process.env[ENV_KEY] = saved;
  });

  it('falls back to the default when the env knob is unset', () => {
    delete process.env[ENV_KEY];
    assert.equal(resolveBackgroundTimeoutMs(undefined), DEFAULT_BG);
  });

  it('rejects a NEGATIVE env value and falls back to the default', () => {
    // RED on revert: the old `parseInt(raw,10)` idiom let -5 through, yielding a
    // nonsensical negative background timeout.
    process.env[ENV_KEY] = '-5';
    assert.equal(resolveBackgroundTimeoutMs(undefined), DEFAULT_BG);
  });

  it('rejects a ZERO env value and falls back to the default', () => {
    process.env[ENV_KEY] = '0';
    assert.equal(resolveBackgroundTimeoutMs(undefined), DEFAULT_BG);
  });

  it('rejects a non-numeric env value (NaN) and falls back to the default', () => {
    // RED on revert: parseInt('abc') → NaN → Math.min(NaN, MAX) → NaN.
    process.env[ENV_KEY] = 'abc';
    const v = resolveBackgroundTimeoutMs(undefined);
    assert.ok(Number.isFinite(v));
    assert.equal(v, DEFAULT_BG);
  });

  it('honors a positive env value (clamped to the ceiling)', () => {
    process.env[ENV_KEY] = '60000';
    assert.equal(resolveBackgroundTimeoutMs(undefined), 60000);
    process.env[ENV_KEY] = String(MAX_BG * 10);
    assert.equal(resolveBackgroundTimeoutMs(undefined), MAX_BG);
  });

  it('a positive per-call timeout wins over the env knob and default', () => {
    process.env[ENV_KEY] = '60000';
    assert.equal(resolveBackgroundTimeoutMs(5000), 5000);
    // numeric-string coercion (providers serialize numbers as strings)
    assert.equal(resolveBackgroundTimeoutMs('7000'), 7000);
  });

  it('a non-positive per-call timeout falls through to the env/default', () => {
    delete process.env[ENV_KEY];
    assert.equal(resolveBackgroundTimeoutMs(-1), DEFAULT_BG);
    assert.equal(resolveBackgroundTimeoutMs(0), DEFAULT_BG);
  });
});

describe('U030 w69: applyCaBundleFanout — single CA knob → all runtime trust vars', () => {
  it('fans NODE_EXTRA_CA_CERTS out to SSL_CERT_FILE / REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE', () => {
    const env: NodeJS.ProcessEnv = { NODE_EXTRA_CA_CERTS: '/etc/ca/corp.pem' };
    applyCaBundleFanout(env);
    assert.equal(env.SSL_CERT_FILE, '/etc/ca/corp.pem');
    assert.equal(env.REQUESTS_CA_BUNDLE, '/etc/ca/corp.pem');
    assert.equal(env.CURL_CA_BUNDLE, '/etc/ca/corp.pem');
  });

  it('does not override a per-runtime var the user already set', () => {
    const env: NodeJS.ProcessEnv = {
      NODE_EXTRA_CA_CERTS: '/etc/ca/corp.pem',
      CURL_CA_BUNDLE: '/custom/curl.pem',
    };
    applyCaBundleFanout(env);
    assert.equal(env.CURL_CA_BUNDLE, '/custom/curl.pem'); // explicit override wins
    assert.equal(env.SSL_CERT_FILE, '/etc/ca/corp.pem');
    assert.equal(env.REQUESTS_CA_BUNDLE, '/etc/ca/corp.pem');
  });

  it('is a no-op when no CA bundle is configured', () => {
    const env: NodeJS.ProcessEnv = { PATH: '/usr/bin' };
    applyCaBundleFanout(env);
    assert.equal(env.SSL_CERT_FILE, undefined);
    assert.equal(env.REQUESTS_CA_BUNDLE, undefined);
    assert.equal(env.CURL_CA_BUNDLE, undefined);
  });

  it('returns the same env object (mutates in place)', () => {
    const env: NodeJS.ProcessEnv = { NODE_EXTRA_CA_CERTS: '/a.pem' };
    assert.equal(applyCaBundleFanout(env), env);
  });
});
