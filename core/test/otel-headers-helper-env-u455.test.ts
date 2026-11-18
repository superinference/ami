import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  AnalyticsTracker,
  resolveOtelHeadersHelperCommand,
  OTEL_HEADERS_HELPER_ENV_VARS,
} from '../src/analytics';

// ---------------------------------------------------------------------------
// U455 — otelHeadersHelper is reachable from the environment.
//
// The OtelHeadersHelper class + its AnalyticsTracker opt already existed, but the
// knob was only reachable by threading an explicit option — so an operator could
// never activate it in production (the engine builds the tracker from env, never
// passing the opt). This wires the command from the operator-controlled env
// (AMI_OTEL_HEADERS_HELPER / CLAUDE_CODE_OTEL_HEADERS_HELPER) so a configured
// helper mints dynamic OTLP auth headers on every export.
// ---------------------------------------------------------------------------

const live = (extra: Record<string, string> = {}): NodeJS.ProcessEnv =>
  ({ ...extra } as NodeJS.ProcessEnv); // no NODE_ENV='test' → egress not suppressed

describe('resolveOtelHeadersHelperCommand', () => {
  it('returns undefined when no env var is set', () => {
    assert.equal(resolveOtelHeadersHelperCommand({} as NodeJS.ProcessEnv), undefined);
  });

  it('reads the AMI-native var', () => {
    assert.equal(
      resolveOtelHeadersHelperCommand({ AMI_OTEL_HEADERS_HELPER: 'mint-auth' } as NodeJS.ProcessEnv),
      'mint-auth',
    );
  });

  it('falls back to the Claude-compatible var', () => {
    assert.equal(
      resolveOtelHeadersHelperCommand({ CLAUDE_CODE_OTEL_HEADERS_HELPER: 'cc-cmd' } as NodeJS.ProcessEnv),
      'cc-cmd',
    );
  });

  it('prefers the AMI-native var over the Claude-compatible one', () => {
    assert.equal(
      resolveOtelHeadersHelperCommand({
        AMI_OTEL_HEADERS_HELPER: 'ami',
        CLAUDE_CODE_OTEL_HEADERS_HELPER: 'cc',
      } as NodeJS.ProcessEnv),
      'ami',
    );
  });

  it('ignores a blank / whitespace-only value and trims', () => {
    assert.equal(resolveOtelHeadersHelperCommand({ AMI_OTEL_HEADERS_HELPER: '   ' } as NodeJS.ProcessEnv), undefined);
    assert.equal(
      resolveOtelHeadersHelperCommand({ AMI_OTEL_HEADERS_HELPER: '  mint  ' } as NodeJS.ProcessEnv),
      'mint',
    );
    // A blank AMI var does NOT shadow a set Claude-compatible var.
    assert.equal(
      resolveOtelHeadersHelperCommand({
        AMI_OTEL_HEADERS_HELPER: '',
        CLAUDE_CODE_OTEL_HEADERS_HELPER: 'cc',
      } as NodeJS.ProcessEnv),
      'cc',
    );
  });

  it('exposes the consulted env vars in precedence order', () => {
    assert.deepEqual(
      [...OTEL_HEADERS_HELPER_ENV_VARS],
      ['AMI_OTEL_HEADERS_HELPER', 'CLAUDE_CODE_OTEL_HEADERS_HELPER'],
    );
  });
});

describe('AnalyticsTracker wires otelHeadersHelper from the environment', () => {
  it('mints and merges dynamic OTLP headers from an env-configured command', async () => {
    let seen: Record<string, string> | undefined;
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers;
      return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      // Env-sourced helper (no opt): proves the knob is reachable from config alone.
      env: live({ AMI_OTEL_HEADERS_HELPER: `echo '{"X-Env-Auth":"minted"}'` }),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      otlpHeaders: { 'x-static': 'a' },
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['x-static'], 'a', 'static header is kept');
    assert.equal(seen?.['X-Env-Auth'], 'minted', 'env-helper header is applied to the POST');
  });

  it('does NOT run any helper when neither the opt nor the env var is set', async () => {
    let seen: Record<string, string> | undefined;
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers;
      return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: live(),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      otlpHeaders: { 'x-static': 'a' },
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['x-static'], 'a', 'static header is kept');
    assert.equal(seen?.['X-Env-Auth'], undefined, 'no helper ran, so no minted header');
  });

  it('lets an explicit opt override the env-resolved command', async () => {
    let seen: Record<string, string> | undefined;
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers;
      return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: live({ AMI_OTEL_HEADERS_HELPER: `echo '{"X-Env-Auth":"env"}'` }),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      // The injected opt (exec) must win over the env command.
      otlpHeadersHelper: { command: 'c', exec: () => JSON.stringify({ 'X-Opt-Auth': 'opt' }) },
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['X-Opt-Auth'], 'opt', 'opt helper wins');
    assert.equal(seen?.['X-Env-Auth'], undefined, 'env helper is not used when the opt is present');
  });
});
