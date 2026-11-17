/**
 * analytics-u024-signal-endpoint-git — two U024 parity additions:
 *  1. Signal-specific OTLP logs endpoint selection (OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)
 *     used VERBATIM (no /v1/logs appended), distinct from the generic endpoint —
 *     resolveOtlpLogsEndpoint + otlpLogsUrl(perSignal) + AnalyticsTracker wiring.
 *  2. Local git user.email detection (getGitEmail) + the opt-in, privacy-preserving
 *     `user.git_email` resource-attribute hash (includeGitUser / AMI_OTEL_INCLUDE_GIT_USER).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  resolveOtlpLogsEndpoint,
  otlpLogsUrl,
  postOtlpBatch,
  getGitEmail,
  hashFilePath,
  AnalyticsTracker,
  type TelemetryBatch,
} from '../src/analytics';

const batch = (): TelemetryBatch => ({ events: [{ type: 't', timestamp: 1, data: {} }], sessionId: 's', batchId: 1 });

// Capture both the URL and request init of each OTLP POST.
const captureFetch = (sink: Array<{ url: string; body: string }>): typeof fetch =>
  (async (url: string, init?: { body?: string }) => {
    sink.push({ url, body: init?.body ?? '' });
    return { ok: true, status: 200 } as Response;
  }) as unknown as typeof fetch;

function resourceAttrs(body: string): Record<string, string> {
  const payload = JSON.parse(body) as Record<string, unknown>;
  const rl = (payload.resourceLogs as Array<Record<string, unknown>>)[0];
  const attrs = (rl.resource as Record<string, unknown>).attributes as Array<{ key: string; value: { stringValue: string } }>;
  return Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
}

describe('U024 resolveOtlpLogsEndpoint — signal-specific selection', () => {
  it('returns null when nothing is configured', () => {
    assert.equal(resolveOtlpLogsEndpoint({} as NodeJS.ProcessEnv), null);
  });
  it('per-signal OTEL_EXPORTER_OTLP_LOGS_ENDPOINT wins and is marked perSignal', () => {
    const r = resolveOtlpLogsEndpoint({
      OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: 'https://c/custom/logs',
      OTEL_EXPORTER_OTLP_ENDPOINT: 'https://generic',
    } as NodeJS.ProcessEnv);
    assert.deepEqual(r, { url: 'https://c/custom/logs', perSignal: true });
  });
  it('falls back to the generic endpoint (not perSignal)', () => {
    assert.deepEqual(
      resolveOtlpLogsEndpoint({ OTEL_EXPORTER_OTLP_ENDPOINT: 'https://generic' } as NodeJS.ProcessEnv),
      { url: 'https://generic', perSignal: false },
    );
  });
  it('honors the AMI SUPERINFERENCE_OTLP_ENDPOINT fallback', () => {
    assert.deepEqual(
      resolveOtlpLogsEndpoint({ SUPERINFERENCE_OTLP_ENDPOINT: 'https://ami' } as NodeJS.ProcessEnv),
      { url: 'https://ami', perSignal: false },
    );
  });
  it('ignores blank / whitespace-only values', () => {
    assert.equal(resolveOtlpLogsEndpoint({ OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: '   ' } as NodeJS.ProcessEnv), null);
    assert.deepEqual(
      resolveOtlpLogsEndpoint({ OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: '  ', OTEL_EXPORTER_OTLP_ENDPOINT: 'https://g' } as NodeJS.ProcessEnv),
      { url: 'https://g', perSignal: false },
    );
  });
});

describe('U024 otlpLogsUrl — perSignal verbatim vs generic append', () => {
  it('generic endpoint gets /v1/logs appended (default)', () => {
    assert.equal(otlpLogsUrl('http://localhost:4318'), 'http://localhost:4318/v1/logs');
    assert.equal(otlpLogsUrl('http://localhost:4318/'), 'http://localhost:4318/v1/logs');
  });
  it('per-signal endpoint is used verbatim (no /v1/logs appended)', () => {
    assert.equal(otlpLogsUrl('https://c/custom/logs', true), 'https://c/custom/logs');
    // A trailing slash is preserved verbatim too.
    assert.equal(otlpLogsUrl('https://c/custom/logs/', true), 'https://c/custom/logs/');
    // Would double-append without the perSignal flag — proving the regression it fixes.
    assert.equal(otlpLogsUrl('https://c/custom/logs', false), 'https://c/custom/logs/v1/logs');
  });
});

describe('U024 postOtlpBatch — perSignal threads into the POST URL', () => {
  it('posts to the endpoint verbatim when perSignal is set', async () => {
    const seen: Array<{ url: string; body: string }> = [];
    await postOtlpBatch('https://c/custom/logs', batch(), { fetchImpl: captureFetch(seen), perSignal: true });
    assert.equal(seen[0].url, 'https://c/custom/logs');
  });
  it('appends /v1/logs when perSignal is absent', async () => {
    const seen: Array<{ url: string; body: string }> = [];
    await postOtlpBatch('https://c/custom/logs', batch(), { fetchImpl: captureFetch(seen) });
    assert.equal(seen[0].url, 'https://c/custom/logs/v1/logs');
  });
});

describe('U024 AnalyticsTracker — per-signal endpoint wiring (end-to-end)', () => {
  it('a perSignal-marked endpoint is POSTed verbatim through the full log→flush path', async () => {
    const seen: Array<{ url: string; body: string }> = [];
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c/custom/logs',
      otlpLogsEndpointPerSignal: true,
      otlpFetch: captureFetch(seen),
      env: {} as NodeJS.ProcessEnv,
    });
    assert.equal(t.isOtlpLogsEndpointPerSignal(), true);
    t.setSessionId('sess');
    t.log('event_one', { a: 1 });
    await t.flushOtlp();
    assert.equal(seen.length, 1);
    assert.equal(seen[0].url, 'https://c/custom/logs');
  });
  it('a generic endpoint still gets /v1/logs appended', async () => {
    const seen: Array<{ url: string; body: string }> = [];
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c/generic',
      otlpFetch: captureFetch(seen),
      env: {} as NodeJS.ProcessEnv,
    });
    assert.equal(t.isOtlpLogsEndpointPerSignal(), false);
    t.setSessionId('sess');
    t.log('event_one');
    await t.flushOtlp();
    assert.equal(seen[0].url, 'https://c/generic/v1/logs');
  });
});

describe('U024 getGitEmail — local git identity', () => {
  it('returns the trimmed email from the git config command', () => {
    const email = getGitEmail('/repo/a', () => '  dev@example.com\n');
    assert.equal(email, 'dev@example.com');
  });
  it('returns undefined on a non-zero exit / git absent (reject-free)', () => {
    const email = getGitEmail('/repo/b', () => { throw new Error('not a git repo'); });
    assert.equal(email, undefined);
  });
  it('returns undefined when no email is configured (empty stdout)', () => {
    assert.equal(getGitEmail('/repo/c', () => '   \n'), undefined);
  });
  it('memoizes per cwd — the command runs once per directory', () => {
    let calls = 0;
    const exec = () => { calls++; return 'memo@example.com'; };
    const dir = '/repo/memo-unique';
    assert.equal(getGitEmail(dir, exec), 'memo@example.com');
    assert.equal(getGitEmail(dir, exec), 'memo@example.com');
    assert.equal(calls, 1);
  });
});

describe('U024 AnalyticsTracker — opt-in git-user resource attribute', () => {
  it('stamps a privacy-preserving hash of the git email (never the raw address)', async () => {
    const seen: Array<{ url: string; body: string }> = [];
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c',
      otlpFetch: captureFetch(seen),
      env: {} as NodeJS.ProcessEnv,
      includeGitUser: true,
      gitUserCwd: '/repo/x',
      gitEmailResolver: () => 'dev@example.com',
    });
    assert.equal(t.getGitUserHash(), hashFilePath('dev@example.com'));
    t.setSessionId('sess');
    t.log('e');
    await t.flushOtlp();
    const attrs = resourceAttrs(seen[0].body);
    assert.equal(attrs['user.git_email'], hashFilePath('dev@example.com'));
    // The raw email never appears anywhere in the exported body.
    assert.ok(!seen[0].body.includes('dev@example.com'));
  });
  it('enables via the AMI_OTEL_INCLUDE_GIT_USER env knob', () => {
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c',
      env: { AMI_OTEL_INCLUDE_GIT_USER: '1' } as NodeJS.ProcessEnv,
      gitUserCwd: '/repo/y',
      gitEmailResolver: () => 'env@example.com',
    });
    assert.equal(t.getGitUserHash(), hashFilePath('env@example.com'));
  });
  it('omits the attribute when the opt is off', () => {
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c',
      env: {} as NodeJS.ProcessEnv,
      gitEmailResolver: () => 'should-not@be-read.com',
    });
    assert.equal(t.getGitUserHash(), undefined);
  });
  it('omits the attribute when git has no email to resolve', () => {
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'https://c',
      env: {} as NodeJS.ProcessEnv,
      includeGitUser: true,
      gitUserCwd: '/repo/z',
      gitEmailResolver: () => undefined,
    });
    assert.equal(t.getGitUserHash(), undefined);
  });
  it('does nothing without an OTLP endpoint (no telemetry → no identity read)', () => {
    let read = false;
    const t = new AnalyticsTracker(undefined, {
      env: {} as NodeJS.ProcessEnv,
      includeGitUser: true,
      gitEmailResolver: () => { read = true; return 'x@y.com'; },
    });
    assert.equal(t.getGitUserHash(), undefined);
    assert.equal(read, false);
  });
});
