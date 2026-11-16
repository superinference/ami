import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as http from 'http';
import * as crypto from 'crypto';

import {
  detectTerminalType,
  getHostPlatformForAnalytics,
  HOST_PLATFORM_OVERRIDES,
  getEnvironmentFingerprint,
  getCiContext,
  getOrCreateUserId,
  USER_ID_FILE,
  getUserBucket,
  NUM_USER_BUCKETS,
  bucketHttpStatus,
  deriveHttpStatusFields,
  buildTelemetryResourceAttrs,
  toOtlpLogsPayload,
  AnalyticsTracker,
  type TelemetryBatch,
} from '../src/analytics';

const E = (o: Record<string, string> = {}) => o as unknown as NodeJS.ProcessEnv;

// ---------------------------------------------------------------------------
// detectTerminalType
// ---------------------------------------------------------------------------
describe('detectTerminalType', () => {
  it('returns null when nothing identifiable is present', () => {
    assert.equal(detectTerminalType(E({}), 'linux'), null);
  });
  it('detects cursor via CURSOR_TRACE_ID and via askpass', () => {
    assert.equal(detectTerminalType(E({ CURSOR_TRACE_ID: 'x' }), 'linux'), 'cursor');
    assert.equal(detectTerminalType(E({ VSCODE_GIT_ASKPASS_MAIN: '/a/cursor/b' }), 'linux'), 'cursor');
    assert.equal(detectTerminalType(E({ VSCODE_GIT_ASKPASS_MAIN: '/windsurf/x' }), 'linux'), 'windsurf');
  });
  it('detects editors/IDEs via bundle id', () => {
    assert.equal(detectTerminalType(E({ __CFBundleIdentifier: 'com.VSCodium' }), 'darwin'), 'codium');
    assert.equal(detectTerminalType(E({ __CFBundleIdentifier: 'com.windsurf.app' }), 'darwin'), 'windsurf');
    assert.equal(detectTerminalType(E({ __CFBundleIdentifier: 'com.jetbrains.pycharm' }), 'darwin'), 'pycharm');
    assert.equal(detectTerminalType(E({ __CFBundleIdentifier: 'com.unknown.app' }), 'darwin'), null);
  });
  it('detects visualstudio, jetbrains jediterm, ghostty and kitty', () => {
    assert.equal(detectTerminalType(E({ VisualStudioVersion: '17' }), 'linux'), 'visualstudio');
    assert.equal(detectTerminalType(E({ TERMINAL_EMULATOR: 'JetBrains-JediTerm' }), 'linux'), 'pycharm');
    assert.equal(detectTerminalType(E({ TERMINAL_EMULATOR: 'JetBrains-JediTerm' }), 'darwin'), 'pycharm');
    assert.equal(detectTerminalType(E({ TERM: 'xterm-ghostty' }), 'linux'), 'ghostty');
    assert.equal(detectTerminalType(E({ TERM: 'xterm-kitty' }), 'linux'), 'kitty');
  });
  it('falls back through TERM_PROGRAM, multiplexers and emulator env vars', () => {
    assert.equal(detectTerminalType(E({ TERM_PROGRAM: 'iTerm.app' }), 'linux'), 'iTerm.app');
    assert.equal(detectTerminalType(E({ TMUX: '1' }), 'linux'), 'tmux');
    assert.equal(detectTerminalType(E({ STY: '1' }), 'linux'), 'screen');
    assert.equal(detectTerminalType(E({ KONSOLE_VERSION: '22' }), 'linux'), 'konsole');
    assert.equal(detectTerminalType(E({ GNOME_TERMINAL_SERVICE: ':1' }), 'linux'), 'gnome-terminal');
    assert.equal(detectTerminalType(E({ XTERM_VERSION: '1' }), 'linux'), 'xterm');
    assert.equal(detectTerminalType(E({ VTE_VERSION: '6003' }), 'linux'), 'vte-based');
    assert.equal(detectTerminalType(E({ ALACRITTY_LOG: '/x' }), 'linux'), 'alacritty');
    assert.equal(detectTerminalType(E({ WT_SESSION: 'x' }), 'win32'), 'windows-terminal');
    assert.equal(detectTerminalType(E({ WSL_DISTRO_NAME: 'Ubuntu' }), 'linux'), 'wsl-Ubuntu');
    assert.equal(detectTerminalType(E({ TERM: 'xterm-256color' }), 'linux'), 'xterm-256color');
  });
  it('defaults to process.env/process.platform', () => {
    // Does not throw; result depends on the runner env, so only assert the type.
    const r = detectTerminalType();
    assert.ok(r === null || typeof r === 'string');
  });
});

// ---------------------------------------------------------------------------
// getHostPlatformForAnalytics + fingerprint
// ---------------------------------------------------------------------------
describe('getHostPlatformForAnalytics', () => {
  it('honors a valid AMI_HOST_PLATFORM override', () => {
    for (const p of HOST_PLATFORM_OVERRIDES) {
      assert.equal(getHostPlatformForAnalytics(E({ AMI_HOST_PLATFORM: p }), 'linux'), p);
    }
  });
  it('ignores an invalid override and uses the detected platform', () => {
    assert.equal(getHostPlatformForAnalytics(E({ AMI_HOST_PLATFORM: 'solaris' }), 'darwin'), 'darwin');
    assert.equal(getHostPlatformForAnalytics(E({}), 'win32'), 'win32');
  });
  it('defaults to process.platform', () => {
    assert.equal(getHostPlatformForAnalytics(), process.platform);
  });
});

describe('getEnvironmentFingerprint', () => {
  it('reports host platform (override-aware), arch and node version', () => {
    const fp = getEnvironmentFingerprint(E({ AMI_HOST_PLATFORM: 'darwin' }), { platform: 'linux', arch: 'arm64', nodeVersion: 'v99.9.9' });
    assert.deepEqual(fp, { 'host.platform': 'darwin', 'host.arch': 'arm64', 'node.version': 'v99.9.9' });
  });
  it('falls back to real process values', () => {
    const fp = getEnvironmentFingerprint(E({}));
    assert.equal(fp['host.arch'], process.arch);
    assert.equal(fp['node.version'], process.version);
  });
});

// ---------------------------------------------------------------------------
// getCiContext
// ---------------------------------------------------------------------------
describe('getCiContext', () => {
  it('returns null outside GitHub Actions', () => {
    assert.equal(getCiContext(E({})), null);
    assert.equal(getCiContext(E({ GITHUB_ACTIONS: 'false' })), null);
  });
  it('captures actor/repository/owner context in CI', () => {
    const ctx = getCiContext(E({
      GITHUB_ACTIONS: 'true',
      GITHUB_ACTOR: 'octocat',
      GITHUB_REPOSITORY: 'o/r',
      GITHUB_REPOSITORY_OWNER: 'o',
      GITHUB_REPOSITORY_OWNER_ID: '42',
    }));
    assert.ok(ctx);
    assert.equal(ctx!.ci, 'true');
    assert.equal(ctx!['ci.actor'], 'octocat');
    assert.equal(ctx!['ci.repository'], 'o/r');
    assert.equal(ctx!['ci.repository_owner'], 'o');
    assert.equal(ctx!['ci.repository_owner_id'], '42');
    // Unset optional vars are omitted entirely.
    assert.equal('ci.actor_id' in ctx!, false);
  });
});

// ---------------------------------------------------------------------------
// getOrCreateUserId + getUserBucket
// ---------------------------------------------------------------------------
describe('getOrCreateUserId', () => {
  let tmp: string;
  afterEach(() => { if (tmp && fs.existsSync(tmp)) fs.rmSync(tmp, { recursive: true, force: true }); });

  it('persists and re-reads a stable 64-hex id', () => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'uid-'));
    const first = getOrCreateUserId(tmp);
    assert.match(first, /^[0-9a-f]{64}$/);
    assert.ok(fs.existsSync(path.join(tmp, USER_ID_FILE)));
    const second = getOrCreateUserId(tmp);
    assert.equal(second, first, 'stable across calls');
  });
  it('regenerates when the persisted value is malformed', () => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'uid-'));
    fs.writeFileSync(path.join(tmp, USER_ID_FILE), 'not-a-valid-id\n');
    const id = getOrCreateUserId(tmp);
    assert.match(id, /^[0-9a-f]{64}$/);
  });
  it('returns an ephemeral id (no persistence) when no dir is given', () => {
    const a = getOrCreateUserId();
    const b = getOrCreateUserId();
    assert.match(a, /^[0-9a-f]{64}$/);
    assert.notEqual(a, b);
  });
});

describe('getUserBucket', () => {
  it('maps into [0, NUM_USER_BUCKETS) deterministically', () => {
    const id = 'a'.repeat(64);
    const b = getUserBucket(id);
    assert.ok(Number.isInteger(b) && b >= 0 && b < NUM_USER_BUCKETS);
    assert.equal(getUserBucket(id), b, 'deterministic');
    // Matches the SHA-256(first 8 hex) mod N contract.
    const expected = parseInt(crypto.createHash('sha256').update(id).digest('hex').slice(0, 8), 16) % NUM_USER_BUCKETS;
    assert.equal(b, expected);
  });
  it('honors a custom bucket count and clamps invalid counts', () => {
    assert.ok(getUserBucket('x', 5) < 5);
    assert.ok(getUserBucket('x', 0) < NUM_USER_BUCKETS);
    assert.ok(getUserBucket('x', NaN) < NUM_USER_BUCKETS);
  });
});

// ---------------------------------------------------------------------------
// bucketHttpStatus + deriveHttpStatusFields
// ---------------------------------------------------------------------------
describe('bucketHttpStatus', () => {
  it('derives Nxx ranges for valid HTTP statuses', () => {
    assert.equal(bucketHttpStatus(200), '2xx');
    assert.equal(bucketHttpStatus('404'), '4xx');
    assert.equal(bucketHttpStatus(503), '5xx');
    assert.equal(bucketHttpStatus(100), '1xx');
  });
  it('returns undefined for non-HTTP / out-of-range / empty', () => {
    assert.equal(bucketHttpStatus(undefined), undefined);
    assert.equal(bucketHttpStatus(null), undefined);
    assert.equal(bucketHttpStatus(''), undefined);
    assert.equal(bucketHttpStatus('ok'), undefined);
    assert.equal(bucketHttpStatus(42), undefined);
    assert.equal(bucketHttpStatus(600), undefined);
  });
});

describe('deriveHttpStatusFields', () => {
  it('renames numeric HTTP status to http_status + range', () => {
    const out = deriveHttpStatusFields({ status: 429, endpoint: 'x' });
    assert.equal(out.status, undefined);
    assert.equal(out.http_status, '429');
    assert.equal(out.http_status_range, '4xx');
    assert.equal(out.endpoint, 'x');
  });
  it('leaves a non-HTTP status untouched (same reference)', () => {
    const data = { status: 'ok', n: 1 };
    assert.equal(deriveHttpStatusFields(data), data);
  });
});

// ---------------------------------------------------------------------------
// buildTelemetryResourceAttrs enrichment + toOtlpLogsPayload integration
// ---------------------------------------------------------------------------
describe('buildTelemetryResourceAttrs enrichment', () => {
  it('attaches environment fingerprint always, terminal.type + CI only when present', () => {
    const plain = buildTelemetryResourceAttrs('sid', E({}));
    assert.ok(plain['host.platform']);
    assert.equal(plain['host.arch'], process.arch);
    assert.equal(plain['terminal.type'], undefined);
    assert.equal(plain.ci, undefined);

    const rich = buildTelemetryResourceAttrs('sid', E({
      TERM_PROGRAM: 'ghostty', GITHUB_ACTIONS: 'true', GITHUB_ACTOR: 'me', AMI_HOST_PLATFORM: 'linux',
    }));
    assert.equal(rich['terminal.type'], 'ghostty');
    assert.equal(rich['host.platform'], 'linux');
    assert.equal(rich.ci, 'true');
    assert.equal(rich['ci.actor'], 'me');
  });
  it('lets explicit extras override derived attrs', () => {
    const attrs = buildTelemetryResourceAttrs('sid', E({}), { 'host.arch': 'custom' });
    assert.equal(attrs['host.arch'], 'custom');
  });
});

describe('toOtlpLogsPayload HTTP-status shaping', () => {
  const mk = (data: Record<string, unknown>): TelemetryBatch => ({ sessionId: 's', batchId: 1, events: [{ type: 'export', timestamp: 1, data }] });
  it('shapes a numeric status in the exported log record attributes', () => {
    const payload = toOtlpLogsPayload(mk({ status: 500 }), {}, E({}));
    const attrs = (((payload.resourceLogs as any)[0]).scopeLogs[0].logRecords[0].attributes as Array<{ key: string; value: { stringValue: string } }>);
    const byKey = Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
    assert.equal(byKey.http_status, '500');
    assert.equal(byKey.http_status_range, '5xx');
    assert.equal('status' in byKey, false);
  });
});

// ---------------------------------------------------------------------------
// AnalyticsTracker wiring: user.id/bucket, timeout, batch delay, killswitch
// ---------------------------------------------------------------------------
async function makeSink(): Promise<{ base: string; received: Array<{ body: any; init: any }>; close: () => Promise<void> }> {
  const received: Array<{ body: any; init: any }> = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', c => chunks.push(c as Buffer));
    req.on('end', () => {
      let body: any = null;
      try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { /* ignore */ }
      received.push({ body, init: req.headers });
      res.statusCode = 200;
      res.end('ok');
    });
  });
  await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
  const addr = server.address();
  const port = typeof addr === 'object' && addr ? addr.port : 0;
  return { base: `http://127.0.0.1:${port}`, received, close: () => new Promise<void>(r => server.close(() => r())) };
}

describe('AnalyticsTracker telemetry resource wiring', () => {
  let tmp: string | undefined;
  afterEach(() => { if (tmp && fs.existsSync(tmp)) fs.rmSync(tmp, { recursive: true, force: true }); tmp = undefined; });

  it('stamps a stable user.id + user.bucket onto exports when a dir is available', async () => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ana-'));
    const sink = await makeSink();
    try {
      const t = new AnalyticsTracker(tmp, { otlpEndpoint: sink.base, env: E({}) });
      t.setSessionId('sess');
      t.log('e', {});
      await t.flushOtlp();
      const attrs = sink.received[0].body.resourceLogs[0].resource.attributes as Array<{ key: string; value: { stringValue: string } }>;
      const byKey = Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
      assert.match(byKey['user.id'], /^[0-9a-f]{64}$/);
      assert.ok(Number(byKey['user.bucket']) >= 0 && Number(byKey['user.bucket']) < NUM_USER_BUCKETS);
      // Persisted id file co-located with the analytics log.
      assert.ok(fs.existsSync(path.join(tmp, USER_ID_FILE)));
    } finally { await sink.close(); }
  });

  it('omits user.id when no endpoint and no dir is configured', async () => {
    const sink = await makeSink();
    try {
      const t = new AnalyticsTracker(undefined, { otlpEndpoint: sink.base, env: E({}) });
      t.log('e', {});
      await t.flushOtlp();
      const attrs = sink.received[0].body.resourceLogs[0].resource.attributes as Array<{ key: string; value: { stringValue: string } }>;
      assert.equal(attrs.find(a => a.key === 'user.id'), undefined);
    } finally { await sink.close(); }
  });

  it('threads a per-request timeout into the OTLP POST', async () => {
    let seenTimeout = false;
    const fetchImpl = (async (_url: string, init: any) => {
      // The AbortSignal fires after otlpTimeoutMs; a never-resolving fetch would abort.
      seenTimeout = init.signal instanceof AbortSignal;
      return { ok: true } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://h/v1/logs', otlpFetch: fetchImpl, env: E({}), otlpTimeoutMs: 1234 });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seenTimeout, true);
  });

  it('paces consecutive batch POSTs by otlpBatchDelayMs during the drain', async () => {
    const delays: number[] = [];
    let fail = true;
    const fetchImpl = (async () => ({ ok: !fail, status: fail ? 500 : 200 } as Response)) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: fetchImpl, env: E({}),
      telemetryMaxBatchSize: 1, otlpMaxAttempts: 1, otlpBatchDelayMs: 25,
      sleepImpl: async (ms) => { delays.push(ms); },
    });
    // Mid-session sends fail and requeue, accumulating a multi-batch backlog.
    t.log('a', {});
    t.log('b', {});
    t.log('c', {});
    await t.whenSent();
    // Now the endpoint recovers; the drain delivers three single-event batches
    // with a configured pause between consecutive POSTs (none after the last).
    fail = false;
    await t.flushOtlp();
    const paused = delays.filter(d => d === 25).length;
    assert.equal(paused, 2, `expected 2 inter-batch pauses, got ${JSON.stringify(delays)}`);
  });

  it('killswitch suppresses all traffic while set, delivers once cleared', async () => {
    const sink = await makeSink();
    try {
      let killed = true;
      const t = new AnalyticsTracker(undefined, { otlpEndpoint: sink.base, env: E({}), isKilled: () => killed });
      t.setSessionId('k');
      t.log('e', {});
      await t.flushOtlp();
      assert.equal(sink.received.length, 0, 'nothing sent while killed');
      killed = false;
      await t.flushOtlp();
      assert.equal(sink.received.length, 1, 'delivered after the flag cleared');
    } finally { await sink.close(); }
  });
});
