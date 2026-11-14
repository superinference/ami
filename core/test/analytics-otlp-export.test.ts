/**
 * analytics-otlp-export — OTLP export hardening + analytics-metadata helpers:
 *  - truncateContent + per-attribute *_truncated / *_original_length companions
 *  - cardinality knobs (AMI_OTEL_INCLUDE_SESSION_ID / _VERSION)
 *  - monotonic event.sequence stamped into logRecords
 *  - parseOtelHeaders + header threading into the POST
 *  - getFileExtensionForAnalytics / getFileExtensionsFromBashCommand / sanitizeAgentName
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  truncateContent, MAX_OTLP_ATTR_BYTES, shouldIncludeTelemetryAttr, buildTelemetryResourceAttrs,
  parseOtelHeaders, toOtlpLogsPayload, AnalyticsTracker,
  getFileExtensionForAnalytics, getFileExtensionsFromBashCommand, sanitizeAgentName,
  type TelemetryBatch,
} from '../src/analytics';

function resourceAttrMap(payload: Record<string, unknown>): Record<string, string> {
  const rl = (payload.resourceLogs as Array<Record<string, unknown>>)[0];
  const attrs = ((rl.resource as Record<string, unknown>).attributes as Array<{ key: string; value: { stringValue: string } }>);
  return Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
}
function logRecordAttrs(payload: Record<string, unknown>, idx = 0): Record<string, string> {
  const rl = (payload.resourceLogs as Array<Record<string, unknown>>)[0];
  const sl = (rl.scopeLogs as Array<Record<string, unknown>>)[0];
  const rec = (sl.logRecords as Array<Record<string, unknown>>)[idx];
  const attrs = rec.attributes as Array<{ key: string; value: { stringValue: string } }>;
  return Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
}

describe('truncateContent', () => {
  it('passes a small value through unchanged', () => {
    const r = truncateContent('hello');
    assert.deepEqual(r, { value: 'hello', truncated: false, originalLength: 5 });
  });
  it('clips an oversized value and reports the original length', () => {
    const big = 'x'.repeat(MAX_OTLP_ATTR_BYTES + 100);
    const r = truncateContent(big);
    assert.equal(r.truncated, true);
    assert.equal(r.originalLength, big.length);
    assert.ok(r.value.includes('[TRUNCATED'));
    assert.ok(Buffer.byteLength(r.value, 'utf8') <= MAX_OTLP_ATTR_BYTES + 40);
  });
  it('honors a custom small cap', () => {
    const r = truncateContent('abcdefghij', 4);
    assert.equal(r.truncated, true);
    assert.ok(r.value.startsWith('abcd'));
  });
});

describe('otlpAttrs truncation companions (via toOtlpLogsPayload)', () => {
  it('emits <key>_truncated + <key>_original_length for an oversized attr', () => {
    const big = 'y'.repeat(MAX_OTLP_ATTR_BYTES + 10);
    const batch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: { blob: big } }] };
    const attrs = logRecordAttrs(toOtlpLogsPayload(batch, {}, {}));
    assert.equal(attrs.blob_truncated, 'true');
    assert.equal(attrs.blob_original_length, String(big.length));
  });
  it('omits companions for a small attr', () => {
    const batch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: { k: 'v' } }] };
    const attrs = logRecordAttrs(toOtlpLogsPayload(batch, {}, {}));
    assert.equal(attrs.k, 'v');
    assert.equal(attrs.k_truncated, undefined);
  });
});

describe('telemetry cardinality knobs', () => {
  it('session.id default ON, service.version default ON', () => {
    assert.equal(shouldIncludeTelemetryAttr('session.id', {}), true);
    assert.equal(shouldIncludeTelemetryAttr('service.version', {}), true);
    assert.equal(shouldIncludeTelemetryAttr('service.name', {}), true);
  });
  it('env override suppresses an attribute', () => {
    assert.equal(shouldIncludeTelemetryAttr('session.id', { AMI_OTEL_INCLUDE_SESSION_ID: '0' }), false);
    assert.equal(shouldIncludeTelemetryAttr('service.version', { AMI_OTEL_INCLUDE_VERSION: 'off' }), false);
    assert.equal(shouldIncludeTelemetryAttr('service.version', { AMI_OTEL_INCLUDE_VERSION: '1' }), true);
  });
  it('buildTelemetryResourceAttrs reflects the knobs and merges extras', () => {
    const on = buildTelemetryResourceAttrs('sid', {}, { extra: 'x' });
    assert.equal(on['session.id'], 'sid');
    assert.ok(on['service.version']);
    assert.equal(on.extra, 'x');
    const off = buildTelemetryResourceAttrs('sid', { AMI_OTEL_INCLUDE_SESSION_ID: '0', AMI_OTEL_INCLUDE_VERSION: '0' });
    assert.equal(off['session.id'], undefined);
    assert.equal(off['service.version'], undefined);
    assert.ok(off['service.name']);
  });
  it('toOtlpLogsPayload gates resource attrs via env', () => {
    const batch: TelemetryBatch = { sessionId: 'sess', batchId: 1, events: [] };
    const withId = resourceAttrMap(toOtlpLogsPayload(batch, {}, {}));
    assert.equal(withId['session.id'], 'sess');
    const without = resourceAttrMap(toOtlpLogsPayload(batch, {}, { AMI_OTEL_INCLUDE_SESSION_ID: 'false' }));
    assert.equal(without['session.id'], undefined);
  });
});

describe('event.sequence in logRecords', () => {
  it('stamps event.sequence when present', () => {
    const batch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: {}, sequence: 7 }] };
    assert.equal(logRecordAttrs(toOtlpLogsPayload(batch, {}, {}))['event.sequence'], '7');
  });
  it('omits event.sequence when absent', () => {
    const batch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: {} }] };
    assert.equal(logRecordAttrs(toOtlpLogsPayload(batch, {}, {}))['event.sequence'], undefined);
  });
});

describe('parseOtelHeaders', () => {
  it('parses comma-separated key=value pairs', () => {
    assert.deepEqual(parseOtelHeaders('a=1,b=2'), { a: '1', b: '2' });
  });
  it('preserves = inside a value (API tokens)', () => {
    assert.deepEqual(parseOtelHeaders('Authorization=Bearer abc==def'), { Authorization: 'Bearer abc==def' });
  });
  it('ignores blanks and malformed pairs', () => {
    assert.deepEqual(parseOtelHeaders(' , =v, k='), { k: '' });
    assert.deepEqual(parseOtelHeaders(undefined), {});
  });
});

describe('AnalyticsTracker OTLP headers threading', () => {
  it('sends OTEL_EXPORTER_OTLP_HEADERS + explicit headers on the POST', async () => {
    let seen: Record<string, string> | undefined;
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers; return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_EXPORTER_OTLP_HEADERS: 'x-env=1' }, otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch, otlpHeaders: { 'x-explicit': '2' },
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['x-env'], '1');
    assert.equal(seen?.['x-explicit'], '2');
  });
});

describe('getFileExtensionForAnalytics', () => {
  it('returns the lowercased extension', () => {
    assert.equal(getFileExtensionForAnalytics('/a/b/File.TS'), 'ts');
    assert.equal(getFileExtensionForAnalytics('x.JSON'), 'json');
  });
  it('returns empty for no extension or a dotfile', () => {
    assert.equal(getFileExtensionForAnalytics('/a/Makefile'), '');
    assert.equal(getFileExtensionForAnalytics('.env'), '');
    assert.equal(getFileExtensionForAnalytics(''), '');
  });
  it("collapses an implausibly long extension to 'other'", () => {
    assert.equal(getFileExtensionForAnalytics('f.' + 'z'.repeat(20)), 'other');
  });
});

describe('getFileExtensionsFromBashCommand', () => {
  it('extracts extensions of file-command arguments', () => {
    assert.deepEqual(getFileExtensionsFromBashCommand('cat a.ts && grep foo b.md'), ['md', 'ts']);
  });
  it('skips flags and non-file commands', () => {
    assert.deepEqual(getFileExtensionsFromBashCommand('rm -rf dir.bak'), ['bak']);
    assert.deepEqual(getFileExtensionsFromBashCommand('echo hello.txt'), []);
    assert.deepEqual(getFileExtensionsFromBashCommand(''), []);
  });
});

describe('sanitizeAgentName', () => {
  const builtins = new Set(['general-purpose', 'code-reviewer']);
  it('passes a built-in name through', () => {
    assert.equal(sanitizeAgentName('code-reviewer', builtins), 'code-reviewer');
  });
  it("collapses a custom name to 'user-defined'", () => {
    assert.equal(sanitizeAgentName('/home/me/secret-agent', builtins), 'user-defined');
  });
  it('passes empty through', () => {
    assert.equal(sanitizeAgentName('', builtins), '');
  });
});
