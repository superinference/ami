import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  AnalyticsTracker,
  sanitizeAnalyticsData,
  markAnalyticsSafe,
  type AnalyticsSafeMetadata,
} from '../src/analytics';

const tmpDirs: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-analytics-egress-'));
  tmpDirs.push(d);
  return d;
}
afterEach(() => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

const LS = String.fromCodePoint(0x2028);
const PS = String.fromCodePoint(0x2029);

describe('analytics egress — NDJSON-safe JSONL writes', () => {
  it('escapes U+2028/U+2029 in a persisted event so a line-splitting reader cannot tear the record', () => {
    const dir = mkdir();
    const tracker = new AnalyticsTracker(dir);
    // A tool name is a fixed-vocabulary field that is NOT content-redacted, so the
    // separator survives the redaction pass and reaches the serializer verbatim.
    tracker.log('tool_use', { label: `before${LS}middle${PS}after` });

    const file = path.join(dir, 'analytics.jsonl');
    const contents = fs.readFileSync(file, 'utf8');

    // Raw separators must NOT appear on disk — they would split the JSONL record.
    assert.equal(contents.includes(LS), false, 'raw U+2028 must be escaped');
    assert.equal(contents.includes(PS), false, 'raw U+2029 must be escaped');
    // Escaped form is present and the file is exactly one newline-terminated line.
    assert.ok(contents.includes('\\u2028'));
    assert.ok(contents.includes('\\u2029'));
    const lines = contents.split('\n').filter(Boolean);
    assert.equal(lines.length, 1);
    // ...and it round-trips back to the original value.
    const parsed = JSON.parse(lines[0]);
    assert.equal(parsed.data.label, `before${LS}middle${PS}after`);
  });

  it('writes the analytics log with a restrictive 0o600 file mode (dir 0o700)', { skip: process.platform === 'win32' }, () => {
    const dir = mkdir();
    const nested = path.join(dir, 'state');
    const tracker = new AnalyticsTracker(nested);
    tracker.log('session_start', { ok: 1 });

    const file = path.join(nested, 'analytics.jsonl');
    const fileMode = fs.statSync(file).mode & 0o777;
    assert.equal(fileMode, 0o600, `expected 0o600, got 0o${fileMode.toString(8)}`);
    const dirMode = fs.statSync(nested).mode & 0o777;
    assert.equal(dirMode, 0o700, `expected 0o700, got 0o${dirMode.toString(8)}`);
  });

  it('skips a poison (unserializable) event without crashing the writer or losing prior lines', () => {
    const dir = mkdir();
    // redact=false writes verbatim, so a genuinely circular payload reaches the
    // NDJSON serializer and must be quarantined (skipped) rather than thrown.
    const tracker = new AnalyticsTracker(dir, { redact: false, env: {} });
    tracker.log('ok_event', { a: 1 });
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    assert.doesNotThrow(() => tracker.log('poison', { circular }));
    tracker.log('ok_event_2', { b: 2 });

    const lines = fs.readFileSync(path.join(dir, 'analytics.jsonl'), 'utf8').split('\n').filter(Boolean);
    const types = lines.map(l => JSON.parse(l).type);
    assert.deepEqual(types, ['ok_event', 'ok_event_2']);
  });
});

describe('analytics egress — redaction brand guard', () => {
  it('sanitizeAnalyticsData returns a value usable wherever branded AnalyticsSafeMetadata is required', () => {
    const safe: AnalyticsSafeMetadata = sanitizeAnalyticsData({ prompt: 'secret text', model: 'opus' }, { env: {} });
    // Runtime redaction still strips the free-text content key.
    assert.equal(safe.prompt, '<REDACTED>');
    assert.equal(safe.prompt_length, 'secret text'.length);
    assert.equal(safe.model, 'opus');
  });

  it('markAnalyticsSafe is an identity brand (no copy, no mutation) for pre-verified data', () => {
    const input = { source: 'slash_command' };
    const branded: AnalyticsSafeMetadata = markAnalyticsSafe(input);
    assert.equal(branded, input);
    assert.deepEqual({ ...branded }, { source: 'slash_command' });
  });

  it('redact=false path brands raw data and still persists it verbatim', () => {
    const dir = mkdir();
    const tracker = new AnalyticsTracker(dir, { redact: false, env: {} });
    tracker.log('verbatim', { prompt: 'keep me raw' });
    const line = fs.readFileSync(path.join(dir, 'analytics.jsonl'), 'utf8').trim();
    assert.equal(JSON.parse(line).data.prompt, 'keep me raw');
  });

  it('redact=true persists only the redaction-verified copy to disk', () => {
    const dir = mkdir();
    const tracker = new AnalyticsTracker(dir, { env: {} });
    tracker.log('turn', { prompt: 'user secret', model: 'opus' });
    const line = fs.readFileSync(path.join(dir, 'analytics.jsonl'), 'utf8').trim();
    const data = JSON.parse(line).data;
    assert.equal(data.prompt, '<REDACTED>');
    assert.equal(data.model, 'opus');
    // In-memory copy stays raw for faithful local introspection.
    assert.equal(tracker.getEvents()[0].data.prompt, 'user secret');
  });
});
