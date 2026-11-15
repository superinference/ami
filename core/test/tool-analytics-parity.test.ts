import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { ToolAnalytics } from '../src/tool-analytics';

// Parity-hardening coverage for tool-usage analytics as an additive superset of
// Claude Code's tool-usage reporting. Each block is a RED-GREEN reproducer:
// it fails against the pre-fix behaviour and passes with the hardening in place.

describe('ToolAnalytics parity — case-insensitive tool key normalization', () => {
  // GAP: record()/getStats() previously keyed on the raw tool name, so `Bash`
  // and `bash` fragmented into separate counters (Claude Code maps Bash→bash).
  it('aggregates differently-cased names into one counter', () => {
    const ta = new ToolAnalytics();
    ta.record('Bash', 100, false);
    ta.record('bash', 200, true);
    ta.record('BASH', 300, false);
    const s = ta.getStats('bAsH');
    // RED (pre-fix): three separate entries → getStats('bAsH').callCount === 0.
    assert.equal(s.callCount, 3);
    assert.equal(s.errorCount, 1);
    assert.equal(s.totalDurationMs, 600);
    assert.equal(s.tool, 'bash');
    // Summary collapses to a single tool, not three.
    assert.equal(ta.getSummary().length, 1);
    assert.equal(ta.totalCalls, 3);
  });
});

describe('ToolAnalytics parity — duration sanitization', () => {
  // GAP: NaN/Infinity/negative durations poisoned totals (NaN avg) and min.
  it('ignores non-finite durations instead of propagating NaN', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', Number.NaN, false);
    ta.record('bash', Number.POSITIVE_INFINITY, false);
    ta.record('bash', 100, false);
    const s = ta.getStats('bash');
    // RED (pre-fix): totalDurationMs/avg become NaN.
    assert.ok(Number.isFinite(s.avgDurationMs), 'avg must be finite');
    assert.ok(Number.isFinite(s.totalDurationMs), 'total must be finite');
    assert.equal(s.totalDurationMs, 100);
    assert.equal(s.avgDurationMs, 100 / 3);
    assert.ok(Number.isFinite(s.p99DurationMs));
  });

  it('clamps negative durations to zero (never a negative min)', () => {
    const ta = new ToolAnalytics();
    ta.record('grep', -50, false);
    ta.record('grep', 20, false);
    const s = ta.getStats('grep');
    // RED (pre-fix): minDurationMs === -50 and totalDurationMs === -30.
    assert.equal(s.minDurationMs, 0);
    assert.equal(s.totalDurationMs, 20);
    assert.ok(s.minDurationMs >= 0);
  });
});

describe('ToolAnalytics parity — success metrics', () => {
  // GAP: only errorRate was exposed; Claude Code reports success counts/rates.
  it('exposes successCount and successRate complementary to errors', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('bash', 100, false);
    ta.record('bash', 100, true);
    const s = ta.getStats('bash');
    // RED (pre-fix): these fields are undefined.
    assert.equal(s.successCount, 2);
    assert.ok(Math.abs(s.successRate - 2 / 3) < 1e-9);
    assert.ok(Math.abs(s.successRate - (1 - s.errorRate)) < 1e-9);
  });

  it('reports zero success metrics for an unknown tool (no NaN)', () => {
    const s = new ToolAnalytics().getStats('nope');
    assert.equal(s.successCount, 0);
    assert.equal(s.successRate, 0);
  });
});

describe('ToolAnalytics parity — overall aggregate report', () => {
  // GAP: no single cross-tool summary object / totalDurationMs accessor.
  it('getOverallStats aggregates across every tool', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false, { requestTokens: 5, resultTokens: 50 });
    ta.record('grep', 200, true, { requestTokens: 3, resultTokens: 30 });
    ta.record('grep', 300, false, { requestTokens: 2, resultTokens: 20 });
    const o = ta.getOverallStats();
    // RED (pre-fix): getOverallStats is not a function.
    assert.equal(o.totalCalls, 3);
    assert.equal(o.totalErrors, 1);
    assert.equal(o.totalSuccesses, 2);
    assert.equal(o.totalDurationMs, 600);
    assert.equal(o.avgDurationMs, 200);
    assert.ok(Math.abs(o.errorRate - 1 / 3) < 1e-9);
    assert.ok(Math.abs(o.successRate - 2 / 3) < 1e-9);
    assert.equal(o.totalRequestTokens, 10);
    assert.equal(o.totalResultTokens, 100);
    assert.equal(o.distinctTools, 2);
  });

  it('getOverallStats + totalDurationMs are zero/NaN-free when empty', () => {
    const ta = new ToolAnalytics();
    const o = ta.getOverallStats();
    assert.equal(o.totalCalls, 0);
    assert.equal(o.avgDurationMs, 0);
    assert.equal(o.errorRate, 0);
    assert.equal(o.successRate, 0);
    assert.equal(o.distinctTools, 0);
    // RED (pre-fix): totalDurationMs accessor does not exist.
    assert.equal(ta.totalDurationMs, 0);
  });

  it('totalDurationMs sums wall-clock across tools', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('grep', 250, false);
    assert.equal(ta.totalDurationMs, 350);
  });
});

describe('ToolAnalytics parity — cross-agent merge', () => {
  // GAP: no way to roll sub-agent analytics into a parent aggregate.
  it('merge folds another instance in, combining counts/durations/tokens', () => {
    const parent = new ToolAnalytics();
    parent.record('bash', 100, false, { resultTokens: 40 });
    const child = new ToolAnalytics();
    child.record('bash', 300, true, { resultTokens: 60 });
    child.record('Grep', 50, false);
    // RED (pre-fix): merge is not a function.
    parent.merge(child);
    const bash = parent.getStats('bash');
    assert.equal(bash.callCount, 2);
    assert.equal(bash.errorCount, 1);
    assert.equal(bash.totalDurationMs, 400);
    assert.equal(bash.minDurationMs, 100);
    assert.equal(bash.maxDurationMs, 300);
    assert.equal(bash.resultTokens, 100);
    // Child-only tool (normalized) appears in parent.
    assert.equal(parent.getStats('grep').callCount, 1);
    assert.equal(parent.totalCalls, 3);
  });

  it('merge re-observes latency samples so percentiles stay representative', () => {
    const a = new ToolAnalytics();
    const b = new ToolAnalytics();
    for (let i = 1; i <= 50; i++) a.record('bash', i, false);
    for (let i = 51; i <= 100; i++) b.record('bash', i, false);
    a.merge(b);
    const s = a.getStats('bash');
    assert.equal(s.callCount, 100);
    assert.ok(s.p95DurationMs >= 90 && s.p95DurationMs <= 100);
  });
});

describe('ToolAnalytics parity — serializable snapshot', () => {
  // GAP: no single serializable point-in-time view (overall + per-tool).
  it('snapshot returns a JSON-serializable overall+tools view', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('grep', 50, true);
    const snap = ta.snapshot();
    // RED (pre-fix): snapshot is not a function.
    assert.equal(snap.overall.totalCalls, 2);
    assert.equal(snap.tools.length, 2);
    // Round-trips through JSON without loss.
    const round = JSON.parse(JSON.stringify(snap));
    assert.equal(round.overall.totalErrors, 1);
    assert.equal(round.tools.length, 2);
  });
});
