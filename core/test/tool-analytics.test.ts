import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { ToolAnalytics } from '../src/tool-analytics';

describe('ToolAnalytics', () => {
  it('tracks a single tool call', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 150, false);
    const stats = ta.getStats('bash');
    assert.equal(stats.callCount, 1);
    assert.equal(stats.errorCount, 0);
    assert.equal(stats.totalDurationMs, 150);
    assert.equal(stats.avgDurationMs, 150);
  });

  it('tracks multiple calls to the same tool', () => {
    const ta = new ToolAnalytics();
    ta.record('file_read', 100, false);
    ta.record('file_read', 200, false);
    ta.record('file_read', 300, true);
    const stats = ta.getStats('file_read');
    assert.equal(stats.callCount, 3);
    assert.equal(stats.errorCount, 1);
    assert.equal(stats.totalDurationMs, 600);
    assert.equal(stats.avgDurationMs, 200);
    assert.ok(Math.abs(stats.errorRate - 1 / 3) < 0.01);
  });

  it('tracks different tools independently', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('grep', 50, false);
    assert.equal(ta.getStats('bash').callCount, 1);
    assert.equal(ta.getStats('grep').callCount, 1);
  });

  it('returns zero stats for unknown tools', () => {
    const ta = new ToolAnalytics();
    const stats = ta.getStats('unknown_tool');
    assert.equal(stats.callCount, 0);
    assert.equal(stats.errorCount, 0);
    assert.equal(stats.totalDurationMs, 0);
    assert.equal(stats.avgDurationMs, 0);
    assert.equal(stats.errorRate, 0);
  });

  it('getSummary returns all tracked tools sorted by call count', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('bash', 200, false);
    ta.record('grep', 50, false);
    ta.record('file_read', 100, false);
    ta.record('file_read', 100, false);
    ta.record('file_read', 100, false);
    const summary = ta.getSummary();
    assert.equal(summary.length, 3);
    assert.equal(summary[0].tool, 'file_read');
    assert.equal(summary[0].callCount, 3);
    assert.equal(summary[1].tool, 'bash');
    assert.equal(summary[1].callCount, 2);
  });

  it('tracks max duration', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('bash', 500, false);
    ta.record('bash', 200, false);
    const stats = ta.getStats('bash');
    assert.equal(stats.maxDurationMs, 500);
  });

  it('tracks min duration', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 300, false);
    ta.record('bash', 100, false);
    ta.record('bash', 200, false);
    const stats = ta.getStats('bash');
    assert.equal(stats.minDurationMs, 100);
  });

  it('reset clears all data', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.reset();
    assert.equal(ta.getSummary().length, 0);
    assert.equal(ta.getStats('bash').callCount, 0);
  });

  it('totalCalls returns the sum across all tools', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false);
    ta.record('grep', 50, false);
    ta.record('bash', 200, false);
    assert.equal(ta.totalCalls, 3);
  });

  it('totalErrors returns the sum of errors across all tools', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, true);
    ta.record('grep', 50, false);
    ta.record('bash', 200, true);
    assert.equal(ta.totalErrors, 2);
  });
});

describe('ToolAnalytics — per-tool token attribution', () => {
  it('splits request vs result tokens per tool', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false, { requestTokens: 10, resultTokens: 500 });
    ta.record('bash', 100, false, { requestTokens: 5, resultTokens: 300 });
    const s = ta.getStats('bash');
    assert.equal(s.requestTokens, 15);
    assert.equal(s.resultTokens, 800);
    assert.equal(ta.totalRequestTokens, 15);
    assert.equal(ta.totalResultTokens, 800);
  });

  it('defaults token fields to 0 and ignores negatives', () => {
    const ta = new ToolAnalytics();
    ta.record('grep', 20, false);
    ta.record('grep', 20, false, { requestTokens: -5, resultTokens: -9 });
    const s = ta.getStats('grep');
    assert.equal(s.requestTokens, 0);
    assert.equal(s.resultTokens, 0);
  });
});

describe('ToolAnalytics — tail-latency percentiles', () => {
  it('exposes p50/p95/p99 and resists a single outlier pinning the view', () => {
    const ta = new ToolAnalytics();
    for (let i = 1; i <= 100; i++) ta.record('bash', i, false);
    const s = ta.getStats('bash');
    assert.ok(s.p50DurationMs >= 49 && s.p50DurationMs <= 52);
    assert.ok(s.p95DurationMs >= 94 && s.p95DurationMs <= 96);
    assert.ok(s.p99DurationMs > s.p95DurationMs);
    assert.ok(s.p95DurationMs < s.maxDurationMs);
  });

  it('unknown tool reports zeroed percentiles', () => {
    const s = new ToolAnalytics().getStats('nope');
    assert.equal(s.p50DurationMs, 0);
    assert.equal(s.p99DurationMs, 0);
  });
});

describe('ToolAnalytics — context-bloat hints', () => {
  it('emits targeted advice + savings for tools over the threshold', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false, { resultTokens: 6000 });
    ta.record('read', 100, false, { resultTokens: 500 }); // below 20%
    const hints = ta.getContextHints(10000);
    assert.equal(hints.length, 1);
    assert.equal(hints[0].tool, 'bash');
    assert.ok(hints[0].pctOfTotal >= 0.2);
    // bash savings fraction 0.5 → 3000
    assert.equal(hints[0].estimatedSavingsTokens, 3000);
    assert.match(hints[0].advice, /head\/tail\/grep/);
  });

  it('uses a generic hint for unknown tools over threshold', () => {
    const ta = new ToolAnalytics();
    ta.record('custom_tool', 100, false, { resultTokens: 5000 });
    const hints = ta.getContextHints(10000);
    assert.equal(hints.length, 1);
    assert.equal(hints[0].estimatedSavingsTokens, 1500); // 0.3 generic
    assert.match(hints[0].advice, /custom_tool/);
  });

  it('returns no hints for zero total or no bloated tool', () => {
    const ta = new ToolAnalytics();
    ta.record('bash', 100, false, { resultTokens: 100 });
    assert.deepEqual(ta.getContextHints(0), []);
    assert.deepEqual(ta.getContextHints(10000), []);
  });
});
