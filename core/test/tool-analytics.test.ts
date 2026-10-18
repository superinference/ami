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
