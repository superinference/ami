import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { AnalyticsTracker, generateToolSummary } from '../src/analytics';

// ---------------------------------------------------------------------------
// AnalyticsTracker — in-memory behaviour
// ---------------------------------------------------------------------------
describe('AnalyticsTracker', () => {
  it('log() adds event with timestamp and type', () => {
    const tracker = new AnalyticsTracker();
    const before = new Date().toISOString();
    tracker.log('test_event', { key: 'value' });
    const after = new Date().toISOString();

    const events = tracker.getEvents();
    assert.equal(events.length, 1);
    assert.equal(events[0].type, 'test_event');
    assert.deepEqual(events[0].data, { key: 'value' });
    // Timestamp should be between before and after
    assert.ok(events[0].timestamp >= before);
    assert.ok(events[0].timestamp <= after);
  });

  it('getEvents() returns all events', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('a');
    tracker.log('b');
    tracker.log('c');

    const events = tracker.getEvents();
    assert.equal(events.length, 3);
    assert.deepEqual(
      events.map(e => e.type),
      ['a', 'b', 'c'],
    );
  });

  it('getEvents() returns a defensive copy', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('original');

    const events = tracker.getEvents();
    events.push({ timestamp: '', type: 'injected', data: {} });

    // The tracker's internal list should be unaffected
    assert.equal(tracker.getEvents().length, 1);
  });

  it('getEventsByType() filters correctly', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('tool_call', { tool: 'readFile' });
    tracker.log('api_request', { model: 'gpt-4o' });
    tracker.log('tool_call', { tool: 'writeFile' });
    tracker.log('error', { msg: 'fail' });

    const toolCalls = tracker.getEventsByType('tool_call');
    assert.equal(toolCalls.length, 2);
    assert.ok(toolCalls.every(e => e.type === 'tool_call'));

    const errors = tracker.getEventsByType('error');
    assert.equal(errors.length, 1);

    const missing = tracker.getEventsByType('nonexistent');
    assert.equal(missing.length, 0);
  });

  it('getSummary() returns event counts by type', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('tool_call');
    tracker.log('tool_call');
    tracker.log('api_request');
    tracker.log('error');
    tracker.log('error');
    tracker.log('error');

    const summary = tracker.getSummary();
    assert.deepEqual(summary, {
      tool_call: 2,
      api_request: 1,
      error: 3,
    });
  });

  it('empty tracker returns empty summary', () => {
    const tracker = new AnalyticsTracker();
    const summary = tracker.getSummary();
    assert.deepEqual(summary, {});
    assert.equal(tracker.getEvents().length, 0);
  });

  it('reset() clears all events', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('a');
    tracker.log('b');
    assert.equal(tracker.getEvents().length, 2);

    tracker.reset();
    assert.equal(tracker.getEvents().length, 0);
    assert.deepEqual(tracker.getSummary(), {});
  });

  it('multiple event types logged in sequence preserve order', () => {
    const tracker = new AnalyticsTracker();
    const types = ['start', 'tool_call', 'api_request', 'tool_call', 'end'];
    for (const t of types) {
      tracker.log(t);
    }

    assert.deepEqual(
      tracker.getEvents().map(e => e.type),
      types,
    );
  });

  it('event data is preserved correctly', () => {
    const tracker = new AnalyticsTracker();
    const data = { nested: { deep: true }, count: 42, list: [1, 2, 3] };
    tracker.log('complex', data);

    const event = tracker.getEvents()[0];
    assert.deepEqual(event.data, data);
  });

  it('log() with no data defaults to empty object', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('bare');

    const event = tracker.getEvents()[0];
    assert.deepEqual(event.data, {});
  });
});

// ---------------------------------------------------------------------------
// AnalyticsTracker — JSONL file persistence
// ---------------------------------------------------------------------------
describe('AnalyticsTracker file persistence', () => {
  let tmpDir: string;

  afterEach(() => {
    // Clean up temp directory
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('writes events to JSONL file in logDir', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'analytics-test-'));
    const tracker = new AnalyticsTracker(tmpDir);

    tracker.log('event_one', { k: 1 });
    tracker.log('event_two', { k: 2 });

    const logPath = path.join(tmpDir, 'analytics.jsonl');
    assert.ok(fs.existsSync(logPath), 'JSONL file should exist');

    const lines = fs.readFileSync(logPath, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 2);

    const first = JSON.parse(lines[0]);
    assert.equal(first.type, 'event_one');
    assert.deepEqual(first.data, { k: 1 });
    assert.ok(typeof first.timestamp === 'string');

    const second = JSON.parse(lines[1]);
    assert.equal(second.type, 'event_two');
    assert.deepEqual(second.data, { k: 2 });
  });

  it('creates nested logDir if it does not exist', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'analytics-test-'));
    const nestedDir = path.join(tmpDir, 'sub', 'dir');
    const tracker = new AnalyticsTracker(nestedDir);

    tracker.log('nested_test');

    const logPath = path.join(nestedDir, 'analytics.jsonl');
    assert.ok(fs.existsSync(logPath));
  });

  it('does not write file when no logDir is provided', () => {
    const tracker = new AnalyticsTracker();
    tracker.log('no_file');
    // There's no file to check — just verify in-memory works
    assert.equal(tracker.getEvents().length, 1);
  });
});

// ---------------------------------------------------------------------------
// generateToolSummary
// ---------------------------------------------------------------------------
describe('generateToolSummary', () => {
  it('formats a single successful tool result', () => {
    const result = generateToolSummary([
      { toolName: 'readFile', output: 'File contents here', isError: false },
    ]);
    assert.equal(result, 'readFile: OK — File contents here');
  });

  it('formats a single error tool result', () => {
    const result = generateToolSummary([
      { toolName: 'writeFile', output: 'Permission denied', isError: true },
    ]);
    assert.equal(result, 'writeFile: ERROR — Permission denied');
  });

  it('joins multiple results with newlines', () => {
    const result = generateToolSummary([
      { toolName: 'a', output: 'ok', isError: false },
      { toolName: 'b', output: 'fail', isError: true },
    ]);
    const lines = result.split('\n');
    assert.equal(lines.length, 2);
    assert.ok(lines[0].includes('a: OK'));
    assert.ok(lines[1].includes('b: ERROR'));
  });

  it('truncates output to first line and 100 chars', () => {
    const longOutput = 'X'.repeat(200) + '\nSecond line';
    const result = generateToolSummary([
      { toolName: 'tool', output: longOutput, isError: false },
    ]);
    // First line of longOutput is 200 chars, but only first 100 should appear
    assert.ok(result.includes('X'.repeat(100)));
    assert.ok(!result.includes('X'.repeat(101)));
    assert.ok(!result.includes('Second line'));
  });

  it('returns empty string for empty input', () => {
    assert.equal(generateToolSummary([]), '');
  });
});
