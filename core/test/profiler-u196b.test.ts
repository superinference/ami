/**
 * U196 (batch b) parity additions to the Profiler. Each block maps to a
 * confirmed parity gap against the reference's telemetry/profiler surface:
 *
 *  - timeAsync lifecycle + getData extractor (diagLogs withDiagnosticsTiming).
 *  - Chrome-Trace / Perfetto export (telemetry/perfettoTracing + sessionTracing).
 *  - Richer per-turn span params: isSpeculative, querySource, messageId,
 *    requestSetupMs, attemptStartTimes (perfettoTracing startLLMRequestPerfettoSpan).
 *  - Named query-phase latency fields: queryOverheadMs, timeToSystemMessageMs,
 *    timeToQueryStartMs, headersReceivedMs (headlessProfiler / queryProfiler).
 *  - Turn-0 cold-start / process-start anchor (headlessProfiler time_to_* fields).
 *  - Bounded checkpoint retention (perfettoTracing MAX_EVENTS cap).
 *  - TTL sweep of abandoned turn-starts (sessionTracing orphaned-span cleanup).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler } from '../src/profiler';
import type { TraceEvent, TimingLifecycleEvent } from '../src/profiler';

describe('Profiler.timeAsync lifecycle + getData (diagLogs parity)', () => {
  it('emits started → completed with duration + extracted data on success', async () => {
    const p = new Profiler();
    const events: TimingLifecycleEvent[] = [];
    const result = await p.timeAsync('git_status', async () => ({ files: 3 }), {
      getData: (r) => ({ changed: r.files }),
      onLifecycle: (ev) => events.push(ev),
    });
    assert.deepEqual(result, { files: 3 });
    assert.equal(events.length, 2);
    assert.equal(events[0].label, 'git_status');
    assert.equal(events[0].phase, 'started');
    assert.equal(events[1].phase, 'completed');
    assert.ok(events[1].durationMs !== undefined && events[1].durationMs >= 0);
    assert.deepEqual(events[1].data, { changed: 3 });
    // Still records the timing in the normal store.
    assert.equal(p.getStatistics('git_status')?.count, 1);
  });

  it('emits started → failed with duration, bumps failed counter, re-throws', async () => {
    const p = new Profiler();
    const events: TimingLifecycleEvent[] = [];
    await assert.rejects(
      p.timeAsync('risky', async () => { throw new Error('boom'); }, { onLifecycle: (ev) => events.push(ev) }),
      /boom/,
    );
    assert.deepEqual(events.map(e => e.phase), ['started', 'failed']);
    assert.ok(events[1].durationMs !== undefined && events[1].durationMs >= 0);
    assert.equal(p.getCounter('risky.failed'), 1);
    assert.equal(p.getStatistics('risky')?.count, 1);
  });

  it('works without opts (unchanged behaviour for existing callers)', async () => {
    const p = new Profiler();
    const r = await p.timeAsync('plain', async () => 42);
    assert.equal(r, 42);
    assert.equal(p.getStatistics('plain')?.count, 1);
  });
});

describe('Profiler Chrome-Trace / Perfetto export', () => {
  const mem = () => ({ rss: 0, heapUsed: 0 });

  it('returns [] / empty traceEvents when nothing was recorded', () => {
    const p = new Profiler({ memSnapshot: mem });
    assert.deepEqual(p.toTraceEvents(), []);
    const parsed = JSON.parse(p.toTraceJson());
    assert.deepEqual(parsed.traceEvents, []);
    assert.equal(parsed.displayTimeUnit, 'ms');
  });

  it('emits an instant event per checkpoint with microsecond timestamps', () => {
    const p = new Profiler({ memSnapshot: mem });
    p.mark('a');
    p.mark('b');
    const evs = p.toTraceEvents();
    const instants = evs.filter((e: TraceEvent) => e.ph === 'i');
    assert.deepEqual(instants.map(e => e.name), ['a', 'b']);
    // First checkpoint anchors the trace at ts 0 (microseconds).
    assert.equal(instants[0].ts, 0);
    assert.ok(instants.every(e => Number.isFinite(e.ts) && e.ts >= 0));
    assert.ok(instants.every(e => typeof e.pid === 'number' && typeof e.tid === 'number'));
  });

  it('emits a complete (X) event per turn with duration in microseconds + args', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 200, toolExecutionTime: 10, compactionTime: 0,
      tokensProduced: 50, tokensConsumed: 100, ttftMs: 80,
      isSpeculative: true, querySource: 'user',
    });
    const evs = p.toTraceEvents();
    const turnEv = evs.find((e: TraceEvent) => e.ph === 'X' && e.name.startsWith('turn'));
    assert.ok(turnEv, 'expected a turn complete event');
    assert.ok(turnEv!.dur !== undefined && turnEv!.dur >= 0);
    assert.equal(turnEv!.args?.is_speculative, true);
    assert.equal(turnEv!.args?.query_source, 'user');
  });

  it('toTraceJson round-trips to a Chrome-Trace object', () => {
    const p = new Profiler({ memSnapshot: mem });
    p.mark('x');
    const parsed = JSON.parse(p.toTraceJson());
    assert.ok(Array.isArray(parsed.traceEvents));
    assert.equal(parsed.traceEvents[0].name, 'x');
  });
});

describe('Profiler richer per-turn span params + query-phase fields', () => {
  it('stores speculative/source/messageId/requestSetup/attempts + query-phase + headers', () => {
    const p = new Profiler();
    p.beginTurn(1);
    p.endTurn(1, {
      apiLatency: 500, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 10, tokensConsumed: 20, ttftMs: 100,
      isSpeculative: true,
      querySource: 'speculation',
      messageId: 'msg_abc',
      requestSetupMs: 12,
      attemptStartTimes: [0, 250],
      queryOverheadMs: 7,
      timeToSystemMessageMs: 33,
      timeToQueryStartMs: 15,
      headersReceivedMs: 60,
    });
    const t = p.getTurn(1)!;
    assert.equal(t.isSpeculative, true);
    assert.equal(t.querySource, 'speculation');
    assert.equal(t.messageId, 'msg_abc');
    assert.equal(t.requestSetupMs, 12);
    assert.deepEqual(t.attemptStartTimes, [0, 250]);
    assert.equal(t.queryOverheadMs, 7);
    assert.equal(t.timeToSystemMessageMs, 33);
    assert.equal(t.timeToQueryStartMs, 15);
    assert.equal(t.headersReceivedMs, 60);
  });

  it('defaults the new fields when omitted and clamps negatives to 0', () => {
    const p = new Profiler();
    p.beginTurn(2);
    p.endTurn(2, {
      apiLatency: 100, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 1, tokensConsumed: 1, ttftMs: 10,
      requestSetupMs: -5, queryOverheadMs: -9, headersReceivedMs: -1,
      timeToSystemMessageMs: -2, timeToQueryStartMs: -3,
    });
    const t = p.getTurn(2)!;
    assert.equal(t.isSpeculative, false);
    assert.equal(t.querySource, '');
    assert.equal(t.messageId, '');
    assert.equal(t.requestSetupMs, 0);
    assert.deepEqual(t.attemptStartTimes, []);
    assert.equal(t.queryOverheadMs, 0);
    assert.equal(t.timeToSystemMessageMs, 0);
    assert.equal(t.timeToQueryStartMs, 0);
    assert.equal(t.headersReceivedMs, 0);
  });

  it('copies the new fields as defaults onto turns finalized by closeOpenTurns', () => {
    const p = new Profiler();
    p.beginTurn(9);
    assert.equal(p.closeOpenTurns(), 1);
    const t = p.getTurn(9)!;
    assert.equal(t.incomplete, true);
    assert.equal(t.isSpeculative, false);
    assert.equal(t.requestSetupMs, 0);
    assert.deepEqual(t.attemptStartTimes, []);
    assert.equal(t.headersReceivedMs, 0);
  });
});

describe('Profiler cold-start / process-start anchor', () => {
  it('timeSinceProcessStart is a non-negative monotonic reading', () => {
    const p = new Profiler();
    const a = p.timeSinceProcessStart();
    const b = p.timeSinceProcessStart();
    assert.ok(a >= 0 && b >= a);
  });

  it('recordColdStart records the elapsed-since-start under the given label', () => {
    const p = new Profiler();
    const ms = p.recordColdStart();
    assert.ok(ms >= 0);
    assert.equal(p.getStatistics('cold_start')?.count, 1);
    const custom = p.recordColdStart('turn0_ready');
    assert.ok(custom >= 0);
    assert.equal(p.getStatistics('turn0_ready')?.count, 1);
  });
});

describe('Profiler bounded checkpoint retention', () => {
  const mem = () => ({ rss: 0, heapUsed: 0 });
  it('evicts the oldest checkpoints past maxCheckpoints', () => {
    const p = new Profiler({ memSnapshot: mem, maxCheckpoints: 3 });
    for (const l of ['a', 'b', 'c', 'd', 'e']) p.mark(l);
    const labels = p.getTimeline().map(e => e.label);
    assert.deepEqual(labels, ['c', 'd', 'e']);
  });
  it('defaults to a large cap that does not evict in normal use', () => {
    const p = new Profiler({ memSnapshot: mem });
    for (let i = 0; i < 50; i++) p.mark(`m${i}`);
    assert.equal(p.getTimeline().length, 50);
  });
});

describe('Profiler TTL sweep of abandoned turn-starts', () => {
  it('evicts started-but-never-ended turns older than maxAgeMs', () => {
    const p = new Profiler();
    p.beginTurn(0);
    const first = Date.now();
    while (Date.now() - first < 5) { /* spin so the next start is strictly later */ }
    p.beginTurn(1);
    // A 1ms window evicts the older abandoned start but keeps the fresh one.
    const dropped = p.sweepStaleTurns(1);
    assert.equal(dropped, 1);
    // Ending turn 0 now yields an incomplete turn (its start was swept).
    p.endTurn(0, { apiLatency: 0, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 0, tokensConsumed: 0, ttftMs: 0 });
    assert.equal(p.getTurn(0)?.incomplete, true);
  });

  it('never evicts a start that already ended normally, and treats negative age as no-op', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, { apiLatency: 1, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 0, tokensConsumed: 0, ttftMs: 0 });
    assert.equal(p.sweepStaleTurns(-1), 0);
    p.beginTurn(1);
    assert.equal(p.sweepStaleTurns(60_000), 0, 'fresh start within the window is kept');
  });
});
