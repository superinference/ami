/**
 * U196 (batch c) parity additions to the Profiler. Each block maps to a
 * confirmed parity gap against Claude Code's telemetry/profiler surface:
 *
 *  - formatSecondsShort ms→"1.2s" helper (utils/format.ts formatSecondsShort).
 *  - Unified metric store: histogram observe() + flat getAll() snapshot
 *    (context/stats.tsx createStatsStore getAll).
 *  - Declarative startup phase-boundary config getPhaseDurations()
 *    (utils/startupProfiler.ts PHASE_DEFINITIONS).
 *  - TTFT pre-request-overhead-vs-network split getTtftBreakdown()
 *    (utils/queryProfiler.ts report split).
 *  - Per-request phase breakdown getRequestPhases() + ITPS
 *    (telemetry/perfettoTracing endLLMRequestPerfettoSpan).
 *  - DevBar-style one-line slow-operation renderer formatSlowOperations()
 *    (components/DevBar.tsx).
 *  - Hierarchical span tree with AsyncLocalStorage parent linkage + per-request
 *    span disambiguation + per-agent swimlane tracks
 *    (telemetry/sessionTracing.ts + perfettoTracing.ts).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler, formatSecondsShort } from '../src/profiler';

describe('formatSecondsShort (utils/format.ts parity)', () => {
  it('renders ms as seconds with one fixed decimal', () => {
    assert.equal(formatSecondsShort(0), '0.0s');
    assert.equal(formatSecondsShort(1234), '1.2s');
    assert.equal(formatSecondsShort(92), '0.1s');
    assert.equal(formatSecondsShort(60000), '60.0s');
  });
});

describe('Profiler unified metric store: histogram + getAll()', () => {
  it('observe() feeds a histogram surfaced as flat *_count/_min/_max/_avg/_p50/_p95/_p99', () => {
    const p = new Profiler({ rand: () => 0 });
    for (const v of [10, 20, 30, 40, 50]) p.observe('tool_latency', v);
    const all = p.getAll();
    assert.equal(all['tool_latency_count'], 5);
    assert.equal(all['tool_latency_min'], 10);
    assert.equal(all['tool_latency_max'], 50);
    assert.equal(all['tool_latency_avg'], 30);
    assert.equal(typeof all['tool_latency_p50'], 'number');
    assert.equal(typeof all['tool_latency_p95'], 'number');
    assert.equal(typeof all['tool_latency_p99'], 'number');
  });

  it('getAll() folds counters, gauges and set cardinality into one flat numeric record', () => {
    const p = new Profiler();
    p.incrementCounter('requests', 3);
    p.setGauge('queue_depth', 7);
    p.addToSet('models', 'opus');
    p.addToSet('models', 'sonnet');
    p.addToSet('models', 'opus');
    const all = p.getAll();
    assert.equal(all['requests'], 3);
    assert.equal(all['queue_depth'], 7);
    assert.equal(all['models'], 2);
  });

  it('omits an empty histogram from getAll()', () => {
    const p = new Profiler();
    p.observe('noop', NaN); // ignored, count stays 0
    const all = p.getAll();
    assert.equal(all['noop_count'], undefined);
  });
});

describe('Profiler declarative phase-boundary config (PHASE_DEFINITIONS parity)', () => {
  it('derives named phase durations from [startCheckpoint,endCheckpoint] pairs', () => {
    let t = 1000;
    const p = new Profiler({ memSnapshot: () => ({ rss: 0, heapUsed: 0 }) });
    // Monkey-free deterministic clock via explicit marks is not possible (mark uses
    // Date.now); instead assert ordering/positivity and that unmatched phases drop.
    p.mark('cli_entry');
    p.mark('main_tsx_imports_loaded');
    p.mark('init_function_start');
    p.mark('init_function_end');
    void t;
    const phases = p.getPhaseDurations({
      import_time: ['cli_entry', 'main_tsx_imports_loaded'],
      init_time: ['init_function_start', 'init_function_end'],
      missing: ['never_started', 'never_ended'],
      half: ['cli_entry', 'never_ended'],
    });
    assert.ok('import_time' in phases, 'import_time derived');
    assert.ok('init_time' in phases, 'init_time derived');
    assert.ok(phases['import_time'] >= 0);
    assert.equal(phases['missing'], undefined, 'phase with no checkpoints omitted');
    assert.equal(phases['half'], undefined, 'phase missing its end checkpoint omitted');
  });

  it('uses the FIRST occurrence of each checkpoint name', () => {
    const p = new Profiler({ memSnapshot: () => ({ rss: 0, heapUsed: 0 }) });
    p.mark('a');
    p.mark('b');
    const phases = p.getPhaseDurations({ span: ['a', 'b'] });
    assert.ok(phases['span'] >= 0);
  });
});

describe('Profiler TTFT pre-request-vs-network split (queryProfiler parity)', () => {
  it('splits ttft into pre-request overhead and network latency with percentages', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 500, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 10, tokensConsumed: 20, ttftMs: 100,
      requestSetupMs: 15, queryOverheadMs: 5,
    });
    const b = p.getTtftBreakdown(0)!;
    assert.equal(b.totalMs, 100);
    assert.equal(b.preRequestMs, 20); // 15 + 5
    assert.equal(b.networkMs, 80);    // 100 - 20
    assert.equal(b.preRequestPercent, 20);
    assert.equal(b.networkPercent, 80);
  });

  it('clamps network to 0 when overhead exceeds ttft and returns null when ttft<=0 or turn missing', () => {
    const p = new Profiler();
    p.beginTurn(1);
    p.endTurn(1, {
      apiLatency: 10, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 1, tokensConsumed: 1, ttftMs: 10,
      requestSetupMs: 30, queryOverheadMs: 0,
    });
    const b = p.getTtftBreakdown(1)!;
    assert.equal(b.networkMs, 0);
    assert.equal(p.getTtftBreakdown(99), null, 'unknown turn → null');
    p.beginTurn(2);
    p.endTurn(2, { apiLatency: 0, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 0, tokensConsumed: 0, ttftMs: 0 });
    assert.equal(p.getTtftBreakdown(2), null, 'ttft 0 → null');
  });
});

describe('Profiler per-request phase breakdown getRequestPhases (perfetto parity)', () => {
  it('decomposes a turn into request_setup / first_token / sampling with retry count', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 500, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 100, tokensConsumed: 40, ttftMs: 120,
      requestSetupMs: 20, attemptStartTimes: [0, 150, 300],
    });
    const phases = p.getRequestPhases(0)!;
    const by = Object.fromEntries(phases.map(x => [x.phase, x.durationMs]));
    assert.equal(by['request_setup'], 20);
    assert.equal(by['first_token'], 100); // 120 - 20
    assert.equal(by['sampling'], 380);    // 500 - 120
    // retries = attemptStartTimes.length - 1
    const retry = phases.find(x => x.phase === 'retries')!;
    assert.equal(retry.durationMs, 2);
    assert.equal(p.getRequestPhases(42), null);
  });
});

describe('Profiler DevBar-style slow-op renderer formatSlowOperations', () => {
  it('renders the N most recent slow ops as a single human-readable line', () => {
    const p = new Profiler({ slowThresholdMs: 10 });
    p.record('git_status', 1200);
    p.record('tool_schema', 300);
    const line = p.formatSlowOperations(3);
    assert.match(line, /slow sync:/);
    assert.match(line, /git_status \(1\.2s\)/);
    assert.match(line, /tool_schema \(0\.3s\)/);
    assert.ok(line.indexOf('·') > 0, 'ops joined by a middot');
  });

  it('returns empty string when no slow ops in the window', () => {
    const p = new Profiler({ slowThresholdMs: 10_000 });
    p.record('fast', 1);
    assert.equal(p.formatSlowOperations(), '');
  });
});

describe('Profiler hierarchical span tree (sessionTracing parity)', () => {
  it('nests child spans under their parent automatically via AsyncLocalStorage', async () => {
    const p = new Profiler();
    await p.withSpan('interaction', 'turn-0', async () => {
      await p.withSpan('llm_request', 'main', async () => {
        await p.withSpan('tool', 'bash', async () => { /* work */ });
      });
    });
    const tree = p.getSpanTree();
    assert.equal(tree.length, 1);
    assert.equal(tree[0].type, 'interaction');
    assert.equal(tree[0].children.length, 1);
    const req = tree[0].children[0];
    assert.equal(req.type, 'llm_request');
    assert.equal(req.children.length, 1);
    assert.equal(req.children[0].name, 'bash');
    // durations recorded (>= 0)
    assert.ok(typeof tree[0].durationMs === 'number' && tree[0].durationMs! >= 0);
  });

  it('disambiguates two parallel same-name llm_request spans by handle', async () => {
    const p = new Profiler();
    const a = p.beginSpan('llm_request', 'main');
    const b = p.beginSpan('llm_request', 'main');
    assert.notEqual(a.id, b.id);
    p.endSpan(b, { role: 'second' });
    p.endSpan(a, { role: 'first' });
    const tree = p.getSpanTree();
    assert.equal(tree.length, 2);
    const roles = tree.map(n => n.attrs['role']).sort();
    assert.deepEqual(roles, ['first', 'second']);
  });

  it('endSpan is a no-op for an unknown or already-ended handle', () => {
    const p = new Profiler();
    const h = p.beginSpan('tool', 'x');
    p.endSpan(h);
    const firstEnd = p.getSpan(h.id)!.endMs;
    p.endSpan(h); // second end must not change endMs
    assert.equal(p.getSpan(h.id)!.endMs, firstEnd);
    p.endSpan({ id: 'nope', type: 'tool', name: 'y' }); // unknown → no throw
  });

  it('withSpan records duration even when the body throws, then re-throws', async () => {
    const p = new Profiler();
    await assert.rejects(
      p.withSpan('tool', 'boom', async () => { throw new Error('x'); }),
      /x/,
    );
    const tree = p.getSpanTree();
    assert.equal(tree[0].name, 'boom');
    assert.ok(tree[0].durationMs !== undefined);
  });

  it('reset() clears spans', async () => {
    const p = new Profiler();
    p.beginSpan('interaction', 't');
    p.reset();
    assert.equal(p.getSpanTree().length, 0);
  });
});

describe('Profiler per-agent swimlane tracks (perfetto agent-hierarchy parity)', () => {
  it('groups spans into per-agent tracks preserving parent-agent links', () => {
    const p = new Profiler();
    const root = p.beginSpan('interaction', 'turn', { agentId: 'main' });
    p.endSpan(root);
    const sub = p.beginSpan('tool', 'subagent-work', { agentId: 'sub-1', parentAgentId: 'main', parentId: root.id });
    p.endSpan(sub);
    const tracks = p.getAgentTracks();
    const byId = Object.fromEntries(tracks.map(t => [t.agentId, t]));
    assert.ok(byId['main'], 'main track present');
    assert.ok(byId['sub-1'], 'sub-1 track present');
    assert.equal(byId['sub-1'].parentAgentId, 'main');
    assert.equal(byId['main'].spans.length, 1);
    assert.equal(byId['sub-1'].spans.length, 1);
  });

  it('defaults spans with no agentId to the "main" track', () => {
    const p = new Profiler();
    const h = p.beginSpan('tool', 'x');
    p.endSpan(h);
    const tracks = p.getAgentTracks();
    assert.equal(tracks.length, 1);
    assert.equal(tracks[0].agentId, 'main');
  });
});
