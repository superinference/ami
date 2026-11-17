import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MemoryGate,
  MAX_CONTEXT_LINES,
  MAX_PERSIST_LINES,
  PER_ENTRY_CONTEXT_CHARS,
  PER_ENTRY_PERSIST_CHARS,
  STALENESS_DAYS,
} from '../src/superinference/memory-gate';
import { SuperInferenceEngine } from '../src/superinference/index';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';
import type { CriticDecision } from '../src/superinference/types';

const approve = (score: number): CriticDecision => ({ approved: true, score });
const DAY_MS = 1000 * 60 * 60 * 24;

// ---------------------------------------------------------------------------
// Gap: deduplication + growth bound (findRelevantMemories parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – dedup + growth bound', () => {
  it('deduplicates by query, keeping the higher-scored observation', () => {
    const gate = new MemoryGate();
    gate.gate('same-q', 'first', approve(0.5), 0.5, 1);
    gate.gate('same-q', 'second (better)', approve(0.9), 0.6, 2);
    const entries = gate.getEntries();
    assert.equal(entries.length, 1, 'repeated query must not grow the store');
    assert.equal(entries[0].criticScore, 0.9);
    assert.equal(entries[0].result, 'second (better)');
  });

  it('keeps the existing entry when a repeated query scores equal or lower', () => {
    const gate = new MemoryGate();
    gate.gate('same-q', 'best', approve(0.9), 0.5, 1);
    gate.gate('same-q', 'worse', approve(0.3), 0.6, 2);
    gate.gate('same-q', 'tie', approve(0.9), 0.6, 3);
    const entries = gate.getEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].result, 'best', 'lower/equal score must not displace the better entry');
  });

  it('evicts the lowest-scored entry (oldest on a tie) past maxEntries', () => {
    const gate = new MemoryGate({ maxEntries: 3 });
    gate.gate('q1', 'r1', approve(0.9), 0.5, 1);
    gate.gate('q2', 'r2', approve(0.5), 0.5, 2); // lowest — should be evicted
    gate.gate('q3', 'r3', approve(0.8), 0.5, 3);
    gate.gate('q4', 'r4', approve(0.7), 0.5, 4); // pushes over cap
    const queries = gate.getEntries().map((e) => e.query);
    assert.equal(queries.length, 3);
    assert.ok(!queries.includes('q2'), 'the lowest-scored entry (q2) must be evicted');
    assert.deepEqual(queries, ['q1', 'q3', 'q4']);
  });
});

// ---------------------------------------------------------------------------
// Gap: relevance ranking + top-N emit budget (findRelevantMemories parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – ranked, budgeted emit', () => {
  it('emits approved context ranked by critic score (descending)', () => {
    const gate = new MemoryGate();
    gate.gate('low', 'rl', approve(0.3), 0.5, 1);
    gate.gate('high', 'rh', approve(0.9), 0.5, 2);
    gate.gate('mid', 'rm', approve(0.6), 0.5, 3);
    const ctx = gate.getApprovedContext();
    assert.ok(ctx.indexOf('high') < ctx.indexOf('mid'), 'high before mid');
    assert.ok(ctx.indexOf('mid') < ctx.indexOf('low'), 'mid before low');
  });

  it('caps the number of emitted entries at maxEmitEntries (top-N)', () => {
    const gate = new MemoryGate({ maxEmitEntries: 2 });
    gate.gate('a', 'ra', approve(0.9), 0.5, 1);
    gate.gate('b', 'rb', approve(0.8), 0.5, 2);
    gate.gate('c', 'rc', approve(0.7), 0.5, 3); // lowest — dropped from the top-2 budget
    const ctx = gate.getApprovedContext();
    assert.ok(ctx.includes('Q: a') && ctx.includes('Q: b'));
    assert.ok(!ctx.includes('Q: c'), 'entry outside the top-N budget must not be emitted');
  });

  it('ranks persisted content by score too', () => {
    const gate = new MemoryGate();
    gate.gate('low-q', 'x', approve(0.2), 0.5, 1);
    gate.gate('high-q', 'y', approve(0.95), 0.5, 2);
    let saved = '';
    gate.persistTo({ saveMemory(_p: string, c: string) { saved = c; } } as never, 'task');
    assert.ok(saved.indexOf('high-q') < saved.indexOf('low-q'));
  });
});

// ---------------------------------------------------------------------------
// Gap: line boundary cap (memdir MAX_ENTRYPOINT_LINES parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – line boundary cap', () => {
  it('exposes default line caps', () => {
    assert.equal(MAX_CONTEXT_LINES, 200);
    assert.equal(MAX_PERSIST_LINES, 200);
  });

  it('truncates getApprovedContext on a line boundary with a line sentinel', () => {
    const gate = new MemoryGate({ maxContextLines: 4 });
    gate.gate('q1', 'r1', approve(0.9), 0.5, 1);
    gate.gate('q2', 'r2', approve(0.9), 0.5, 2);
    gate.gate('q3', 'r3', approve(0.9), 0.5, 3); // 8 joined lines > 4
    const ctx = gate.getApprovedContext();
    assert.match(ctx, /… \[truncated: \d+ more lines omitted\]/);
    assert.ok(ctx.split('\n').length <= 5, 'kept lines + one sentinel line');
  });

  it('truncates persistTo on a line boundary', () => {
    const gate = new MemoryGate({ maxPersistLines: 3 });
    for (let i = 0; i < 5; i++) gate.gate(`q${i}`, `r${i}`, approve(0.9), 0.5, i);
    let saved = '';
    gate.persistTo({ saveMemory(_p: string, c: string) { saved = c; } } as never, 'task');
    assert.match(saved, /… \[truncated: \d+ more lines omitted\]/);
  });
});

// ---------------------------------------------------------------------------
// Gap: error-handling fallback around persistence I/O (memdir parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – persistence I/O fault tolerance', () => {
  beforeEach(() => { __resetLoggerForTest(); });

  it('does not throw, and records the failure, when saveMemory raises', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', approve(0.9), 0.5, 1);
    const err = Object.assign(new Error('disk full'), { code: 'ENOSPC' });
    assert.doesNotThrow(() =>
      gate.persistTo({ saveMemory() { throw err; } } as never, 'task'),
    );
    const status = gate.getConsolidationStatus();
    assert.equal(status.lastConsolidatedAt, null, 'failed write must not record a consolidation');
    assert.equal(status.state, 'never');
    assert.ok(status.lastError && status.lastError.includes('ENOSPC'));
    assert.ok(status.lastError!.includes('disk full'));
    // The failure is captured for diagnostics introspection.
    assert.ok(getInMemoryErrors().some((e) => e.error.includes('memory-gate')));
  });

  it('clears a prior error after a later successful write', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', approve(0.9), 0.5, 1);
    gate.persistTo({ saveMemory() { throw new Error('boom'); } } as never, 'task');
    assert.ok(gate.getConsolidationStatus().lastError !== null);
    gate.persistTo({ saveMemory() {} } as never, 'task');
    const status = gate.getConsolidationStatus();
    assert.equal(status.lastError, null);
    assert.equal(status.state, 'idle');
  });
});

// ---------------------------------------------------------------------------
// Gap: age/freshness caveat derived from stored timestamp (memoryAge parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – staleness caveat', () => {
  it('exposes the default staleness threshold', () => {
    assert.equal(STALENESS_DAYS, 7);
  });

  it('appends a staleness caveat to entries older than the threshold', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', approve(0.9), 0.5, 1);
    const future = Date.now() + 10 * DAY_MS; // make the fresh entry read as 10 days old
    const ctx = gate.getApprovedContext(future);
    assert.match(ctx, /\[stale: 10d old — point-in-time, verify before relying\]/);
  });

  it('does not annotate fresh entries', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'fresh', approve(0.9), 0.5, 1);
    assert.doesNotMatch(gate.getApprovedContext(), /stale/);
  });

  it('honors a configurable staleness threshold', () => {
    const gate = new MemoryGate({ stalenessDays: 1 });
    gate.gate('q', 'r', approve(0.9), 0.5, 1);
    const twoDays = Date.now() + 2 * DAY_MS;
    assert.match(gate.getApprovedContext(twoDays), /\[stale: 2d old/);
  });

  it('annotates persisted content as well', () => {
    const gate = new MemoryGate({ stalenessDays: 1 });
    gate.gate('q', 'r', approve(0.9), 0.5, 1);
    let saved = '';
    gate.persistTo({ saveMemory(_p: string, c: string) { saved = c; } } as never, 'task', Date.now() + 5 * DAY_MS);
    assert.match(saved, /\[stale: 5d old/);
  });
});

// ---------------------------------------------------------------------------
// Gap: configurable truncation / cap knobs (memoryAge parity)
// ---------------------------------------------------------------------------

describe('MemoryGate – configurable knobs', () => {
  it('exposes default per-entry truncation lengths', () => {
    assert.equal(PER_ENTRY_CONTEXT_CHARS, 500);
    assert.equal(PER_ENTRY_PERSIST_CHARS, 200);
  });

  it('honors a custom per-entry context truncation length', () => {
    const gate = new MemoryGate({ perEntryContextChars: 10 });
    gate.gate('q', 'y'.repeat(50), approve(0.9), 0.5, 1);
    const aLine = gate.getApprovedContext().split('\n').find((l) => l.startsWith('A:'))!;
    assert.ok(aLine.includes('y'.repeat(10)));
    assert.ok(!aLine.includes('y'.repeat(11)));
    assert.match(aLine, /…\[truncated 40 chars\]/);
  });

  it('honors a custom per-entry persist truncation length', () => {
    const gate = new MemoryGate({ perEntryPersistChars: 5 });
    gate.gate('q', 'z'.repeat(20), approve(0.9), 0.5, 1);
    let saved = '';
    gate.persistTo({ saveMemory(_p: string, c: string) { saved = c; } } as never, 'task');
    assert.match(saved, /zzzzz…\[truncated 15 chars\]/);
  });

  it('honors a custom aggregate char cap', () => {
    const gate = new MemoryGate({ maxContextChars: 40 });
    for (let i = 0; i < 10; i++) gate.gate(`q${i}`, 'r'.repeat(30), approve(0.9), 0.5, i);
    const ctx = gate.getApprovedContext();
    assert.ok(ctx.length <= 40 + 60, `expected capped output, got ${ctx.length}`);
    assert.match(ctx, /… \[truncated: \d+ more chars omitted\]/);
  });
});

// ---------------------------------------------------------------------------
// End-to-end wiring: SuperInferenceConfig knobs reach the gate
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – memory-gate config wiring', () => {
  it('threads memoryMaxEntries from config into the gate ring', () => {
    const engine = new SuperInferenceEngine({ enabled: true, memoryMaxEntries: 2 });
    for (let i = 0; i < 5; i++) {
      engine.memoryGate.gate(`q${i}`, `r${i}`, approve(0.9), 0.5, i);
    }
    assert.equal(engine.memoryGate.getEntries().length, 2);
  });

  it('threads memoryStalenessDays from config into the gate', () => {
    const engine = new SuperInferenceEngine({ enabled: true, memoryStalenessDays: 1 });
    engine.memoryGate.gate('q', 'r', approve(0.9), 0.5, 1);
    assert.match(engine.memoryGate.getApprovedContext(Date.now() + 3 * DAY_MS), /\[stale: 3d old/);
  });

  it('threads memoryMaxEmitEntries from config into the gate', () => {
    const engine = new SuperInferenceEngine({ enabled: true, memoryMaxEmitEntries: 1 });
    engine.memoryGate.gate('keep', 'rk', approve(0.9), 0.5, 1);
    engine.memoryGate.gate('drop', 'rd', approve(0.1), 0.5, 2);
    const ctx = engine.memoryGate.getApprovedContext();
    assert.ok(ctx.includes('Q: keep'));
    assert.ok(!ctx.includes('Q: drop'));
  });
});
