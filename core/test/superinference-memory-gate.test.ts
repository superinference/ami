import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MemoryGate,
  MAX_ENTRIES,
  MAX_CONTEXT_CHARS,
  MAX_PERSIST_CHARS,
} from '../src/superinference/memory-gate';
import type { CriticDecision } from '../src/superinference/types';

// ---------------------------------------------------------------------------
// gate() — Eq. 5
// ---------------------------------------------------------------------------

describe('MemoryGate – gate()', () => {
  let gate: MemoryGate;

  beforeEach(() => {
    gate = new MemoryGate();
  });

  it('adds entry when decision is approved', () => {
    const decision: CriticDecision = { approved: true, score: 0.85 };
    const result = gate.gate('query1', 'result1', decision, 0.5, 1);
    assert.equal(result, true);
    assert.equal(gate.getEntries().length, 1);
  });

  it('does not add entry when decision is rejected', () => {
    const decision: CriticDecision = { approved: false, score: 0.2 };
    const result = gate.gate('query1', 'result1', decision, 0.5, 1);
    assert.equal(result, false);
    assert.equal(gate.getEntries().length, 0);
  });

  it('stores correct metadata in the entry', () => {
    const decision: CriticDecision = { approved: true, score: 0.9 };
    gate.gate('test query', 'test result', decision, 0.7, 3);
    const entries = gate.getEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].query, 'test query');
    assert.equal(entries[0].result, 'test result');
    assert.equal(entries[0].criticScore, 0.9);
    assert.equal(entries[0].belief, 0.7);
    assert.equal(entries[0].step, 3);
    assert.ok(entries[0].timestamp > 0);
  });

  it('accumulates multiple approved entries', () => {
    gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1);
    gate.gate('q2', 'r2', { approved: true, score: 0.9 }, 0.6, 2);
    gate.gate('q3', 'r3', { approved: false, score: 0.3 }, 0.4, 3);
    assert.equal(gate.getEntries().length, 2);
  });
});

// ---------------------------------------------------------------------------
// getEntries() returns copies
// ---------------------------------------------------------------------------

describe('MemoryGate – getEntries()', () => {
  it('returns a copy of the entries array', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);
    const entries1 = gate.getEntries();
    const entries2 = gate.getEntries();
    assert.notEqual(entries1, entries2); // different array references
    assert.deepEqual(entries1, entries2); // same content
  });

  it('returns empty array when no entries', () => {
    const gate = new MemoryGate();
    assert.deepEqual(gate.getEntries(), []);
  });
});

// ---------------------------------------------------------------------------
// getApprovedContext()
// ---------------------------------------------------------------------------

describe('MemoryGate – getApprovedContext()', () => {
  it('returns empty string when no entries', () => {
    const gate = new MemoryGate();
    assert.equal(gate.getApprovedContext(), '');
  });

  it('formats Q/A pairs from approved entries', () => {
    const gate = new MemoryGate();
    gate.gate('what is X?', 'X is a variable', { approved: true, score: 0.8 }, 0.5, 1);
    const context = gate.getApprovedContext();
    assert.ok(context.includes('Q: what is X?'));
    assert.ok(context.includes('A: X is a variable'));
  });

  it('truncates long results to 500 chars with an explicit per-entry marker', () => {
    const gate = new MemoryGate();
    const longResult = 'x'.repeat(1000);
    gate.gate('q', longResult, { approved: true, score: 0.8 }, 0.5, 1);
    const context = gate.getApprovedContext();
    const aLine = context.split('\n').find(l => l.startsWith('A:'));
    // Exactly 500 chars of content are retained...
    assert.ok(aLine!.includes('x'.repeat(500)), 'should retain 500 chars of content');
    assert.ok(!aLine!.includes('x'.repeat(501)), 'should retain no more than 500 chars');
    // ...and a marker names how many chars were dropped (no silent mid-content cut).
    assert.match(aLine!, /…\[truncated 500 chars\]$/);
  });

  it('does not add a per-entry marker when the result is within the cap', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'x'.repeat(500), { approved: true, score: 0.8 }, 0.5, 1);
    const ctx = gate.getApprovedContext();
    assert.doesNotMatch(ctx, /truncated/);
    assert.equal(ctx, `Q: q\nA: ${'x'.repeat(500)}`);
  });

  it('includes multiple entries separated by double newlines', () => {
    const gate = new MemoryGate();
    gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1);
    gate.gate('q2', 'r2', { approved: true, score: 0.9 }, 0.6, 2);
    const context = gate.getApprovedContext();
    assert.ok(context.includes('q1'));
    assert.ok(context.includes('q2'));
    assert.ok(context.includes('\n\n'));
  });
});

// ---------------------------------------------------------------------------
// persistTo()
// ---------------------------------------------------------------------------

describe('MemoryGate – persistTo()', () => {
  it('calls saveMemory on the memory manager when entries exist', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);

    let savedPath = '';
    let savedContent = '';
    let savedDesc = '';
    let savedScope = '';

    const mockMemory = {
      saveMemory(path: string, content: string, desc: string, scope: string) {
        savedPath = path;
        savedContent = content;
        savedDesc = desc;
        savedScope = scope;
      },
    };

    gate.persistTo(mockMemory as any, 'test task');
    assert.ok(savedPath.startsWith('task-'));
    assert.ok(savedContent.includes('test task'));
    assert.ok(savedContent.includes('Step 1'));
    assert.equal(savedDesc, 'Critic-approved task results');
    assert.equal(savedScope, 'project');
  });

  it('does nothing when no entries exist', () => {
    const gate = new MemoryGate();
    let called = false;
    const mockMemory = {
      saveMemory() { called = true; },
    };
    gate.persistTo(mockMemory as any, 'task');
    assert.equal(called, false);
  });

  it('includes score in persisted content', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'result text here', { approved: true, score: 0.85 }, 0.5, 1);

    let savedContent = '';
    const mockMemory = {
      saveMemory(_p: string, content: string) { savedContent = content; },
    };
    gate.persistTo(mockMemory as any, 'task');
    assert.ok(savedContent.includes('0.85'));
  });

  it('truncates long results in persisted content to 200 chars with a marker', () => {
    const gate = new MemoryGate();
    const longResult = 'x'.repeat(500);
    gate.gate('q', longResult, { approved: true, score: 0.8 }, 0.5, 1);

    let savedContent = '';
    const mockMemory = {
      saveMemory(_p: string, content: string) { savedContent = content; },
    };
    gate.persistTo(mockMemory as any, 'task');
    const resultPart = savedContent.split('\n').find(l => l.includes('xxx'));
    assert.ok(resultPart!.length < 500, 'Should truncate long results');
    // The dropped tail is signalled, not silently cut mid-content.
    assert.ok(resultPart!.includes('…[truncated 300 chars]'), 'should mark the dropped tail');
    assert.ok(resultPart!.includes('x'.repeat(200)), 'should retain 200 chars of content');
  });
});

// ---------------------------------------------------------------------------
// Consolidation freshness (lastConsolidatedAt + getConsolidationStatus)
// ---------------------------------------------------------------------------

describe('MemoryGate – consolidation status', () => {
  it('reports "never" before any persistTo()', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);
    const status = gate.getConsolidationStatus();
    assert.equal(status.state, 'never');
    assert.equal(status.lastConsolidatedAt, null);
    assert.equal(status.relative, null);
    assert.equal(status.pending, 1);
  });

  it('records lastConsolidatedAt and goes "idle" after persistTo()', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);
    const before = Date.now();
    gate.persistTo({ saveMemory() {} } as any, 'task');
    const after = Date.now();
    const status = gate.getConsolidationStatus();
    assert.equal(status.state, 'idle');
    const ts = status.lastConsolidatedAt;
    assert.ok(ts !== null && ts >= before && ts <= after, 'timestamp within the persist window');
  });

  it('does not mark a consolidation when there is nothing to persist', () => {
    const gate = new MemoryGate();
    gate.persistTo({ saveMemory() {} } as any, 'task');
    const status = gate.getConsolidationStatus();
    assert.equal(status.lastConsolidatedAt, null);
    assert.equal(status.state, 'never');
  });

  it('renders a relative "ago" string against an injected now', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);
    gate.persistTo({ saveMemory() {} } as any, 'task');
    const ts = gate.getConsolidationStatus().lastConsolidatedAt!;
    // 90 seconds later: the interval table selects the minute unit (1m ago).
    const status = gate.getConsolidationStatus(ts + 90_000);
    assert.equal(status.relative, '1m ago');
  });

  it('reset() clears the consolidation timestamp back to "never"', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.8 }, 0.5, 1);
    gate.persistTo({ saveMemory() {} } as any, 'task');
    assert.notEqual(gate.getConsolidationStatus().lastConsolidatedAt, null);
    gate.reset();
    const status = gate.getConsolidationStatus();
    assert.equal(status.lastConsolidatedAt, null);
    assert.equal(status.state, 'never');
  });
});

// ---------------------------------------------------------------------------
// Overall size caps on gate state and model-facing content
// ---------------------------------------------------------------------------

describe('MemoryGate – size caps', () => {
  it('bounds the entries array at MAX_ENTRIES (drops oldest)', () => {
    const gate = new MemoryGate();
    for (let i = 0; i < MAX_ENTRIES + 25; i++) {
      gate.gate(`q${i}`, `r${i}`, { approved: true, score: 0.9 }, 0.5, i);
    }
    const entries = gate.getEntries();
    assert.equal(entries.length, MAX_ENTRIES);
    // Oldest (q0) was evicted; the most recent entry is retained.
    assert.equal(entries[0].query, 'q25');
    assert.equal(entries[entries.length - 1].query, `q${MAX_ENTRIES + 24}`);
  });

  it('getApprovedContext caps aggregate output and appends a truncation sentinel', () => {
    const gate = new MemoryGate();
    // Each entry contributes ~500 chars; enough entries overflow MAX_CONTEXT_CHARS.
    const big = 'x'.repeat(500);
    for (let i = 0; i < 40; i++) {
      gate.gate(`q${i}`, big, { approved: true, score: 0.9 }, 0.5, i);
    }
    const ctx = gate.getApprovedContext();
    assert.ok(
      ctx.length <= MAX_CONTEXT_CHARS + 80,
      `context should be capped near MAX_CONTEXT_CHARS, got ${ctx.length}`,
    );
    assert.match(ctx, /… \[truncated: \d+ more chars omitted\]$/);
  });

  it('getApprovedContext does not truncate when under budget', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'short answer', { approved: true, score: 0.9 }, 0.5, 1);
    const ctx = gate.getApprovedContext();
    assert.equal(ctx, 'Q: q\nA: short answer');
    assert.doesNotMatch(ctx, /truncated/);
  });

  it('persistTo caps aggregate persisted content with a sentinel', () => {
    const gate = new MemoryGate();
    const big = 'y'.repeat(200);
    for (let i = 0; i < 80; i++) {
      gate.gate(`q${i}`, big, { approved: true, score: 0.9 }, 0.5, i);
    }
    let savedContent = '';
    const mockMemory = { saveMemory(_p: string, content: string) { savedContent = content; } };
    gate.persistTo(mockMemory as any, 'task');
    // Header + capped body; body portion must respect the budget + sentinel.
    assert.ok(savedContent.includes('# Task: task'));
    assert.match(savedContent, /… \[truncated: \d+ more chars omitted\]$/);
    assert.ok(
      savedContent.length <= MAX_PERSIST_CHARS + 120,
      `persisted content should be capped, got ${savedContent.length}`,
    );
  });
});

// ---------------------------------------------------------------------------
// reset()
// ---------------------------------------------------------------------------

describe('MemoryGate – reset()', () => {
  it('clears all entries', () => {
    const gate = new MemoryGate();
    gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1);
    gate.gate('q2', 'r2', { approved: true, score: 0.9 }, 0.6, 2);
    assert.equal(gate.getEntries().length, 2);

    gate.reset();
    assert.equal(gate.getEntries().length, 0);
    assert.equal(gate.getApprovedContext(), '');
  });
});
