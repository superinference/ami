import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { MemoryGate } from '../src/superinference/memory-gate';
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

  it('truncates long results to 500 chars', () => {
    const gate = new MemoryGate();
    const longResult = 'x'.repeat(1000);
    gate.gate('q', longResult, { approved: true, score: 0.8 }, 0.5, 1);
    const context = gate.getApprovedContext();
    // The 'A:' line should have at most ~500 chars of the result
    const aLine = context.split('\n').find(l => l.startsWith('A:'));
    assert.ok(aLine!.length <= 510, `Result should be truncated, got ${aLine!.length} chars`);
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

  it('truncates long results in persisted content to 200 chars', () => {
    const gate = new MemoryGate();
    const longResult = 'x'.repeat(500);
    gate.gate('q', longResult, { approved: true, score: 0.8 }, 0.5, 1);

    let savedContent = '';
    const mockMemory = {
      saveMemory(_p: string, content: string) { savedContent = content; },
    };
    gate.persistTo(mockMemory as any, 'task');
    // The result portion should be substring'd to 200 chars
    const resultPart = savedContent.split('\n').find(l => l.includes('xxx'));
    assert.ok(resultPart!.length < 500, 'Should truncate long results');
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
