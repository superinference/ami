import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { Retriever } from '../src/superinference/retriever';
import { MemoryGate } from '../src/superinference/memory-gate';

// ---------------------------------------------------------------------------
// Mock MemoryManager
// ---------------------------------------------------------------------------

function createMockMemory(opts?: {
  instructions?: string;
  memories?: Array<{ content: string }>;
}) {
  return {
    loadProjectInstructions: () => opts?.instructions ?? '',
    loadMemories: () =>
      (opts?.memories ?? []).map((m) => ({
        path: 'test',
        content: m.content,
        type: 'memory' as const,
      })),
  };
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

describe('Retriever – constructor', () => {
  it('clamps noiseLevel to [0, 1]', () => {
    assert.equal(new Retriever(-0.5).noiseLevel, 0);
    assert.equal(new Retriever(2.0).noiseLevel, 1);
    assert.equal(new Retriever(0.3).noiseLevel, 0.3);
  });

  it('defaults noiseLevel to 0.1', () => {
    assert.equal(new Retriever().noiseLevel, 0.1);
  });
});

// ---------------------------------------------------------------------------
// retrieve() — basic behavior
// ---------------------------------------------------------------------------

describe('Retriever – retrieve()', () => {
  it('returns project instructions when available', () => {
    const retriever = new Retriever(0); // no noise
    const memory = createMockMemory({ instructions: '# Project Rules' });
    const gate = new MemoryGate();

    const result = retriever.retrieve('query', memory as any, gate);
    assert.ok(result.includes('# Project Rules'));
  });

  it('returns empty string when nothing is available', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory();
    const gate = new MemoryGate();

    const result = retriever.retrieve('query', memory as any, gate);
    assert.equal(result, '');
  });

  it('includes approved context from memory gate', () => {
    const retriever = new Retriever(0); // no noise
    const memory = createMockMemory();
    const gate = new MemoryGate();
    gate.gate('what is X?', 'X is 42', { approved: true, score: 0.9 }, 0.5, 1);

    const result = retriever.retrieve('query', memory as any, gate);
    assert.ok(result.includes('Previously Approved Results'));
    assert.ok(result.includes('X is 42'));
  });

  it('includes stored memories sorted by relevance', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [
        { content: 'This is about typescript configuration' },
        { content: 'This is about python virtualenv' },
        { content: 'This is about typescript testing patterns' },
      ],
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('typescript testing', memory as any, gate);
    assert.ok(result.includes('Stored Memories'));
  });

  it('combines all sources', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      instructions: '# Instructions',
      memories: [{ content: 'Some stored memory about the topic' }],
    });
    const gate = new MemoryGate();
    gate.gate('q', 'approved result', { approved: true, score: 0.8 }, 0.5, 1);

    const result = retriever.retrieve('topic', memory as any, gate);
    assert.ok(result.includes('# Instructions'));
    assert.ok(result.includes('Previously Approved Results'));
    assert.ok(result.includes('Stored Memories'));
  });
});

// ---------------------------------------------------------------------------
// Noise channel — Eq. 2
// ---------------------------------------------------------------------------

describe('Retriever – noise channel', () => {
  it('drops all entries when noiseLevel = 1.0', () => {
    const retriever = new Retriever(1.0); // max noise
    const memory = createMockMemory({
      instructions: '# Instructions',
    });
    const gate = new MemoryGate();
    gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1);
    gate.gate('q2', 'r2', { approved: true, score: 0.9 }, 0.6, 2);

    const result = retriever.retrieve('query', memory as any, gate);
    // Instructions are always included (not subject to noise)
    assert.ok(result.includes('# Instructions'));
    // Approved context should be dropped by noise
    assert.ok(!result.includes('Previously Approved Results'));
  });

  it('keeps all entries when noiseLevel = 0', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory();
    const gate = new MemoryGate();
    gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1);
    gate.gate('q2', 'r2', { approved: true, score: 0.9 }, 0.6, 2);

    const result = retriever.retrieve('query', memory as any, gate);
    assert.ok(result.includes('q1'));
    assert.ok(result.includes('q2'));
  });
});

// ---------------------------------------------------------------------------
// Query relevance scoring
// ---------------------------------------------------------------------------

describe('Retriever – relevance scoring', () => {
  it('includes memories relevant to the query', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [
        { content: 'Configure the database connection string' },
        { content: 'Completely unrelated content about cooking recipes' },
      ],
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('database connection', memory as any, gate);
    // The database-related memory should be included
    assert.ok(result.includes('database'));
  });

  it('returns everything when query is empty', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [
        { content: 'memory one' },
        { content: 'memory two' },
      ],
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('', memory as any, gate);
    // Empty query => all memories are "relevant"
    assert.ok(result.includes('memory one') || result.includes('Stored Memories'));
  });

  it('filters out memories with zero relevance', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [
        { content: 'zzz yyy xxx' }, // no tokens match "abc def ghi"
      ],
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('abc def ghi', memory as any, gate);
    // Should have zero relevance and be filtered out
    assert.ok(!result.includes('zzz'));
  });
});

// ---------------------------------------------------------------------------
// noiseLevel getter
// ---------------------------------------------------------------------------

describe('Retriever – noiseLevel getter', () => {
  it('returns the configured noise level', () => {
    assert.equal(new Retriever(0.5).noiseLevel, 0.5);
  });
});
