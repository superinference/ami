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

// ---------------------------------------------------------------------------
// Graceful fallback on backing-store failure (gap: error handling / fallback)
// ---------------------------------------------------------------------------

describe('Retriever – graceful fallback on store error', () => {
  it('skips a throwing loadMemories but keeps instructions', () => {
    const retriever = new Retriever(0);
    const memory = {
      loadProjectInstructions: () => '# Project Rules',
      loadMemories: () => {
        throw new Error('fs blew up');
      },
    };
    const gate = new MemoryGate();

    const result = retriever.retrieve('query', memory as any, gate);
    assert.ok(result.includes('# Project Rules'));
    assert.ok(!result.includes('Stored Memories'));
  });

  it('skips throwing instructions + approved context but keeps memories', () => {
    const retriever = new Retriever(0);
    const memory = {
      loadProjectInstructions: () => {
        throw new Error('instructions read failed');
      },
      loadMemories: () => [
        { path: 'p', content: 'relevant topic memory', type: 'memory' as const },
      ],
    };
    const gate = {
      getApprovedContext: () => {
        throw new Error('gate read failed');
      },
    };

    const result = retriever.retrieve('topic', memory as any, gate as any);
    assert.ok(result.includes('Stored Memories'));
    assert.ok(result.includes('relevant topic memory'));
  });

  it('returns empty string when every source throws', () => {
    const retriever = new Retriever(0);
    const memory = {
      loadProjectInstructions: () => {
        throw new Error('x');
      },
      loadMemories: () => {
        throw new Error('y');
      },
    };
    const gate = {
      getApprovedContext: () => {
        throw new Error('z');
      },
    };

    const result = retriever.retrieve('q', memory as any, gate as any);
    assert.equal(result, '');
  });
});

// ---------------------------------------------------------------------------
// Hard cap / top-K on retrieved memories (gap: top-K cap)
// ---------------------------------------------------------------------------

describe('Retriever – top-K cap', () => {
  it('injects at most 5 stored memories even when more are relevant', () => {
    const retriever = new Retriever(0); // no noise — deterministic
    const memory = createMockMemory({
      memories: Array.from({ length: 12 }, (_, i) => ({
        content: `topic memory number ${i}`,
      })),
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('topic', memory as any, gate);
    const section = result.split('# Stored Memories')[1] ?? '';
    const lines = section.split('\n').filter(l => l.includes('topic memory number'));
    assert.equal(lines.length, 5);
  });

  it('caps approved-context entries to 5', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory();
    const gate = new MemoryGate();
    for (let i = 0; i < 9; i++) {
      gate.gate(`q${i}`, `approved-result-${i}`, { approved: true, score: 0.9 }, 0.5, i + 1);
    }

    const result = retriever.retrieve('query', memory as any, gate);
    const matches = result.match(/approved-result-\d+/g) ?? [];
    assert.equal(matches.length, 5);
  });
});

// ---------------------------------------------------------------------------
// Token-boundary matching (gap: keyword false-positive guard)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Cancellation / abort support (gap: cancellation abort support)
// ---------------------------------------------------------------------------

describe('Retriever – cancellation / abort', () => {
  it('returns empty string immediately when the signal is already aborted', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      instructions: '# Instructions',
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    gate.gate('q', 'approved result', { approved: true, score: 0.9 }, 0.5, 1);

    const controller = new AbortController();
    controller.abort();

    const result = retriever.retrieve('topic', memory as any, gate, {
      signal: controller.signal,
    });
    assert.equal(result, '');
  });

  it('bails after instructions when aborted before the approved-context stage', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      instructions: '# Instructions',
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    gate.gate('q', 'approved result', { approved: true, score: 0.9 }, 0.5, 1);

    // aborted flips true on the SECOND read (after instructions, before approved)
    let reads = 0;
    const signal = {
      get aborted() {
        reads++;
        return reads >= 2;
      },
    } as unknown as AbortSignal;

    const result = retriever.retrieve('topic', memory as any, gate, { signal });
    assert.ok(result.includes('# Instructions'));
    assert.ok(!result.includes('Previously Approved Results'));
    assert.ok(!result.includes('Stored Memories'));
  });

  it('bails after approved context when aborted before the memories stage', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      instructions: '# Instructions',
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    gate.gate('q', 'approved result', { approved: true, score: 0.9 }, 0.5, 1);

    // aborted flips true on the THIRD read (after approved, before memories)
    let reads = 0;
    const signal = {
      get aborted() {
        reads++;
        return reads >= 3;
      },
    } as unknown as AbortSignal;

    const result = retriever.retrieve('topic', memory as any, gate, { signal });
    assert.ok(result.includes('# Instructions'));
    assert.ok(result.includes('Previously Approved Results'));
    assert.ok(!result.includes('Stored Memories'));
  });

  it('assembles everything when the signal is never aborted', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      instructions: '# Instructions',
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    gate.gate('q', 'approved result', { approved: true, score: 0.9 }, 0.5, 1);

    const controller = new AbortController();
    const result = retriever.retrieve('topic', memory as any, gate, {
      signal: controller.signal,
    });
    assert.ok(result.includes('# Instructions'));
    assert.ok(result.includes('Previously Approved Results'));
    assert.ok(result.includes('Stored Memories'));
  });
});

// ---------------------------------------------------------------------------
// Cross-turn dedup of already-surfaced memories (gap: cross-turn dedup)
// ---------------------------------------------------------------------------

describe('Retriever – cross-turn dedup', () => {
  it('records surfaced memories and skips them on a later call sharing the ledger', () => {
    const retriever = new Retriever(0); // no noise — deterministic
    const memory = createMockMemory({
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    const surfacedMemories = new Set<string>();

    const first = retriever.retrieve('topic', memory as any, gate, { surfacedMemories });
    assert.ok(first.includes('Stored Memories'));
    assert.ok(first.includes('relevant topic memory'));
    assert.ok(surfacedMemories.has('relevant topic memory'));

    // Second call with the same ledger: the only memory is already surfaced,
    // so it is skipped and no Stored Memories section is produced.
    const second = retriever.retrieve('topic', memory as any, gate, { surfacedMemories });
    assert.ok(!second.includes('Stored Memories'));
  });

  it('still surfaces fresh memories not in the ledger', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [
        { content: 'already shown topic memory' },
        { content: 'fresh topic memory' },
      ],
    });
    const gate = new MemoryGate();
    const surfacedMemories = new Set<string>(['already shown topic memory']);

    const result = retriever.retrieve('topic', memory as any, gate, { surfacedMemories });
    assert.ok(result.includes('Stored Memories'));
    assert.ok(result.includes('fresh topic memory'));
    assert.ok(!result.includes('already shown topic memory'));
    assert.ok(surfacedMemories.has('fresh topic memory'));
  });

  it('does not record memories dropped by the noise channel', () => {
    const retriever = new Retriever(1.0); // max noise — every entry dropped
    const memory = createMockMemory({
      memories: [{ content: 'relevant topic memory' }],
    });
    const gate = new MemoryGate();
    const surfacedMemories = new Set<string>();

    const result = retriever.retrieve('topic', memory as any, gate, { surfacedMemories });
    assert.ok(!result.includes('Stored Memories'));
    // Nothing was actually surfaced, so the ledger stays empty.
    assert.equal(surfacedMemories.size, 0);
  });
});

describe('Retriever – token-boundary relevance', () => {
  it('does not treat a query token as relevant to a superstring word', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [{ content: 'this discusses categories and categorization' }],
    });
    const gate = new MemoryGate();

    // "cat" is a substring of "categories" but not a whole token → no match.
    const result = retriever.retrieve('cat', memory as any, gate);
    assert.ok(!result.includes('Stored Memories'));
  });

  it('still matches on whole-token overlap', () => {
    const retriever = new Retriever(0);
    const memory = createMockMemory({
      memories: [{ content: 'the cat sat on the mat' }],
    });
    const gate = new MemoryGate();

    const result = retriever.retrieve('cat', memory as any, gate);
    assert.ok(result.includes('Stored Memories'));
    assert.ok(result.includes('the cat sat'));
  });
});
