import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'node:path';
import * as os from 'node:os';
import * as fs from 'node:fs';

import { Critic } from '../src/superinference/critic';
import { MemoryGate } from '../src/superinference/memory-gate';
import { Retriever } from '../src/superinference/retriever';
import { MemoryManager } from '../src/memory';
import type { CriticDecision } from '../src/superinference/types';

// ---------------------------------------------------------------------------
// Critic — PPV (Equation 4)
// ---------------------------------------------------------------------------
describe('Critic PPV (Equation 4)', () => {
  it('paper example: p\'=0.7, alpha=0.05, beta=0.10 -> PPV ~0.977', () => {
    const critic = new Critic(0.05, 0.10);
    const ppv = critic.ppv(0.7);
    // PPV = (1-0.10)*0.7 / ((1-0.10)*0.7 + 0.05*(1-0.7))
    //     = 0.9*0.7 / (0.9*0.7 + 0.05*0.3)
    //     = 0.63 / (0.63 + 0.015)
    //     = 0.63 / 0.645
    //     ≈ 0.97674
    assert.ok(Math.abs(ppv - 0.97674) < 0.001, `Expected ~0.977, got ${ppv}`);
  });

  it('p\'=1.0 -> PPV=1.0 (certainly correct prior)', () => {
    const critic = new Critic(0.05, 0.10);
    const ppv = critic.ppv(1.0);
    assert.equal(ppv, 1.0);
  });

  it('p\'=0.0 -> PPV=0.0 (certainly incorrect prior)', () => {
    const critic = new Critic(0.05, 0.10);
    const ppv = critic.ppv(0.0);
    assert.equal(ppv, 0.0);
  });

  it('p\'=0.5 with default rates', () => {
    const critic = new Critic(0.05, 0.10);
    const ppv = critic.ppv(0.5);
    // PPV = 0.9*0.5 / (0.9*0.5 + 0.05*0.5)
    //     = 0.45 / (0.45 + 0.025)
    //     = 0.45 / 0.475 ≈ 0.94737
    assert.ok(Math.abs(ppv - 0.94737) < 0.001, `Expected ~0.947, got ${ppv}`);
  });
});

// ---------------------------------------------------------------------------
// Critic — constructor defaults
// ---------------------------------------------------------------------------
describe('Critic constructor', () => {
  it('default alpha=0.05, beta=0.10', () => {
    const critic = new Critic();
    assert.equal(critic.alphaRate, 0.05);
    assert.equal(critic.betaRate, 0.10);
  });

  it('custom alpha and beta', () => {
    const critic = new Critic(0.01, 0.20);
    assert.equal(critic.alphaRate, 0.01);
    assert.equal(critic.betaRate, 0.20);
  });
});

// ---------------------------------------------------------------------------
// MemoryGate — Equation 5
// ---------------------------------------------------------------------------
describe('MemoryGate (Equation 5)', () => {
  it('approved decision adds entry', () => {
    const gate = new MemoryGate();
    const decision: CriticDecision = { approved: true, score: 0.9, reason: 'Looks correct' };
    const added = gate.gate('test query', 'test result', decision, 0.7, 1);
    assert.equal(added, true);
    assert.equal(gate.getEntries().length, 1);
  });

  it('rejected decision does not add entry', () => {
    const gate = new MemoryGate();
    const decision: CriticDecision = { approved: false, score: 0.3, reason: 'Incorrect' };
    const added = gate.gate('test query', 'test result', decision, 0.4, 1);
    assert.equal(added, false);
    assert.equal(gate.getEntries().length, 0);
  });

  it('getEntries returns correct data', () => {
    const gate = new MemoryGate();
    const decision: CriticDecision = { approved: true, score: 0.85 };
    gate.gate('find bug', 'fixed in line 42', decision, 0.6, 3);

    const entries = gate.getEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].query, 'find bug');
    assert.equal(entries[0].result, 'fixed in line 42');
    assert.equal(entries[0].criticScore, 0.85);
    assert.equal(entries[0].belief, 0.6);
    assert.equal(entries[0].step, 3);
    assert.ok(entries[0].timestamp > 0);
  });

  it('getEntries returns a defensive copy', () => {
    const gate = new MemoryGate();
    const decision: CriticDecision = { approved: true, score: 0.9 };
    gate.gate('q', 'r', decision, 0.5, 1);

    const entries1 = gate.getEntries();
    entries1.length = 0; // mutate the returned array
    assert.equal(gate.getEntries().length, 1); // internal state unaffected
  });

  it('getApprovedContext formats correctly', () => {
    const gate = new MemoryGate();
    gate.gate('query1', 'result1', { approved: true, score: 0.9 }, 0.5, 1);
    gate.gate('query2', 'result2', { approved: true, score: 0.8 }, 0.6, 2);

    const context = gate.getApprovedContext();
    assert.ok(context.includes('Q: query1'));
    assert.ok(context.includes('A: result1'));
    assert.ok(context.includes('Q: query2'));
    assert.ok(context.includes('A: result2'));
  });

  it('getApprovedContext returns empty string when no entries', () => {
    const gate = new MemoryGate();
    assert.equal(gate.getApprovedContext(), '');
  });

  it('reset clears all entries', () => {
    const gate = new MemoryGate();
    gate.gate('q', 'r', { approved: true, score: 0.9 }, 0.5, 1);
    assert.equal(gate.getEntries().length, 1);
    gate.reset();
    assert.equal(gate.getEntries().length, 0);
  });
});

// ---------------------------------------------------------------------------
// Retriever — combined context
// ---------------------------------------------------------------------------
describe('Retriever', () => {
  it('returns combined context from memory + gate', () => {
    // Use a temp directory so MemoryManager finds no real instruction files
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-retriever-'));
    const memory = new MemoryManager(tmpDir);
    const gate = new MemoryGate();
    const retriever = new Retriever(0); // η=0 for deterministic test

    // Add an approved entry to the gate
    gate.gate('what is X', 'X is 42', { approved: true, score: 0.95 }, 0.8, 1);

    // Save a memory entry with keywords that match the query
    memory.saveMemory('test-mem', 'Some stored knowledge about retrieval', 'test memory', 'project');

    const context = retriever.retrieve('stored knowledge retrieval', memory, gate);
    assert.ok(context.includes('Previously Approved Results'), 'Should include approved results header');
    assert.ok(context.includes('X is 42'), 'Should include gate content');
    assert.ok(context.includes('Stored Memories'), 'Should include stored memories header');
    assert.ok(context.includes('stored knowledge'), 'Should include memory content');

    // Clean up
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns empty string when no context available', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-retriever-'));
    const memory = new MemoryManager(tmpDir);
    const gate = new MemoryGate();
    const retriever = new Retriever(0);

    const context = retriever.retrieve('anything', memory, gate);
    assert.equal(context, '');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('default noise level is 0.1', () => {
    const retriever = new Retriever();
    assert.equal(retriever.noiseLevel, 0.1);
  });

  it('custom noise level', () => {
    const retriever = new Retriever(0.25);
    assert.equal(retriever.noiseLevel, 0.25);
  });
});

// ---------------------------------------------------------------------------
// Critic — extractJSON (balanced braces)
// ---------------------------------------------------------------------------
describe('Critic extractJSON (balanced braces)', () => {
  const critic = new Critic();

  it('valid simple JSON', () => {
    const input = '{"approved": true, "score": 0.9, "reason": "looks good"}';
    const result = critic.extractJSON(input);
    assert.deepEqual(result, { approved: true, score: 0.9, reason: 'looks good' });
  });

  it('JSON with nested braces in reason', () => {
    const input = '{"approved": true, "score": 0.8, "reason": "found {issue} in code"}';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, true);
    assert.equal(result!.score, 0.8);
    assert.equal(result!.reason, 'found {issue} in code');
  });

  it('JSON with prefix text', () => {
    const input = 'Here is my assessment: {"approved": false, "score": 0.2, "reason": "wrong"}';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, false);
    assert.equal(result!.score, 0.2);
    assert.equal(result!.reason, 'wrong');
  });

  it('no JSON at all', () => {
    const result = critic.extractJSON('This is just plain text');
    assert.equal(result, null);
  });

  it('incomplete JSON (no closing brace)', () => {
    const result = critic.extractJSON('{"approved": true');
    assert.equal(result, null);
  });

  it('empty string', () => {
    const result = critic.extractJSON('');
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// MemoryGate — persistTo()
// ---------------------------------------------------------------------------
describe('MemoryGate persistTo()', () => {
  let tmpDir: string;

  it('persists approved entries to disk via MemoryManager', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-persist-'));
    const memory = new MemoryManager(tmpDir);
    const gate = new MemoryGate();

    gate.gate('how to fix bug', 'patch line 42', { approved: true, score: 0.9 }, 0.7, 1);
    gate.gate('add tests', 'wrote 3 tests', { approved: true, score: 0.85 }, 0.8, 2);

    gate.persistTo(memory, 'Fix critical bug');

    const memoryDir = path.join(tmpDir, '.superinference', 'memory');
    assert.ok(fs.existsSync(memoryDir), 'Memory directory should exist');

    const files = fs.readdirSync(memoryDir).filter(f => f.endsWith('.md'));
    assert.equal(files.length, 1, 'Should have created exactly one memory file');

    const content = fs.readFileSync(path.join(memoryDir, files[0]), 'utf-8');
    assert.ok(content.includes('Fix critical bug'), 'File should contain task description');
    assert.ok(content.includes('how to fix bug'), 'File should contain first query');
    assert.ok(content.includes('add tests'), 'File should contain second query');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('persistTo() with empty entries is a no-op', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-persist-empty-'));
    const memory = new MemoryManager(tmpDir);
    const gate = new MemoryGate();

    gate.persistTo(memory, 'Empty task');

    const memoryDir = path.join(tmpDir, '.superinference', 'memory');
    assert.equal(fs.existsSync(memoryDir), false, 'Memory directory should not be created');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Retriever — noise channel (Eq. 2)
// ---------------------------------------------------------------------------
describe('Retriever noise channel (Eq. 2)', () => {
  function buildGateWithEntries(): MemoryGate {
    const gate = new MemoryGate();
    gate.gate('database optimization', 'use index on column id', { approved: true, score: 0.9 }, 0.8, 1);
    gate.gate('css layout fix', 'use flexbox for alignment', { approved: true, score: 0.85 }, 0.7, 2);
    gate.gate('api authentication', 'add bearer token header', { approved: true, score: 0.92 }, 0.75, 3);
    return gate;
  }

  it('η=0: retrieve() includes ALL approved entries (no corruption)', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-noise-zero-'));
    const memory = new MemoryManager(tmpDir);
    const gate = buildGateWithEntries();
    const retriever = new Retriever(0);

    const context = retriever.retrieve('database optimization', memory, gate);
    assert.ok(context.includes('use index on column id'), 'Should include entry 1');
    assert.ok(context.includes('use flexbox for alignment'), 'Should include entry 2');
    assert.ok(context.includes('add bearer token header'), 'Should include entry 3');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('η=1: retrieve() returns no approved/memory entries (max corruption)', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-noise-one-'));
    const memory = new MemoryManager(tmpDir);
    memory.saveMemory('stored-item', 'some stored knowledge', 'description', 'project');
    const gate = buildGateWithEntries();
    const retriever = new Retriever(1);

    const context = retriever.retrieve('anything', memory, gate);
    // With η=1 all entries are dropped by the noise channel
    assert.ok(!context.includes('Previously Approved Results'), 'Should not include approved results');
    assert.ok(!context.includes('Stored Memories'), 'Should not include stored memories');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('query relevance: matching entries appear, unrelated may not', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-noise-relevance-'));
    const memory = new MemoryManager(tmpDir);
    // Save memories with very distinct keywords
    memory.saveMemory('quantum-physics', 'quantum entanglement particle wave duality', 'physics notes', 'project');
    memory.saveMemory('chocolate-recipe', 'chocolate cake flour sugar butter baking', 'baking recipe', 'project');
    const gate = new MemoryGate();
    const retriever = new Retriever(0); // No noise so filtering is purely relevance

    const context = retriever.retrieve('quantum physics particle entanglement', memory, gate);
    assert.ok(context.includes('quantum entanglement'), 'Relevant entry should appear');
    // The chocolate entry has zero keyword overlap with the query and relevance=0,
    // so queryRelevance filters it out
    assert.ok(!context.includes('chocolate cake'), 'Unrelated entry should not appear');

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
