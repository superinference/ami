import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { MemoryManager } from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

/**
 * U016 / wave-71 — Memory recall-shape introspection telemetry.
 *
 * Parity with the reference's `logMemoryRecallShape` (memdir/findRelevantMemories.ts):
 * `MemoryManager.selectRelevantMemories` must emit a PII-free `memory_recall`
 * diagnostic on EVERY recall — including a recall that ran but selected nothing
 * (encoded with a -1 age sentinel) — so a selection-rate can be computed and
 * "ran, picked nothing" is distinguishable from "never ran". Recorded through the
 * `AMI_DIAGNOSTICS_FILE` NO-PII JSONL channel (counts + age only, never paths or
 * the query text).
 */

let tmpDir: string;
let memoryDir: string;
let diagFile: string;
let prevDiag: string | undefined;

function writeMemory(name: string, description: string, memType: string, body: string): void {
  const content = `---\nname: ${name}\ndescription: ${description}\ntype: ${memType}\n---\n\n${body}\n`;
  fs.writeFileSync(path.join(memoryDir, `${name}.md`), content, 'utf-8');
}

function readRecallEvents(): Array<Record<string, unknown>> {
  if (!fs.existsSync(diagFile)) return [];
  return fs
    .readFileSync(diagFile, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map(l => JSON.parse(l) as Record<string, unknown>)
    .filter(r => r.event === 'memory_recall');
}

function lastRecall(): { candidates: number; selected: number; topAgeDays: number; queryTokens: number } {
  const events = readRecallEvents();
  assert.ok(events.length > 0, 'expected at least one memory_recall diagnostic event');
  return events[events.length - 1]!.data as unknown as {
    candidates: number;
    selected: number;
    topAgeDays: number;
    queryTokens: number;
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memw71-'));
  memoryDir = projectStateDir(tmpDir, 'memory');
  fs.mkdirSync(memoryDir, { recursive: true });
  diagFile = path.join(tmpDir, 'diag', 'diagnostics.jsonl');
  prevDiag = process.env.AMI_DIAGNOSTICS_FILE;
  process.env.AMI_DIAGNOSTICS_FILE = diagFile;
});

afterEach(() => {
  if (prevDiag === undefined) delete process.env.AMI_DIAGNOSTICS_FILE;
  else process.env.AMI_DIAGNOSTICS_FILE = prevDiag;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('MemoryManager.selectRelevantMemories — recall-shape telemetry (U016/w71)', () => {
  it('fires on an empty selection with a -1 age sentinel', () => {
    // No memory files at all → ran, selected nothing.
    fs.rmSync(memoryDir, { recursive: true, force: true });
    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('anything at all', 5);
    assert.equal(result.length, 0);

    const shape = lastRecall();
    assert.equal(shape.candidates, 0);
    assert.equal(shape.selected, 0);
    assert.equal(shape.topAgeDays, -1, 'empty selection must use the -1 age sentinel');
  });

  it('records a non-negative age and real counts when memories are selected', () => {
    writeMemory('auth-feedback', 'authentication approach', 'feedback', 'Always use JWT tokens for authentication.');
    writeMemory('deploy-ref', 'deployment pipeline', 'reference', 'Deploy using GitHub Actions.');
    const mm = new MemoryManager(tmpDir);

    // 2 candidates, budget 5 → all surfaced (the <= maxResults branch).
    const result = mm.selectRelevantMemories('implement JWT authentication', 5);
    assert.equal(result.length, 2);

    const shape = lastRecall();
    assert.equal(shape.candidates, 2);
    assert.equal(shape.selected, 2);
    assert.ok(shape.topAgeDays >= 0, `expected a real age, got ${shape.topAgeDays}`);
    assert.ok(shape.queryTokens > 0);
  });

  it('fires from the TF-IDF scoring branch (candidates > budget) with the candidate denominator', () => {
    for (let i = 0; i < 8; i++) {
      writeMemory(`mem-${i}`, `topic ${i}`, 'project', `body about subject number ${i} and tokens`);
    }
    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('subject number 3 tokens', 3);
    assert.equal(result.length, 3);

    const shape = lastRecall();
    assert.equal(shape.candidates, 8, 'denominator is the full candidate pool, not the capped selection');
    assert.equal(shape.selected, 3);
    assert.ok(shape.topAgeDays >= 0);
    assert.ok(shape.queryTokens > 0);
  });

  it('reports queryTokens:0 for the empty-query fallback branch', () => {
    for (let i = 0; i < 8; i++) {
      writeMemory(`m${i}`, `d${i}`, 'user', `content ${i}`);
    }
    const mm = new MemoryManager(tmpDir);
    // Non-tokenizable query but more candidates than the budget → the
    // queryTokens.length === 0 fallback branch runs and still emits telemetry.
    const result = mm.selectRelevantMemories('   ', 3);
    assert.equal(result.length, 3);

    const shape = lastRecall();
    assert.equal(shape.queryTokens, 0);
    assert.equal(shape.candidates, 8);
    assert.equal(shape.selected, 3);
  });

  it('is a silent no-op when AMI_DIAGNOSTICS_FILE is unset (no PII channel configured)', () => {
    delete process.env.AMI_DIAGNOSTICS_FILE;
    writeMemory('m1', 'd1', 'user', 'content');
    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('content', 5);
    assert.equal(result.length, 1);
    assert.equal(fs.existsSync(diagFile), false, 'no diagnostics file should be written when the channel is off');
  });
});
