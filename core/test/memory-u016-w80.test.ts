import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  resolveMemoryRelevanceConfig,
  DEFAULT_MEMORY_RELEVANCE,
  FRONTMATTER_MAX_LINES,
} from '../src/memory';

// Each test isolates the auto-memory store via AMI_MEMORY_DIR (a tmp dir) so the
// global ~/.ami projects store is never touched and team state is deterministic.
let cwd: string;
let memDir: string;
const savedEnv: Record<string, string | undefined> = {};

function stashEnv(...keys: string[]): void {
  for (const k of keys) savedEnv[k] = process.env[k];
}
function restoreEnv(): void {
  for (const [k, v] of Object.entries(savedEnv)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

beforeEach(() => {
  cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-w80-cwd-'));
  memDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-w80-store-'));
  stashEnv('AMI_MEMORY_DIR', 'AMI_ENABLE_TEAM_MEMORY', 'AMI_MEMORY_BM25_K1', 'AMI_MEMORY_BM25_B', 'AMI_MEMORY_BM25_AVGDL', 'AMI_DISABLE_AUTO_MEMORY');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_ENABLE_TEAM_MEMORY;
  delete process.env.AMI_MEMORY_BM25_K1;
  delete process.env.AMI_MEMORY_BM25_B;
  delete process.env.AMI_MEMORY_BM25_AVGDL;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  restoreEnv();
  for (const d of [cwd, memDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch {}
  }
});

// ---------------------------------------------------------------------------
// BM25 relevance config knobs (parity: findRelevantMemories relevance config)
// ---------------------------------------------------------------------------
describe('resolveMemoryRelevanceConfig', () => {
  it('returns the documented defaults when no env override is set', () => {
    assert.deepEqual(resolveMemoryRelevanceConfig({}), DEFAULT_MEMORY_RELEVANCE);
  });

  it('reads and applies valid env overrides', () => {
    const cfg = resolveMemoryRelevanceConfig({
      AMI_MEMORY_BM25_K1: '2.0',
      AMI_MEMORY_BM25_B: '0.3',
      AMI_MEMORY_BM25_AVGDL: '120',
    });
    assert.deepEqual(cfg, { k1: 2.0, b: 0.3, avgDl: 120 });
  });

  it('clamps out-of-range knobs into their safe ranges', () => {
    const cfg = resolveMemoryRelevanceConfig({
      AMI_MEMORY_BM25_K1: '999',   // > 10
      AMI_MEMORY_BM25_B: '5',      // > 1
      AMI_MEMORY_BM25_AVGDL: '0',  // < 1
    });
    assert.equal(cfg.k1, 10);
    assert.equal(cfg.b, 1);
    assert.equal(cfg.avgDl, 1);
  });

  it('clamps negative knobs up to the floor', () => {
    const cfg = resolveMemoryRelevanceConfig({ AMI_MEMORY_BM25_K1: '-3', AMI_MEMORY_BM25_B: '-1' });
    assert.equal(cfg.k1, 0);
    assert.equal(cfg.b, 0);
  });

  it('falls back to defaults for malformed / empty values', () => {
    assert.deepEqual(
      resolveMemoryRelevanceConfig({ AMI_MEMORY_BM25_K1: 'abc', AMI_MEMORY_BM25_B: '', AMI_MEMORY_BM25_AVGDL: '  ' }),
      DEFAULT_MEMORY_RELEVANCE,
    );
  });
});

describe('selectRelevantMemories honors the BM25 relevance knobs', () => {
  it('a per-call relevance override flips the top pick by length normalization', () => {
    const mgr = new MemoryManager(cwd);
    // A short memory mentioning the term once.
    mgr.saveMemory('short-note', 'alpha concise.', 'short', 'project');
    // A long memory mentioning the term three times but padded with filler.
    const filler = Array.from({ length: 300 }, (_, i) => `filler${i}`).join(' ');
    mgr.saveMemory('long-note', `alpha alpha alpha ${filler}`, 'long', 'project');
    // An irrelevant third memory so the scoring branch (candidates > maxResults) runs.
    mgr.saveMemory('other-note', 'unrelated content here.', 'other', 'project');

    // No length normalization (b=0) → the higher term-frequency (long) memory wins.
    const topNoPenalty = mgr.selectRelevantMemories('alpha', 1, { relevance: { b: 0 } });
    assert.equal(topNoPenalty.length, 1);
    assert.equal(topNoPenalty[0].name, 'long-note');

    // Maximal length penalty (avgDl 1, b 1) → the long memory is crushed, short wins.
    const topPenalty = mgr.selectRelevantMemories('alpha', 1, { relevance: { avgDl: 1, b: 1 } });
    assert.equal(topPenalty.length, 1);
    assert.equal(topPenalty[0].name, 'short-note');
  });

  it('reads BM25 knobs from the environment when no per-call override is given', () => {
    const mgr = new MemoryManager(cwd);
    mgr.saveMemory('short-note', 'alpha concise.', 'short', 'project');
    const filler = Array.from({ length: 300 }, (_, i) => `filler${i}`).join(' ');
    mgr.saveMemory('long-note', `alpha alpha alpha ${filler}`, 'long', 'project');
    mgr.saveMemory('other-note', 'unrelated content here.', 'other', 'project');

    process.env.AMI_MEMORY_BM25_B = '0';
    assert.equal(mgr.selectRelevantMemories('alpha', 1)[0].name, 'long-note');

    process.env.AMI_MEMORY_BM25_B = '1';
    process.env.AMI_MEMORY_BM25_AVGDL = '1';
    assert.equal(mgr.selectRelevantMemories('alpha', 1)[0].name, 'short-note');
  });
});

// ---------------------------------------------------------------------------
// Team-memory enabled-state introspection (parity: teamMemPaths.ts)
// ---------------------------------------------------------------------------
describe('getTeamMemoryState', () => {
  it('reports disabled + zero files by default, with a stable dir/entrypoint shape', () => {
    const mgr = new MemoryManager(cwd);
    const state = mgr.getTeamMemoryState();
    assert.equal(state.enabled, false);
    assert.equal(state.fileCount, 0);
    assert.equal(state.dir, path.join(memDir, 'team'));
    assert.equal(state.entrypoint, path.join(memDir, 'team', 'MEMORY.md'));
  });

  it('reflects AMI_ENABLE_TEAM_MEMORY and counts shared files', () => {
    process.env.AMI_ENABLE_TEAM_MEMORY = '1';
    const mgr = new MemoryManager(cwd);
    mgr.saveMemory('shared-rule', 'Team convention.', 'shared', 'project', 'team');
    const state = mgr.getTeamMemoryState();
    assert.equal(state.enabled, true);
    assert.equal(state.fileCount, 1);
  });
});

// ---------------------------------------------------------------------------
// Candidate instruction entrypoints ("(new)" affordance, parity: MemoryFileSelector)
// ---------------------------------------------------------------------------
describe('enumerateInstructionCandidates', () => {
  it('marks an absent project entrypoint as not-existing at the canonical CLAUDE.md path', () => {
    const mgr = new MemoryManager(cwd);
    const cands = mgr.enumerateInstructionCandidates();
    const project = cands.find(c => c.scope === 'project')!;
    assert.ok(project);
    assert.equal(project.exists, false);
    assert.equal(path.basename(project.path), 'CLAUDE.md');
    assert.equal(project.path, path.join(cwd, 'CLAUDE.md'));
    // The user candidate is always present with a boolean exists flag.
    const user = cands.find(c => c.scope === 'user')!;
    assert.ok(user);
    assert.equal(typeof user.exists, 'boolean');
  });

  it('reports an existing project instruction file at its real discovered path', () => {
    fs.writeFileSync(path.join(cwd, 'AGENTS.md'), '# rules');
    const mgr = new MemoryManager(cwd);
    const project = mgr.enumerateInstructionCandidates().find(c => c.scope === 'project')!;
    assert.equal(project.exists, true);
    assert.equal(project.path, path.join(cwd, 'AGENTS.md'));
  });
});

// ---------------------------------------------------------------------------
// Bounded header-only scan (parity: memoryScan.ts FRONTMATTER_MAX_LINES / scanMemoryFiles)
// ---------------------------------------------------------------------------
describe('scanMemoryHeaders', () => {
  it('FRONTMATTER_MAX_LINES is the documented 30-line bound', () => {
    assert.equal(FRONTMATTER_MAX_LINES, 30);
  });

  it('returns header metadata (name/description/type/bytes/mtime) without the body', () => {
    const mgr = new MemoryManager(cwd);
    mgr.saveMemory('topic-one', 'Body paragraph one.', 'the first topic', 'feedback');
    const headers = mgr.scanMemoryHeaders();
    assert.equal(headers.length, 1);
    const h = headers[0];
    assert.equal(h.name, 'topic-one');
    assert.equal(h.description, 'the first topic');
    assert.equal(h.memoryType, 'feedback');
    assert.ok(h.bytes > 0);
    assert.ok(h.mtimeMs > 0);
  });

  it('parses the frontmatter even when the body is far larger than the read window', () => {
    const mgr = new MemoryManager(cwd);
    // A huge body that must NOT be slurped — only the leading frontmatter is parsed.
    mgr.saveMemory('big-topic', 'X'.repeat(300_000), 'a big one', 'project');
    const header = mgr.scanMemoryHeaders().find(x => x.name === 'big-topic');
    assert.ok(header);
    assert.equal(header!.description, 'a big one');
  });

  it('excludes the MEMORY.md index and the team/ subtree, newest first', () => {
    process.env.AMI_ENABLE_TEAM_MEMORY = '1';
    const mgr = new MemoryManager(cwd);
    // Create the index + a team memory; neither should appear in the header scan.
    fs.writeFileSync(path.join(memDir, 'MEMORY.md'), '# index\n- pointer');
    mgr.saveMemory('team-one', 'shared', 'team note', 'project', 'team');
    mgr.saveMemory('private-one', 'mine', 'private note', 'project');
    const names = mgr.scanMemoryHeaders().map(h => h.name);
    assert.ok(names.includes('private-one'));
    assert.ok(!names.includes('MEMORY'));
    assert.ok(!names.includes('team-one'));
  });

  it('returns an empty list when the memory dir does not exist yet', () => {
    // Point the store at a path that was never created.
    process.env.AMI_MEMORY_DIR = path.join(memDir, 'never-created');
    const mgr = new MemoryManager(cwd);
    assert.deepEqual(mgr.scanMemoryHeaders(), []);
  });
});
