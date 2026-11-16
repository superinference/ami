import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { MemoryManager } from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

// Parity gates for the Memory Guidelines block emitted by getMemoryContext().
// Mirrors Claude Code's memdir/memoryTypes.ts sections:
//   TYPES_SECTION_INDIVIDUAL, WHAT_NOT_TO_SAVE_SECTION, WHEN_TO_ACCESS_SECTION,
//   TRUSTING_RECALL_SECTION (## Before recommending from memory),
//   body_structure guidance + MEMORY_FRONTMATTER_EXAMPLE.

let tmpDir: string;

function withMemory(): MemoryManager {
  const memDir = projectStateDir(tmpDir, 'memory');
  fs.mkdirSync(memDir, { recursive: true });
  fs.writeFileSync(
    path.join(memDir, 'seed.md'),
    '---\nname: seed\ndescription: seed memory\ntype: project\n---\n\nSeed body so the guidance block is emitted.',
  );
  return new MemoryManager(tmpDir);
}

describe('getMemoryContext — memory-type taxonomy guidance (gap F0182)', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('emits per-type when_to_save / how_to_use guidance for all four types', () => {
    const ctx = withMemory().getMemoryContext();
    // each type still named
    for (const t of ['user', 'feedback', 'project', 'reference']) {
      assert.ok(ctx.includes(`<name>${t}</name>`), `missing <name>${t}</name>`);
    }
    assert.ok(ctx.includes('<when_to_save>'), 'missing when_to_save guidance');
    assert.ok(ctx.includes('<how_to_use>'), 'missing how_to_use guidance');
  });

  it('includes concrete worked examples per type (eval-validated cues)', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('data scientist'), 'missing user worked example');
    assert.ok(ctx.includes('[saves feedback memory:'), 'missing feedback worked example');
    assert.ok(ctx.includes('merge freeze'), 'missing project worked example');
    assert.ok(ctx.includes('INGEST'), 'missing reference worked example');
  });
});

describe('getMemoryContext — What NOT to save + explicit-save gate (gap)', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('lists what NOT to save with per-item reasons', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('What NOT to save'), 'missing What NOT to save header');
    assert.ok(/git log/i.test(ctx), 'missing git log authority bullet');
    assert.ok(ctx.includes('Debugging solutions'), 'missing debugging-recipe exclusion');
  });

  it('includes the explicit-save gate sentence', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(
      ctx.includes('even when the user explicitly asks you to save'),
      'missing explicit-save gate',
    );
    assert.ok(ctx.includes('surprising'), 'missing surprising/non-obvious redirect');
  });
});

describe('getMemoryContext — Memory scope (private vs team) guidance', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('emits a Memory scope section distinguishing private vs team', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('Memory scope'), 'missing Memory scope header');
    assert.ok(/\bprivate\b/.test(ctx), 'missing private scope');
    assert.ok(/\bteam\b/.test(ctx), 'missing team scope');
    assert.ok(/synced at the start of every session/i.test(ctx), 'missing team-sync description');
  });

  it('warns never to put secrets/credentials in a shared team memory', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(
      /never save API keys or user credentials/i.test(ctx),
      'missing team-memory sensitive-data warning',
    );
  });
});

describe('getMemoryContext — When to access memories (gap)', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('includes a When to access section with the MUST-access rule', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('When to access memories'), 'missing When to access header');
    assert.ok(/MUST access memory/i.test(ctx), 'missing explicit must-access rule');
  });

  it('includes the ignore-memory anti-pattern bullet', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(
      /ignore/i.test(ctx) && ctx.includes('as if MEMORY.md were empty'),
      'missing ignore-memory anti-pattern handling',
    );
  });
});

describe('getMemoryContext — Before recommending from memory (gap)', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('includes the trusting-recall verification checklist', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('Before recommending from memory'), 'missing section header');
    assert.ok(ctx.includes('check the file exists'), 'missing file-exists check');
    assert.ok(/grep for it/i.test(ctx), 'missing grep-the-symbol check');
    assert.ok(
      ctx.includes('is not the same as'),
      'missing "says X exists" vs "X exists now" caveat',
    );
  });
});

describe('getMemoryContext — body-structure + frontmatter guidance (gap)', () => {
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-guid-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} });

  it('includes **Why:** / **How to apply:** body scaffolding', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('**Why:**'), 'missing Why: scaffolding');
    assert.ok(ctx.includes('**How to apply:**'), 'missing How to apply: scaffolding');
    assert.ok(ctx.includes('<body_structure>'), 'missing body_structure guidance');
  });

  it('shows a frontmatter example naming all memory types', () => {
    const ctx = withMemory().getMemoryContext();
    assert.ok(ctx.includes('```markdown'), 'missing frontmatter example fence');
    assert.ok(ctx.includes('user, feedback, project, reference'), 'missing type enum in example');
  });
});
