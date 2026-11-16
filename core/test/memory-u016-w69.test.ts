/**
 * U016 (wave 69) — memory.ts parity gaps:
 *  - Recursive team-memory scan (nested subdirectories discovered)
 *    (parity: memdir/memoryScan.ts readdir({recursive:true}))
 *  - Aggregate file-count cap on the shared/untrusted team store
 *    (parity: memdir/memdir.ts hard caps on untrusted memory content)
 *  - Exact ISO-8601 mtime in the stored-memory index manifest
 *    (parity: memdir/memoryScan.ts formatMemoryManifest)
 *  - Per-type <scope> guidance + private/team contradiction check
 *    (parity: memdir/memoryTypes.ts COMBINED types section)
 *  - Scope-tailored write guidance (private general / team project-specific)
 *    (parity: AgentTool/agentMemory.ts loadAgentMemoryPrompt)
 *  - resolveAtImports skips INDENTED code blocks + fires a per-@import
 *    instructionsLoaded hook with loadReason 'include'
 *    (parity: utils/claudemd.ts lexer-token @include extraction + load reasons)
 *  - Per-source provenance/trust labels on instruction sections +
 *    differentiated InstructionsLoaded load_reason (session_start /
 *    nested_traversal) and memory_type (Project/Local)
 *    (parity: utils/claudemd.ts getClaudeMds + sdk/coreSchemas.ts)
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w69.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { MemoryManager } from '../src/memory';
import { serializeHookEvent } from '../src/hooks';

let cwdDir: string;
let memDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedDisableEnv = process.env.AMI_DISABLE_AUTO_MEMORY;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writePrivateMemory(name: string, body: string, type = 'project'): void {
  const p = path.join(memDir, `${name}.md`);
  fs.writeFileSync(p, `---\nname: ${name}\ndescription: ${name} desc\ntype: ${type}\n---\n\n${body}\n`, 'utf-8');
}

function writeTeamFile(rel: string, body: string): string {
  const p = path.join(memDir, 'team', rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `---\nname: ${path.basename(rel, '.md')}\ndescription: d\ntype: project\n---\n\n${body}\n`, 'utf-8');
  return p;
}

beforeEach(() => {
  cwdDir = mkTmp('si-mem-w69-cwd-');
  memDir = mkTmp('si-mem-w69-store-');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  try { fs.rmSync(cwdDir, { recursive: true, force: true }); } catch { /* ignore */ }
  try { fs.rmSync(memDir, { recursive: true, force: true }); } catch { /* ignore */ }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedDisableEnv === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY;
  else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisableEnv;
});

// ── Recursive team-memory scan ───────────────────────────────────────────────
describe('U016-w69 — recursive team-memory scan', () => {
  it('discovers a team memory in a nested subdirectory', () => {
    writeTeamFile('top.md', 'top-level team note');
    writeTeamFile(path.join('conventions', 'nested.md'), 'nested-team-note-marker');
    const loaded = new MemoryManager(cwdDir).loadTeamMemories();
    assert.ok(loaded.some(m => m.includes('top-level team note')), 'top-level team memory loads');
    assert.ok(loaded.some(m => m.includes('nested-team-note-marker')), 'nested team memory must be discovered');
  });

  it('excludes a nested team/.../MEMORY.md index by basename', () => {
    writeTeamFile('real.md', 'a real team memory');
    const idx = path.join(memDir, 'team', 'sub', 'MEMORY.md');
    fs.mkdirSync(path.dirname(idx), { recursive: true });
    fs.writeFileSync(idx, '- [x](x.md) — index entry not a body\n', 'utf-8');
    const loaded = new MemoryManager(cwdDir).loadTeamMemories();
    assert.ok(loaded.some(m => m.includes('a real team memory')));
    assert.ok(!loaded.some(m => m.includes('index entry not a body')), 'MEMORY.md index must not load as a body');
  });
});

// ── Aggregate team-memory file-count cap ─────────────────────────────────────
describe('U016-w69 — team-memory aggregate count cap', () => {
  it('caps the number of loaded team files at 200', () => {
    for (let i = 0; i < 205; i++) writeTeamFile(`t${i}.md`, `team body ${i}`);
    const loaded = new MemoryManager(cwdDir).loadTeamMemories();
    assert.equal(loaded.length, 200, 'team store must be bounded to 200 files');
  });
});

// ── ISO mtime in the stored-memory index manifest ────────────────────────────
describe('U016-w69 — ISO mtime in index manifest', () => {
  it('renders an exact ISO-8601 timestamp alongside the freshness bucket', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules\nBe precise.', 'utf-8');
    writePrivateMemory('alpha', 'x'.repeat(50));
    writePrivateMemory('beta', 'y'.repeat(50));
    // storedInlineBudget 0 forces the compact INDEX rendering path.
    const ctx = new MemoryManager(cwdDir).getMemoryContext({ storedInlineBudget: 0 });
    assert.match(ctx, /Stored Memories \(index/, 'index path must be selected');
    assert.match(ctx, /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/, 'an ISO-8601 timestamp must appear in the manifest');
  });
});

// ── Per-type <scope> + contradiction + scope-tailored guidance ───────────────
describe('U016-w69 — scope guidance in the memory prompt', () => {
  function ctxWithMemory(): string {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules', 'utf-8');
    writePrivateMemory('m', 'some body');
    return new MemoryManager(cwdDir).getMemoryContext();
  }
  it('emits a <scope> directive per memory type', () => {
    const ctx = ctxWithMemory();
    assert.ok(ctx.includes('<scope>Always private'), 'user type scope');
    assert.ok(ctx.includes('Bias toward team'), 'project type scope');
    assert.match(ctx, /<scope>Usually team/, 'reference type scope');
  });
  it('includes the private/team contradiction check', () => {
    const ctx = ctxWithMemory();
    assert.match(ctx, /contradict/i, 'contradiction guidance must be present');
  });
  it('includes scope-tailored write guidance', () => {
    const ctx = ctxWithMemory();
    assert.ok(ctx.includes('keep **private** memories general'), 'scope-tailored wording must be present');
  });
});

// ── resolveAtImports: indented-code skip + include hook ──────────────────────
describe('U016-w69 — resolveAtImports indented code + include hook', () => {
  it('does NOT inline an @path inside an indented (4-space) code block', () => {
    fs.writeFileSync(path.join(cwdDir, 'secret.md'), 'SECRET-IMPORT-BODY', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    const base = path.join(cwdDir, 'CLAUDE.md');
    const indented = mgr.resolveAtImports('    @secret.md', base);
    assert.ok(!indented.includes('SECRET-IMPORT-BODY'), 'indented @path must not be inlined');
    assert.ok(indented.includes('@secret.md'), 'the literal indented text is preserved');
    // Control: a non-indented @path IS inlined.
    const normal = mgr.resolveAtImports('@secret.md', base);
    assert.ok(normal.includes('SECRET-IMPORT-BODY'), 'non-indented @path is still inlined');
  });

  it('fires an instructionsLoaded hook with loadReason "include" per inlined import', () => {
    fs.writeFileSync(path.join(cwdDir, 'child.md'), 'CHILD-BODY', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));
    const out = mgr.resolveAtImports('@child.md', path.join(cwdDir, 'CLAUDE.md'));
    assert.ok(out.includes('CHILD-BODY'));
    const inc = events.find(e => e.event === 'instructionsLoaded' && e.data.loadReason === 'include');
    assert.ok(inc, 'an include load reason must fire for a successful @-import');
  });
});

// ── Provenance labels + differentiated load reasons ──────────────────────────
describe('U016-w69 — provenance labels + load reasons', () => {
  it('labels a project instruction file and fires session_start/Project', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules\nBe terse.', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));
    const out = mgr.loadProjectInstructions();
    assert.match(out, /## AGENTS\.md \(project instructions, checked into the codebase\)/);
    const ev = events.find(e => e.event === 'instructionsLoaded' && e.data.filePath.endsWith('AGENTS.md'));
    assert.ok(ev, 'an instructionsLoaded event must fire');
    assert.equal(ev!.data.loadReason, 'session_start');
    assert.equal(ev!.data.memoryType, 'Project');
  });

  it('labels a local variant as private-not-checked-in with memory_type Local', () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.local.md'), '# local\nmy local rule', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));
    const out = mgr.loadProjectInstructions();
    assert.match(out, /CLAUDE\.local\.md \(private project instructions, not checked in\)/);
    const ev = events.find(e => e.data?.filePath?.endsWith('CLAUDE.local.md'));
    assert.ok(ev);
    assert.equal(ev!.data.memoryType, 'Local');
  });

  it('fires nested_traversal for a parent-directory instruction file', () => {
    const parent = mkTmp('si-mem-w69-parent-');
    const child = path.join(parent, 'child');
    fs.mkdirSync(child);
    fs.writeFileSync(path.join(parent, 'AGENTS.md'), '# parent rules', 'utf-8');
    const mgr = new MemoryManager(child);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));
    mgr.loadProjectInstructions();
    const ev = events.find(e => e.data?.filePath?.endsWith('AGENTS.md'));
    assert.ok(ev, 'parent instruction file must load');
    assert.equal(ev!.data.loadReason, 'nested_traversal');
    try { fs.rmSync(parent, { recursive: true, force: true }); } catch { /* ignore */ }
  });
});

// ── hook serialization snake_case aliases ────────────────────────────────────
describe('U016-w69 — instructionsLoaded hook serialization', () => {
  it('emits load_reason and memory_type snake_case aliases', () => {
    const parsed = JSON.parse(serializeHookEvent({
      event: 'instructionsLoaded', filePath: '/x/AGENTS.md', loadReason: 'session_start', memoryType: 'Project',
    }));
    assert.equal(parsed.load_reason, 'session_start');
    assert.equal(parsed.memory_type, 'Project');
    assert.equal(parsed.loadReason, 'session_start', 'camelCase kept for back-compat');
  });
});
