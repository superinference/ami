import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { MemoryManager, computeIndexAppend } from '../src/memory';
import { writeMemoryTool } from '../src/tools/write-memory';
import { readMemoryTool } from '../src/tools/read-memory';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U016 / w65 — memory.ts parity additions:
//  - persistence guidance ("Memory and other forms of persistence")
//  - empty-memory affordance in getMemoryContext
//  - two-step save (file + MEMORY.md index pointer) via saveMemory(addToIndex)
//    wired end-to-end through the write_memory tool
//
// The auto-memory store is pinned to an isolated temp dir via AMI_MEMORY_DIR,
// and HOME is isolated so loadUserGlobalInstructions never reads the real
// ~/.ami or ~/.claude of the test machine.
// ---------------------------------------------------------------------------

let cwdDir: string;
let memDir: string;
let homeDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedDisableEnv = process.env.AMI_DISABLE_AUTO_MEMORY;
const savedHome = process.env.HOME;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeMemoryFile(name: string, body: string): void {
  fs.mkdirSync(memDir, { recursive: true });
  fs.writeFileSync(
    path.join(memDir, `${name}.md`),
    `---\nname: ${name}\ndescription: ${name} desc\ntype: project\n---\n\n${body}\n`,
    'utf-8',
  );
}

beforeEach(() => {
  cwdDir = mkTmp('si-u016w65-cwd-');
  memDir = mkTmp('si-u016w65-store-');
  homeDir = mkTmp('si-u016w65-home-');
  process.env.AMI_MEMORY_DIR = memDir;
  process.env.HOME = homeDir;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  for (const d of [cwdDir, memDir, homeDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR; else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedDisableEnv === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY; else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisableEnv;
  if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
});

const toolCtx = (): ToolContext =>
  ({ cwd: cwdDir, abortSignal: new AbortController().signal } as unknown as ToolContext);

// ---------------------------------------------------------------------------
// computeIndexAppend — shared pure index-append rule
// ---------------------------------------------------------------------------

describe('computeIndexAppend', () => {
  it('appends a new entry to an empty index', () => {
    assert.equal(computeIndexAppend('', '- [a](a.md) — x'), '\n- [a](a.md) — x\n');
  });

  it('appends after existing content, trimming trailing whitespace', () => {
    assert.equal(
      computeIndexAppend('- [a](a.md) — x\n\n\n', '- [b](b.md) — y'),
      '- [a](a.md) — x\n- [b](b.md) — y\n',
    );
  });

  it('returns null (dedup) when the entry is already present', () => {
    assert.equal(computeIndexAppend('- [a](a.md) — x\n', '- [a](a.md) — x'), null);
  });
});

// ---------------------------------------------------------------------------
// Persistence guidance
// ---------------------------------------------------------------------------

describe('getMemoryContext — "Memory and other forms of persistence" guidance', () => {
  it('steers ephemeral approach/progress state toward plans and tasks', () => {
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /### Memory and other forms of persistence/);
    assert.match(ctx, /plan/i);
    assert.match(ctx, /tasks/i);
    assert.match(ctx, /FUTURE conversations/);
  });
});

// ---------------------------------------------------------------------------
// Empty-memory affordance
// ---------------------------------------------------------------------------

describe('getMemoryContext — empty-memory affordance', () => {
  it('surfaces the empty-MEMORY.md affordance when instructions exist but no memories', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules\nAlways run tests.', 'utf-8');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(ctx, /## Project Instructions/);
    assert.match(ctx, /is currently empty\. When you save new memories/);
    // No memory bodies, so the Stored Memories section must be absent.
    assert.doesNotMatch(ctx, /## Stored Memories/);
  });

  it('preserves the empty-string contract when there is no content at all', () => {
    // No instruction files, no memories, HOME isolated so no user-global file.
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.equal(ctx, '');
  });

  it('does NOT show the empty affordance once memories exist', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules', 'utf-8');
    writeMemoryFile('alpha', 'alpha body content');
    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.doesNotMatch(ctx, /is currently empty/);
    assert.match(ctx, /## Stored Memories/);
  });
});

// ---------------------------------------------------------------------------
// Two-step save (file + MEMORY.md index pointer)
// ---------------------------------------------------------------------------

describe('saveMemory — two-step index pointer (addToIndex)', () => {
  it('default save writes only the topic file, no MEMORY.md index', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('deploy-cmd', 'run deploy.sh', 'how to deploy', 'reference');
    assert.ok(fs.existsSync(path.join(memDir, 'deploy-cmd.md')));
    assert.ok(!fs.existsSync(path.join(memDir, 'MEMORY.md')), 'no index written by default');
  });

  it('addToIndex=true writes the topic file AND a one-line MEMORY.md pointer', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('deploy-cmd', 'run deploy.sh', 'how to deploy', 'reference', 'private', true);
    const index = fs.readFileSync(path.join(memDir, 'MEMORY.md'), 'utf-8');
    assert.match(index, /- \[deploy-cmd\]\(deploy-cmd\.md\) — how to deploy/);
  });

  it('dedups: saving the same pointer twice keeps a single index line', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('note', 'body one', 'desc', 'project', 'private', true);
    mgr.saveMemory('note', 'body two', 'desc', 'project', 'private', true);
    const index = fs.readFileSync(path.join(memDir, 'MEMORY.md'), 'utf-8');
    const occurrences = index.split('\n').filter(l => l.includes('[note](note.md)')).length;
    assert.equal(occurrences, 1);
  });

  it('team scope indexes a relative team/ pointer at the root index', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('ci-runner', 'CI uses arm64', 'ci pool', 'reference', 'team', true);
    // File lands under the team subtree
    assert.ok(fs.existsSync(path.join(memDir, 'team', 'ci-runner.md')));
    // Pointer lives in the root MEMORY.md and references the team/ path
    const index = fs.readFileSync(path.join(memDir, 'MEMORY.md'), 'utf-8');
    assert.match(index, /- \[ci-runner\]\(team\/ci-runner\.md\) — ci pool/);
  });

  it('the MEMORY.md index is excluded from the memory-body scan', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('note', 'body', 'desc', 'project', 'private', true);
    const names = mgr.loadMemories().map(m => m.name);
    assert.ok(names.includes('note'));
    assert.ok(!names.some(n => (n || '').toLowerCase().includes('memory')), 'MEMORY.md is not loaded as a memory body');
  });
});

// ---------------------------------------------------------------------------
// E2E — write_memory tool drives the full two-step save path
// ---------------------------------------------------------------------------

describe('write_memory tool — end-to-end two-step save', () => {
  it('persists the file, writes the MEMORY.md pointer, and reads back via read_memory', async () => {
    const r = await writeMemoryTool.execute(
      { name: 'build-step', content: 'npm run build', description: 'build command', type: 'reference' },
      toolCtx(),
    );
    assert.notEqual(r.isError, true);
    assert.match(String(r.output), /indexed it in MEMORY\.md/);

    // The MEMORY.md index now carries the one-line pointer.
    const index = fs.readFileSync(path.join(memDir, 'MEMORY.md'), 'utf-8');
    assert.match(index, /- \[build-step\]\(build-step\.md\) — build command/);

    // The body is still readable through read_memory (index excluded from bodies).
    const back = await readMemoryTool.execute({ name: 'build-step' }, toolCtx());
    assert.match(String(back.output), /npm run build/);
  });
});
