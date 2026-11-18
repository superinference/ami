/**
 * U637 (w118) — memory file-access COUNTING + glob/pattern classification.
 *
 * Parity gaps (Claude Code utils/attribution.ts `countMemoryFileAccessFromEntries`
 * + MEMORY_ACCESS_TOOL_NAMES, and utils/memoryFileDetection.ts
 * `isAutoManagedMemoryPattern`):
 *  - a transcript walker that counts memory-file accesses across read/grep/glob/
 *    edit/write tool_use blocks via a tool-name allowlist + the shared
 *    isMemoryFileAccess predicate;
 *  - a glob/pattern classifier for auto-managed memory globs;
 *  - file_write added to the path-taking predicate domain.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import {
  classifyToolSessionFileType,
  isMemoryFileAccess,
  countMemoryFileAccessFromEntries,
  isAutoManagedMemoryPattern,
  MEMORY_ACCESS_TOOL_NAMES,
  type MemoryDirs,
  type MemoryAccessEntry,
} from '../src/memory-file-detection';
import { MemoryManager } from '../src/memory';

const dirs: MemoryDirs = {
  memoryDir: '/home/u/.ami/projects/proj/memory',
  transcriptDir: '/home/u/.ami/projects/proj/sessions',
};

const memFile = '/home/u/.ami/projects/proj/memory/topic.md';
const memSession = '/home/u/.ami/projects/proj/memory/session-9.md';
const nonMem = '/home/u/project/src/index.ts';

/** Build an assistant transcript entry carrying the given tool_use blocks. */
function assistant(blocks: { name: string; input: unknown }[]): MemoryAccessEntry {
  return {
    type: 'assistant',
    message: { content: blocks.map((b) => ({ type: 'tool_use', name: b.name, input: b.input })) },
  };
}

describe('file_write joins the path-taking predicate domain', () => {
  it('classifies and counts a file_write to a memory/session file', () => {
    assert.equal(
      classifyToolSessionFileType('file_write', { file_path: memSession }, dirs),
      'session_memory',
    );
    assert.equal(isMemoryFileAccess('file_write', { file_path: memFile }, dirs), true);
    assert.equal(isMemoryFileAccess('file_write', { file_path: nonMem }, dirs), false);
  });
});

describe('MEMORY_ACCESS_TOOL_NAMES', () => {
  it('is the read/grep/glob/edit/write allowlist', () => {
    assert.deepEqual(
      [...MEMORY_ACCESS_TOOL_NAMES].sort(),
      ['file_edit', 'file_read', 'file_write', 'glob', 'grep'].sort(),
    );
  });
});

describe('countMemoryFileAccessFromEntries', () => {
  it('counts memory accesses across read/grep/glob/edit/write, skipping others', () => {
    const entries: MemoryAccessEntry[] = [
      assistant([
        { name: 'file_read', input: { file_path: memFile } },   // +1
        { name: 'file_edit', input: { file_path: memFile } },   // +1
        { name: 'file_write', input: { file_path: memSession } }, // +1
        { name: 'grep', input: { path: memSession } },          // +1 (session_memory)
        { name: 'file_read', input: { file_path: nonMem } },    // skip (not memory)
        { name: 'bash', input: { command: 'ls' } },             // skip (not allowlisted)
      ]),
    ];
    assert.equal(countMemoryFileAccessFromEntries(entries, dirs), 4);
  });

  it('only walks assistant entries with an array content, ignoring malformed blocks', () => {
    const entries: MemoryAccessEntry[] = [
      { type: 'user', message: { content: [{ type: 'tool_use', name: 'file_read', input: { file_path: memFile } }] } },
      { type: 'assistant', message: { content: 'not-an-array' } },
      { type: 'assistant', message: null },
      assistant([{ name: 'file_read', input: { file_path: memFile } }]), // +1
      // malformed blocks inside a valid entry:
      { type: 'assistant', message: { content: [null, 42, { type: 'text', text: 'x' }, { type: 'tool_use', name: 123, input: {} }] } },
    ];
    // null entry in the array must not throw.
    assert.equal(countMemoryFileAccessFromEntries([...entries, null as unknown as MemoryAccessEntry], dirs), 1);
  });

  it('applies the normalizeToolName map so reference-named journal entries count', () => {
    // Reference vocabulary (Read/Grep/Edit) — identity default would skip them.
    const entries: MemoryAccessEntry[] = [
      assistant([
        { name: 'Read', input: { file_path: memFile } },
        { name: 'Edit', input: { file_path: memSession } },
      ]),
    ];
    assert.equal(countMemoryFileAccessFromEntries(entries, dirs), 0, 'identity default skips ref names');
    const refToAmi: Record<string, string> = { Read: 'file_read', Edit: 'file_edit', Grep: 'grep', Glob: 'glob', Write: 'file_write' };
    assert.equal(
      countMemoryFileAccessFromEntries(entries, dirs, (n) => refToAmi[n] ?? n),
      2,
    );
  });
});

describe('isAutoManagedMemoryPattern', () => {
  it('is true for session-memory and session-transcript glob intents', () => {
    assert.equal(isAutoManagedMemoryPattern('session-*.md'), true);
    assert.equal(isAutoManagedMemoryPattern('**/*.jsonl'), true);
  });
  it('is true for agent-memory globs (local/remote/generic)', () => {
    assert.equal(isAutoManagedMemoryPattern('agent-memory/**'), true);
    assert.equal(isAutoManagedMemoryPattern('**/agent-memory-local/*.md'), true);
    assert.equal(isAutoManagedMemoryPattern('x/agent-memory-remote/y'), true);
    // Windows-style separators normalize.
    assert.equal(isAutoManagedMemoryPattern('a\\agent-memory\\b'), true);
  });
  it('is false for an ordinary or empty pattern', () => {
    assert.equal(isAutoManagedMemoryPattern('**/*.ts'), false);
    assert.equal(isAutoManagedMemoryPattern('src/memory-notes/*.md'), false);
    assert.equal(isAutoManagedMemoryPattern(''), false);
  });
});

describe('MemoryManager wrappers (real workspace dirs)', () => {
  it('counts accesses and classifies patterns against the resolved memory dir', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mfd637-'));
    try {
      const mm = new MemoryManager(dir);
      const liveMem = path.join(mm.getMemoryDir(), 'session-live.md');
      const entries: MemoryAccessEntry[] = [
        {
          type: 'assistant',
          message: {
            content: [
              { type: 'tool_use', name: 'file_write', input: { file_path: liveMem } },
              { type: 'tool_use', name: 'file_read', input: { file_path: path.join(dir, 'README.md') } },
            ],
          },
        },
      ];
      assert.equal(mm.countMemoryFileAccessFromEntries(entries), 1);
      assert.equal(mm.isAutoManagedMemoryPattern('session-*.md'), true);
      assert.equal(mm.isAutoManagedMemoryPattern('**/*.ts'), false);
    } finally {
      await fsp.rm(dir, { recursive: true, force: true });
    }
  });
});
