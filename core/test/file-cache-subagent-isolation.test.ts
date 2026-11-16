import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { fileEditTool } from '../src/tools/file-edit';
import { getFileCache, releaseFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// End-to-end wiring: a forked sub-agent (ToolContext.agentId set) drives the
// REAL file_read / file_edit tools against an ISOLATED, clone-on-fork file
// cache — its reads/edits never mutate the parent's directory-shared cache.
// ---------------------------------------------------------------------------
let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fc-iso-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('file tools — per-subagent cache isolation (end-to-end)', () => {
  it("a sub-agent's file_read lands in its isolated cache, not the parent shared cache", async () => {
    const parentFile = path.join(tmpDir, 'parent.txt');
    const childFile = path.join(tmpDir, 'child.txt');
    fs.writeFileSync(parentFile, 'parent content\n');
    fs.writeFileSync(childFile, 'child content\n');

    // Parent (no agentId) reads parentFile → shared dir cache.
    await fileReadTool.execute({ file_path: parentFile }, ctx());
    const shared = getFileCache(tmpDir);
    assert.ok(shared.listPaths().includes(parentFile), 'parent read is in the shared cache');

    // Sub-agent (agentId set) reads childFile → isolated cache only.
    await fileReadTool.execute({ file_path: childFile }, ctx({ agentId: 'sub-1' }));
    const childCache = getFileCache(tmpDir, 'sub-1');
    assert.ok(childCache.listPaths().includes(childFile), "child read is in the sub-agent's cache");
    // Isolation: the child read never leaked into the parent's shared cache.
    assert.equal(shared.listPaths().includes(childFile), false);
    // Clone-on-fork: the child inherited the parent's already-read file.
    assert.ok(childCache.listPaths().includes(parentFile), 'clone-on-fork inherited the parent read');

    releaseFileCache(tmpDir, 'sub-1');
  });

  it("a sub-agent's file_edit updates only its own cache; the parent is forced to re-read", async () => {
    const shared = path.join(tmpDir, 'shared.txt');
    fs.writeFileSync(shared, 'original line\n');

    // Parent reads it (shared cache now holds original bytes + mtime).
    await fileReadTool.execute({ file_path: shared }, ctx());
    const parentCache = getFileCache(tmpDir);
    assert.equal(parentCache.hasChanged(shared), false);

    // Sub-agent reads then edits the same file.
    const childCtx = ctx({ agentId: 'sub-2' });
    await fileReadTool.execute({ file_path: shared }, childCtx);
    const editResult = await fileEditTool.execute(
      { file_path: shared, old_string: 'original line', new_string: 'rewritten by child' },
      childCtx,
    );
    assert.equal(editResult.isError, undefined, 'child edit succeeds after its own read');

    // The sub-agent's isolated cache reflects its write (unchanged vs disk)...
    const childCache = getFileCache(tmpDir, 'sub-2');
    assert.equal(childCache.hasChanged(shared), false);
    // ...but the PARENT's shared cache still holds the pre-edit mtime, so it now
    // detects the on-disk change and will force a fresh read before any edit —
    // exactly the read-before-edit safety the isolation preserves.
    assert.equal(parentCache.hasChanged(shared), true);

    releaseFileCache(tmpDir, 'sub-2');
  });

  it('releaseFileCache drops the isolated cache so a later fork re-clones fresh', async () => {
    const f = path.join(tmpDir, 'f.txt');
    fs.writeFileSync(f, 'v1\n');
    await fileReadTool.execute({ file_path: f }, ctx({ agentId: 'sub-3' }));
    const first = getFileCache(tmpDir, 'sub-3');
    releaseFileCache(tmpDir, 'sub-3');
    const second = getFileCache(tmpDir, 'sub-3');
    assert.notEqual(first, second, 'a released agent id yields a fresh clone on next access');
    releaseFileCache(tmpDir, 'sub-3');
  });
});
