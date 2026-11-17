import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { classifyToolResultState } from '../src/message-display';
import type { ToolContext } from '../src/types';

// ===========================================================================
// U006-w84 — file_edit tags RECOVERABLE validation failures with behavior:'ask'.
//
// Parity with Claude Code's FileEditTool, which classifies most edit-validation
// failures (identical strings, not-read, too-large, string-not-found,
// multiple-matches, modified-since-read) as `behavior: 'ask'` (fix-and-retry)
// rather than a fatal error. AMI previously collapsed all of these into a single
// hard `isError` with no soft/fatal distinction.
// ===========================================================================

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-ask-u006-'));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('U006-w84 — file_edit behavior:ask recoverable outcomes', () => {
  it('identical old_string/new_string is a recoverable ask, not a fatal error', async () => {
    const f = path.join(tmpDir, 'a.txt');
    fs.writeFileSync(f, 'hello world');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'hello', new_string: 'hello' },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.equal(r.behavior, 'ask');
    assert.equal(classifyToolResultState(r), 'ask');
  });

  it('editing a file that was never file_read is a recoverable ask', async () => {
    const f = path.join(tmpDir, 'b.txt');
    fs.writeFileSync(f, 'alpha beta');
    // filesRead tracking is ACTIVE but empty => the read-before-edit guard fires.
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'alpha', new_string: 'gamma' },
      ctx({ filesRead: new Set<string>() }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /must file_read/i);
    assert.equal(r.behavior, 'ask');
    assert.equal(classifyToolResultState(r), 'ask');
  });

  it('old_string not found in the file is a recoverable ask', async () => {
    const f = path.join(tmpDir, 'c.txt');
    fs.writeFileSync(f, 'the quick brown fox');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'nonexistent-span', new_string: 'x' },
      ctx({ filesRead: new Set<string>([f]) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /not found/i);
    assert.equal(r.behavior, 'ask');
    assert.equal(classifyToolResultState(r), 'ask');
  });

  it('multiple matches without replace_all is a recoverable ask', async () => {
    const f = path.join(tmpDir, 'd.txt');
    fs.writeFileSync(f, 'dup\ndup\ndup\n');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'dup', new_string: 'uniq' },
      ctx({ filesRead: new Set<string>([f]) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /locations/i);
    assert.equal(r.behavior, 'ask');
    assert.equal(classifyToolResultState(r), 'ask');
  });

  it('a hard/fatal validation error (empty file_path) is NOT tagged ask', async () => {
    const r = await fileEditTool.execute(
      { file_path: '', old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(r.isError, true);
    assert.notEqual(r.behavior, 'ask');
    assert.equal(classifyToolResultState(r), 'error');
  });
});
