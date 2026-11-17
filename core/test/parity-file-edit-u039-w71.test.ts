// Unit U039 (wave w71) — additive parity gaps for core/core/src/tools/file-edit.ts:
//  - Team-memory-scoped secret guard: a write that would introduce a secret into
//    a file under the SHARED team-memory tree is blocked with a distinct,
//    collaborator-aware message (parity: Claude Code's teamMemSecretGuard, which
//    scopes a stricter "shared with all repository collaborators" block to
//    team-memory paths). Any other path keeps the generic repository-scoped
//    warning. Reuses the ONE canonical classifier MemoryManager.isTeamMemFile.
//  - Accurate 1-based edit location: the single-replace `[edit: …]` patch-info
//    tag now carries the TRUE 1-based line where the change landed (the first
//    hunk's oldStart), so the model/UI can locate the edit without re-reading the
//    file (parity: Claude Code's readEditContext lineOffset).
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;
let prevMemDir: string | undefined;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    detachedMode: true,
    ...overrides,
  };
}

const SECRET_LINE = 'password = "my-secret-password-123"';

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w71-')));
  prevMemDir = process.env.AMI_MEMORY_DIR;
  // Pin the memory tree UNDER the workspace so the team-memory dir is a real,
  // in-workspace path the edit tool will accept (and classify as team memory).
  process.env.AMI_MEMORY_DIR = path.join(tmpDir, '.mem');
});

afterEach(() => {
  if (prevMemDir === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = prevMemDir;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('fileEditTool – team-memory-scoped secret guard', () => {
  it('blocks a secret written into a team-memory file with a collaborator-aware message', async () => {
    const teamDir = path.join(tmpDir, '.mem', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    const file = path.join(teamDir, 'shared.md');
    fs.writeFileSync(file, 'line one\nline two\n');

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'line two', new_string: `line two\n${SECRET_LINE}` },
      ctx(),
    );

    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /shared TEAM MEMORY/, res.output);
    assert.match(res.output, /all repository collaborators/i, res.output);
    // The secret must NOT be persisted.
    assert.ok(!fs.readFileSync(file, 'utf-8').includes('my-secret-password-123'), 'secret must not be written');
  });

  it('keeps the generic repository-scoped warning for a non-team-memory file', async () => {
    const file = path.join(tmpDir, 'app.ts');
    fs.writeFileSync(file, 'line one\nline two\n');

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'line two', new_string: `line two\n${SECRET_LINE}` },
      ctx(),
    );

    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /exposed to everyone with access to this repository/, res.output);
    assert.doesNotMatch(res.output, /TEAM MEMORY/, res.output);
  });

  it('applies the team-memory scope on the full-overwrite (write) path too', async () => {
    const teamDir = path.join(tmpDir, '.mem', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    const file = path.join(teamDir, 'notes.md');

    // Brand-new file (no old_string) → write/create path.
    const res = await fileEditTool.execute(
      { file_path: file, new_string: `hello\n${SECRET_LINE}\n` },
      ctx(),
    );

    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /shared TEAM MEMORY/, res.output);
  });
});

describe('fileEditTool – accurate 1-based edit location in patch-info', () => {
  it('reports the first changed line (:1) for an edit on line 1', async () => {
    const file = path.join(tmpDir, 's.txt');
    fs.writeFileSync(file, 'hello world\nsecond line\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'hello world', new_string: 'goodbye world' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /\[edit: s\.txt:1, \+1 -1\]/, res.output);
  });

  it('reports the true 1-based line for an edit deep in the file', async () => {
    const file = path.join(tmpDir, 'deep.txt');
    fs.writeFileSync(file, 'a\nb\nc\nTARGET\ne\nf\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'TARGET', new_string: 'CHANGED' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    // TARGET is on line 4 (1-based) — the surfaced location points at the change,
    // not the hunk's leading-context start.
    assert.match(res.output, /\[edit: deep\.txt:4, \+1 -1\]/, res.output);
    // A structured patch is still attached for per-hunk rendering.
    assert.ok(res.structuredPatch && res.structuredPatch.length > 0, JSON.stringify(res.structuredPatch));
  });
});
