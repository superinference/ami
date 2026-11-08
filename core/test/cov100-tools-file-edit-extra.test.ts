import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// Standalone coverage top-up for core/core/src/tools/file-edit.ts.
// Targets the sequential-edits ("edits: [...]") secret-detection branch
// (applySequentialEdits: scanAddedSecrets on the post-edit content), which the
// existing suite only exercises for the single-replace and replace_all paths.
// Core-isolation: imports ONLY from ../src.

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-edit-cov100-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('fileEditTool – sequential edits secret guard', () => {
  it('blocks a sequential edit that introduces a secret and leaves the file untouched', async () => {
    const file = path.join(tmpDir, 'config.go');
    const original = 'package cli\n\nvar x = 1\n';
    fs.writeFileSync(file, original);

    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          {
            old_string: 'var x = 1',
            new_string: 'var x = 1\npassword = "my-secret-password-123"',
          },
        ],
      },
      ctx(),
    );

    // The edit itself applied cleanly, but the secret scan on the resulting
    // content must reject the write.
    assert.equal(result.isError, true);
    assert.match(result.output, /Potential secrets detected in edited content/);
    // Nothing is persisted: the original content is preserved verbatim.
    assert.equal(fs.readFileSync(file, 'utf-8'), original);
    assert.equal(
      fs.readFileSync(file, 'utf-8').includes('my-secret-password-123'),
      false,
    );
  });

  it('applies a clean sequential edit when no secret is introduced (contrast case)', async () => {
    const file = path.join(tmpDir, 'config.go');
    fs.writeFileSync(file, 'package cli\n\nvar x = 1\n');

    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [{ old_string: 'var x = 1', new_string: 'var x = 2' }],
      },
      ctx(),
    );

    assert.equal(result.isError, false);
    assert.match(result.output, /Successfully edited/);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'package cli\n\nvar x = 2\n');
  });
});
