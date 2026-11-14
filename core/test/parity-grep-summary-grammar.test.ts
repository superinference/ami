import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { grepTool } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal };
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-gram-')); });
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('grep – result summary singular/plural grammar', () => {
  it('uses "Found 1 file" for a single matching file (files_with_matches)', async () => {
    fs.writeFileSync(path.join(tmpDir, 'only.ts'), 'const NEEDLE = 1;\n');
    const result = await grepTool.execute({ pattern: 'NEEDLE' }, ctx());
    assert.ok(/Found 1 file\b/.test(result.output), result.output);
    assert.ok(!/Found 1 files/.test(result.output), result.output);
  });

  it('uses "Found N files" for multiple matching files', async () => {
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), 'const NEEDLE = 1;\n');
    fs.writeFileSync(path.join(tmpDir, 'b.ts'), 'const NEEDLE = 2;\n');
    const result = await grepTool.execute({ pattern: 'NEEDLE' }, ctx());
    assert.ok(/Found 2 files\b/.test(result.output), result.output);
  });

  it('uses "1 match in 1 file" grammar in content mode', async () => {
    fs.writeFileSync(path.join(tmpDir, 'solo.ts'), 'const NEEDLE = 1;\nother\n');
    const result = await grepTool.execute({ pattern: 'NEEDLE', output_mode: 'content' }, ctx());
    assert.ok(/1 match in 1 file\b/.test(result.output), result.output);
    assert.ok(!/1 matches/.test(result.output), result.output);
  });
});
