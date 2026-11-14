import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { globTool, globMaxResults, GLOB_MAX_RESULTS_ENV } from '../src/tools/glob';
import type { ToolContext } from '../src/types';

let tmpDir: string;
function ctx(): ToolContext { return { cwd: tmpDir, abortSignal: new AbortController().signal }; }

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-glob-page-'));
  for (let i = 0; i < 12; i++) fs.writeFileSync(path.join(tmpDir, `f${String(i).padStart(2, '0')}.ts`), '');
});
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('glob – result-limit env knob', () => {
  it('reads AMI_GLOB_MAX_RESULTS with a default fallback', () => {
    assert.equal(globMaxResults({} as NodeJS.ProcessEnv), 100);
    assert.equal(globMaxResults({ [GLOB_MAX_RESULTS_ENV]: '5' } as unknown as NodeJS.ProcessEnv), 5);
    assert.equal(globMaxResults({ [GLOB_MAX_RESULTS_ENV]: 'x' } as unknown as NodeJS.ProcessEnv), 100);
  });
});

describe('glob – offset/limit pagination', () => {
  it('returns only `limit` matches and reports the total', async () => {
    const result = await globTool.execute({ pattern: '*.ts', limit: 5 }, ctx());
    assert.ok(result.output.startsWith('[12 files (truncated)]'), result.output);
    const listed = result.output.split('\n').filter(l => l.endsWith('.ts'));
    assert.equal(listed.length, 5);
  });

  it('pages with offset and shows the range', async () => {
    const result = await globTool.execute({ pattern: '*.ts', offset: 5, limit: 4 }, ctx());
    assert.ok(/showing 6-9 of 12 matches/.test(result.output), result.output);
    const listed = result.output.split('\n').filter(l => l.endsWith('.ts'));
    assert.equal(listed.length, 4);
  });

  it('reports an empty range when offset is past the end', async () => {
    const result = await globTool.execute({ pattern: '*.ts', offset: 99 }, ctx());
    assert.ok(/No files in range \(offset 99, 12 total matches\)/.test(result.output), result.output);
  });

  it('default (no offset/limit) preserves single-page behavior', async () => {
    const result = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.ok(result.output.startsWith('[12 files]'), result.output);
    assert.ok(!result.output.includes('showing'), result.output);
  });
});
