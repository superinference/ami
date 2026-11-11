import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { globTool, globHooks, GLOB_RIPGREP_ENV, isRipgrepGlobEnabled } from '../src/tools/glob';
import type { ToolContext } from '../src/types';

describe('globTool', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'file1.ts'), 'export const a = 1;');
    fs.writeFileSync(path.join(tmpDir, 'file2.ts'), 'export const b = 2;');
    fs.writeFileSync(path.join(tmpDir, 'readme.md'), '# Readme');
    fs.mkdirSync(path.join(tmpDir, 'sub'));
    fs.writeFileSync(path.join(tmpDir, 'sub', 'nested.ts'), 'export const c = 3;');
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('has correct name and schema', () => {
    assert.equal(globTool.name, 'glob');
    assert.ok(globTool.isReadOnly);
    assert.ok(globTool.isConcurrencySafe);
  });

  it('finds .ts files with glob pattern', async () => {
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(result.output.includes('file1.ts'));
    assert.ok(result.output.includes('file2.ts'));
    assert.ok(result.output.includes('nested.ts'));
  });

  it('returns no matches for unmatched pattern', async () => {
    const result = await globTool.execute!({ pattern: '**/*.py' }, ctx);
    assert.ok(result.output.includes('No files found'));
  });

  it('finds specific file', async () => {
    const result = await globTool.execute!({ pattern: 'readme.md' }, ctx);
    assert.ok(result.output.includes('readme.md'));
    assert.ok(result.output.includes('1 file'));
  });

  it('respects path parameter', async () => {
    const result = await globTool.execute!({ pattern: '*.ts', path: path.join(tmpDir, 'sub') }, ctx);
    assert.ok(result.output.includes('nested.ts'));
    assert.ok(!result.output.includes('file1.ts'));
  });

  it('returns error for invalid path', async () => {
    const result = await globTool.execute!({ pattern: '*.ts', path: '/nonexistent/dir/xyz' }, ctx);
    assert.ok(result.output.includes('No files found') || result.isError);
  });
});

// ---------------------------------------------------------------------------
// G5: opt-in ripgrep route (AMI_GLOB_RG) with tinyglobby fallback
// ---------------------------------------------------------------------------
describe('globTool — G5 ripgrep route', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origRg = globHooks.rgFiles;
  const origGlob = globHooks.glob;
  const origEnv = process.env[GLOB_RIPGREP_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-rg-test-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '1');
    fs.writeFileSync(path.join(tmpDir, 'b.ts'), '2');
  });
  after(() => {
    globHooks.rgFiles = origRg;
    globHooks.glob = origGlob;
    if (origEnv === undefined) delete process.env[GLOB_RIPGREP_ENV];
    else process.env[GLOB_RIPGREP_ENV] = origEnv;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('isRipgrepGlobEnabled honors the env flag (truthy/falsey/unset)', () => {
    assert.equal(isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: '1' }), true);
    assert.equal(isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: 'true' }), true);
    assert.equal(isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: '0' }), false);
    assert.equal(isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: 'off' }), false);
    assert.equal(isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: '' }), false);
    assert.equal(isRipgrepGlobEnabled({}), false);
  });

  it('uses the ripgrep route when enabled (tinyglobby NOT called)', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    let globCalled = false;
    globHooks.glob = (async () => { globCalled = true; return []; }) as typeof globHooks.glob;
    globHooks.rgFiles = async () => ['a.ts', 'b.ts'];
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(result.output.includes('a.ts') && result.output.includes('b.ts'));
    assert.equal(globCalled, false, 'tinyglobby must not run when rg handled it');
  });

  it('falls back to tinyglobby when rg is unavailable (returns null)', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    let globCalled = false;
    globHooks.rgFiles = async () => null; // rg missing/errored
    globHooks.glob = (async () => { globCalled = true; return ['a.ts']; }) as typeof globHooks.glob;
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.equal(globCalled, true, 'must fall back to tinyglobby when rg returns null');
    assert.ok(result.output.includes('a.ts'));
  });

  it('treats rg empty result ([]) as "no files found" (no fallback)', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    let globCalled = false;
    globHooks.rgFiles = async () => [];
    globHooks.glob = (async () => { globCalled = true; return ['a.ts']; }) as typeof globHooks.glob;
    const result = await globTool.execute!({ pattern: '**/*.xyz' }, ctx);
    assert.ok(result.output.includes('No files found'));
    assert.equal(globCalled, false, 'an empty rg result is authoritative — no tinyglobby fallback');
  });

  it('does NOT invoke rg when the flag is off (default tinyglobby path)', async () => {
    delete process.env[GLOB_RIPGREP_ENV];
    let rgCalled = false;
    globHooks.rgFiles = async () => { rgCalled = true; return ['a.ts']; };
    globHooks.glob = origGlob;
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.equal(rgCalled, false, 'rg must not run unless AMI_GLOB_RG is set');
    assert.ok(result.output.includes('a.ts'));
  });

  // Exercise the REAL ripgrepFiles walker (not the rgFiles mock) via an injected
  // execFileImpl, so its success / no-match / binary-missing branches are covered
  // deterministically without a ripgrep binary.
  describe('real ripgrepFiles via injected execFileImpl', () => {
    const origExec = globHooks.execFileImpl;
    beforeEach(() => { process.env[GLOB_RIPGREP_ENV] = '1'; globHooks.rgFiles = origRg; });
    afterEach(() => { globHooks.execFileImpl = origExec; });

    it('parses rg stdout into relative paths (success)', async () => {
      globHooks.execFileImpl = ((_f: string, args: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
        // the pattern is passed as `-g <pattern>`
        assert.ok(args.includes('--files') && args.includes('-g'));
        cb(null, 'a.ts\n  b.ts  \n\n');
      }) as typeof globHooks.execFileImpl;
      const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
      assert.ok(result.output.includes('a.ts') && result.output.includes('b.ts'));
    });

    it('treats rg exit code 1 as an empty match set (no fallback)', async () => {
      let globCalled = false;
      globHooks.glob = (async () => { globCalled = true; return ['a.ts']; }) as typeof globHooks.glob;
      globHooks.execFileImpl = ((_f: string, _a: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
        cb({ code: 1 }, '');
      }) as typeof globHooks.execFileImpl;
      const result = await globTool.execute!({ pattern: '**/*.none' }, ctx);
      assert.ok(result.output.includes('No files found'));
      assert.equal(globCalled, false);
    });

    it('falls back to tinyglobby when rg is missing (ENOENT)', async () => {
      let globCalled = false;
      globHooks.glob = (async () => { globCalled = true; return ['a.ts']; }) as typeof globHooks.glob;
      globHooks.execFileImpl = ((_f: string, _a: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
        cb({ code: 'ENOENT' }, '');
      }) as typeof globHooks.execFileImpl;
      const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
      assert.equal(globCalled, true, 'ENOENT must fall back to tinyglobby');
      assert.ok(result.output.includes('a.ts'));
    });
  });
});
