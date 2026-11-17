import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  globTool,
  globHooks,
  extractGlobBaseDirectory,
  GLOB_RIPGREP_ENV,
} from '../src/tools/glob';
import type { ToolContext } from '../src/types';

describe('U079b glob — strict input schema (reject unknown fields)', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-strict-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '1');
  });
  after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  it('declares additionalProperties:false on the input schema', () => {
    assert.equal(globTool.inputSchema.additionalProperties, false);
  });

  it('rejects an unknown/typo\'d top-level key with an actionable error', async () => {
    const r = await globTool.execute!({ pattern: '**/*.ts', recursive: true }, ctx);
    assert.ok(r.isError, 'unknown key must fail loud');
    assert.match(r.output, /Unknown parameter\(s\): recursive/);
    assert.match(r.output, /Allowed: pattern, path, offset, limit/);
  });

  it('accepts the declared keys (pattern, path, offset, limit)', async () => {
    const r = await globTool.execute!({ pattern: '**/*.ts', offset: 0, limit: 5 }, ctx);
    assert.ok(!r.isError);
    assert.ok(r.output.includes('a.ts'));
  });
});

describe('U079b glob — extractGlobBaseDirectory', () => {
  it('splits an absolute pattern at the first glob metachar', () => {
    assert.deepEqual(
      extractGlobBaseDirectory('/home/user/project/src/**/*.ts'),
      { baseDir: '/home/user/project/src', relativePattern: '**/*.ts' },
    );
  });

  it('treats a literal absolute path (no glob char) as dir + filename', () => {
    assert.deepEqual(
      extractGlobBaseDirectory('/home/user/readme.md'),
      { baseDir: '/home/user', relativePattern: 'readme.md' },
    );
  });

  it('special-cases the filesystem root (/*.txt → base /)', () => {
    assert.deepEqual(
      extractGlobBaseDirectory('/*.txt'),
      { baseDir: '/', relativePattern: '*.txt' },
    );
  });

  it('returns empty baseDir when the glob char precedes any separator', () => {
    assert.deepEqual(
      extractGlobBaseDirectory('*.ts'),
      { baseDir: '', relativePattern: '*.ts' },
    );
  });
});

describe('U079b glob — absolute pattern support (rooted at extracted base dir)', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'glob-abs-')));
    fs.mkdirSync(path.join(tmpDir, 'src'));
    fs.writeFileSync(path.join(tmpDir, 'src', 'only.ts'), '1');
    fs.writeFileSync(path.join(tmpDir, 'top.ts'), '1');
    // cwd is a SUBDIR so the absolute pattern points outside cwd but inside an allowed root.
    ctx = { cwd: path.join(tmpDir, 'src'), allowedReadRoots: [tmpDir] } as ToolContext;
  });
  after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  it('searches the pattern\'s base directory, not cwd', async () => {
    const r = await globTool.execute!({ pattern: `${tmpDir}/*.ts` }, ctx);
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('top.ts'), 'matches file under the absolute base dir');
    assert.ok(!r.output.includes('only.ts'), 'does not descend into src for a non-recursive pattern');
  });

  it('confines an absolute pattern to the allowed roots (escape rejected)', async () => {
    const r = await globTool.execute!({ pattern: '/etc/**/*.conf' }, ctx);
    assert.ok(r.isError, 'absolute pattern outside the workspace must be rejected');
    assert.match(r.output, /outside the workspace/);
  });

  it('reports directory-not-found for an absolute pattern whose base is missing', async () => {
    const missing = path.join(tmpDir, 'nope');
    const r = await globTool.execute!({ pattern: `${missing}/*.ts` }, ctx);
    assert.ok(r.isError);
    assert.match(r.output, /Directory does not exist/);
  });
});

describe('U079b glob — ripgrep route uses native --sortr=modified and skips stat sort', () => {
  let tmpDir: string;
  let ctx: ToolContext;
  const origExec = globHooks.execFileImpl;
  const origRg = globHooks.rgFiles;
  const origRgEnv = process.env[GLOB_RIPGREP_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-sort-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '1');
  });
  afterEach(() => {
    globHooks.execFileImpl = origExec;
    globHooks.rgFiles = origRg;
  });
  after(() => {
    if (origRgEnv === undefined) delete process.env[GLOB_RIPGREP_ENV]; else process.env[GLOB_RIPGREP_ENV] = origRgEnv;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('adds --sortr=modified to the rg args', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    let seen: string[] = [];
    globHooks.execFileImpl = ((_f: string, args: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
      seen = args;
      cb(null, 'a.ts\n');
      return { kill() {} } as never;
    }) as typeof globHooks.execFileImpl;
    await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(seen.includes('--sortr=modified'), 'rg orders newest-first natively');
  });

  it('trusts the rg-returned order (no per-file stat re-sort)', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    // rg returns a deliberate order that a mtime stat-sort would NOT reproduce
    // (these files do not exist on disk, so a stat-sort would collapse to alpha order).
    globHooks.rgFiles = async () => ['zebra.ts', 'alpha.ts', 'middle.ts'];
    const r = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    const lines = r.output.split('\n');
    const z = lines.indexOf('zebra.ts');
    const a = lines.indexOf('alpha.ts');
    const m = lines.indexOf('middle.ts');
    assert.ok(z < a && a < m, `rg order preserved, got: ${r.output}`);
  });
});
