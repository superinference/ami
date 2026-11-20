import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { globTool, globHooks, GLOB_RIPGREP_ENV } from '../src/tools/glob';
import { ORPHANED_PLUGIN_HOLD_DIRNAME } from '../src/tools/tool-utils';
import type { ToolContext } from '../src/types';

/**
 * U079c — two parity behaviors on the glob tool:
 *  - orphaned plugin hold-dir exclusion (`.orphaned/`): superseded plugin
 *    versions relocated by common's plugin-orphan-cache must never surface in
 *    glob results (parity: the reference's getGlobExclusionsForPluginCache).
 *  - file-READ deny patterns from the permission context (`readIgnorePatterns`)
 *    must be folded into the ignore set on BOTH the tinyglobby and ripgrep routes
 *    (parity: the reference's getFileReadIgnorePatterns → glob exclusions).
 */
describe('U079c glob — orphaned plugin hold dir excluded (tinyglobby route)', () => {
  let tmpDir: string;
  let ctx: ToolContext;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-orphan-'));
    ctx = { cwd: tmpDir } as ToolContext;
    fs.writeFileSync(path.join(tmpDir, 'live.ts'), '1');
    // A superseded plugin version relocated into the hidden hold dir.
    const stale = path.join(tmpDir, ORPHANED_PLUGIN_HOLD_DIRNAME, 'plugin-123-abc', 'src');
    fs.mkdirSync(stale, { recursive: true });
    fs.writeFileSync(path.join(stale, 'stale.ts'), '1');
  });
  after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  it('does not return files from the .orphaned/ hold dir by default', async () => {
    const result = await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    assert.ok(result.output.includes('live.ts'), 'live plugin file is listed');
    assert.ok(!result.output.includes('stale.ts'), 'orphaned version file must be hidden');
    assert.ok(!result.output.includes(ORPHANED_PLUGIN_HOLD_DIRNAME));
  });

  it('escape hatch: a pattern that explicitly targets the hold dir still lists it', async () => {
    const result = await globTool.execute!(
      { pattern: `**/${ORPHANED_PLUGIN_HOLD_DIRNAME}/**/*.ts` },
      ctx,
    );
    assert.ok(result.output.includes('stale.ts'), 'deliberate audit of orphaned versions must work');
  });
});

describe('U079c glob — permission read-deny patterns folded into ignore (tinyglobby route)', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-readdeny-'));
    fs.writeFileSync(path.join(tmpDir, 'app.ts'), '1');
    fs.writeFileSync(path.join(tmpDir, 'secret.env'), 'TOKEN=1');
    fs.mkdirSync(path.join(tmpDir, 'nested'));
    fs.writeFileSync(path.join(tmpDir, 'nested', 'secret.env'), 'TOKEN=2');
  });
  after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  it('omits read-denied files from results at any depth', async () => {
    const ctx = { cwd: tmpDir, readIgnorePatterns: ['secret.env'] } as ToolContext;
    const result = await globTool.execute!({ pattern: '**/*' }, ctx);
    assert.ok(result.output.includes('app.ts'), 'non-denied file is listed');
    assert.ok(!result.output.includes('secret.env'), 'a read-denied file must never be enumerated');
  });

  it('lists the file when no read-deny pattern matches it', async () => {
    const ctx = { cwd: tmpDir, readIgnorePatterns: ['other.key'] } as ToolContext;
    const result = await globTool.execute!({ pattern: '**/*' }, ctx);
    assert.ok(result.output.includes('secret.env'), 'unrelated deny pattern does not hide it');
  });
});

describe('U079c glob — ripgrep route negates orphaned + read-deny globs', () => {
  let tmpDir: string;
  const origExec = globHooks.execFileImpl;
  const origRgEnv = process.env[GLOB_RIPGREP_ENV];

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'glob-rg-orphan-'));
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '1');
  });
  afterEach(() => { globHooks.execFileImpl = origExec; });
  after(() => {
    if (origRgEnv === undefined) delete process.env[GLOB_RIPGREP_ENV];
    else process.env[GLOB_RIPGREP_ENV] = origRgEnv;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  async function captureRgArgs(ctx: ToolContext): Promise<string[]> {
    let seen: string[] = [];
    globHooks.execFileImpl = ((_f: string, args: string[], _o: unknown, cb: (e: unknown, out: string) => void) => {
      seen = args;
      cb(null, 'a.ts\n');
      return { kill() {} } as never;
    }) as typeof globHooks.execFileImpl;
    await globTool.execute!({ pattern: '**/*.ts' }, ctx);
    return seen;
  }

  it('threads !**/.orphaned/** and the read-deny glob into rg -g args', async () => {
    process.env[GLOB_RIPGREP_ENV] = '1';
    const ctx = { cwd: tmpDir, readIgnorePatterns: ['secret.env'] } as ToolContext;
    const args = await captureRgArgs(ctx);
    assert.ok(args.includes(`!**/${ORPHANED_PLUGIN_HOLD_DIRNAME}/**`), 'orphaned hold dir negated for rg');
    assert.ok(args.includes('!**/secret.env'), 'read-deny pattern negated for rg');
  });
});
