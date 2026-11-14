import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { grepTool } from '../src/tools/grep';
import { globTool } from '../src/tools/glob';
import { listDirTool } from '../src/tools/list-dir';
import { fuzzyFindAndReplace } from '../src/tools/fuzzy-match';
import { stripUrlCredentials, scoutCloneTimeoutMs } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, over: Partial<ToolContext> = {}): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, ...over } as ToolContext;
}

let tmp: string;
beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'parity-tf2-')); });
afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

// --- UNC guards (grep + glob) ------------------------------------------------
describe('UNC-path guards', () => {
  it('grep rejects a UNC path before touching the fs', async () => {
    const r = await grepTool.execute({ pattern: 'x', path: '\\\\server\\share' }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /UNC paths are not allowed/);
  });
  it('glob rejects a UNC path', async () => {
    const r = await globTool.execute({ pattern: '*', path: '//server/share' }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /UNC paths are not allowed/);
  });
});

// --- Self-correcting directory-not-found errors ------------------------------
describe('directory-not-found self-correction', () => {
  it('list_dir names the cwd and suggests a sibling', async () => {
    fs.mkdirSync(path.join(tmp, 'source'));
    const r = await listDirTool.execute({ path: path.join(tmp, 'sorce') }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /current working directory/);
    assert.match(r.output, /Did you mean .*source/);
  });
  it('glob reports a missing base directory with the cwd', async () => {
    const r = await globTool.execute({ pattern: '*.ts', path: path.join(tmp, 'nope') }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /Directory does not exist/);
  });
  it('grep reports a missing search path with the cwd', async () => {
    const r = await grepTool.execute({ pattern: 'x', path: path.join(tmp, 'nope') }, ctx(tmp));
    assert.ok(r.isError);
    assert.match(r.output, /Path does not exist/);
  });
});

// --- grep: glob param alias, fixed_strings, count summary --------------------
describe('grep param + mode behavior', () => {
  beforeEach(() => {
    fs.writeFileSync(path.join(tmp, 'a.ts'), 'foo\nfoo.bar\nfoo\n');
    fs.writeFileSync(path.join(tmp, 'b.js'), 'foo\n');
  });

  it('accepts the `glob` alias for the include filter', async () => {
    const r = await grepTool.execute({ pattern: 'foo', path: tmp, glob: '*.ts', output_mode: 'files_with_matches' }, ctx(tmp));
    assert.ok(!r.isError);
    assert.match(r.output, /a\.ts/);
    assert.ok(!r.output.includes('b.js'));
  });

  it('fixed_strings matches a regex metachar literally', async () => {
    const r = await grepTool.execute({ pattern: 'foo.bar', path: tmp, fixed_strings: true, output_mode: 'content' }, ctx(tmp));
    assert.ok(!r.isError);
    assert.match(r.output, /foo\.bar/);
  });

  it('count mode emits a natural-language total summary', async () => {
    const r = await grepTool.execute({ pattern: 'foo', path: tmp, output_mode: 'count' }, ctx(tmp));
    assert.ok(!r.isError);
    assert.match(r.output, /Found \d+ total occurrences across \d+ files\./);
  });
});

// --- scout helpers -----------------------------------------------------------
describe('scout helpers', () => {
  it('stripUrlCredentials removes userinfo', () => {
    assert.equal(stripUrlCredentials('https://user:tok@github.com/o/r'), 'https://github.com/o/r');
    assert.equal(stripUrlCredentials('https://github.com/o/r'), 'https://github.com/o/r');
  });
  it('scoutCloneTimeoutMs honours input then env then default', () => {
    assert.equal(scoutCloneTimeoutMs({ timeout_ms: 5000 }), 5000);
    assert.equal(scoutCloneTimeoutMs({}, { AMI_SCOUT_CLONE_TIMEOUT_MS: '7000' } as NodeJS.ProcessEnv), 7000);
    assert.equal(scoutCloneTimeoutMs({}, {} as NodeJS.ProcessEnv), 60000);
  });
});

// --- fuzzy: delete trailing newline ------------------------------------------
describe('fuzzy delete-trailing-newline', () => {
  it('removes the whole line (no leftover blank line) on deletion', () => {
    const r = fuzzyFindAndReplace('a\nDELETE_ME\nb\n', 'DELETE_ME', '');
    assert.equal(r.newContent, 'a\nb\n');
  });
});
