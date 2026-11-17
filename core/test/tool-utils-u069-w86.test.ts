import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';

import { resolveFilePath, safeReadFile } from '../src/tools/tool-utils';
import { fileReadTool } from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U069 (w86) — close the symlink→special-file hole in the FIFO/socket/device
// guard. The existing lstat pre-guard (w70) inspects only the LINK entry, so a
// symlink (or symlink chain) pointing at an IN-WORKSPACE FIFO/socket/char/block
// device passes resolveFilePath with no error — and a later readFileSync follows
// the link and blocks forever on the FIFO (a DoS a crafted link could trigger
// just by being validated). resolveFilePath now re-checks the fully-resolved
// symlink target and refuses before any consumer reads.
//
// RED-GREEN: with the fix reverted, resolveFilePath(link→FIFO) returns NO error
// (the first test's assert.ok(error) fails) and safeReadFile(link→FIFO) blocks
// until the per-test timeout fires (the second test fails). Both go GREEN with
// the target special-file guard in place.
// ---------------------------------------------------------------------------

let hasMkfifo = false;
try {
  const probe = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w86-probe-'));
  try {
    execFileSync('mkfifo', [path.join(probe, 'p')]);
    hasMkfifo = true;
  } finally {
    fs.rmSync(probe, { recursive: true, force: true });
  }
} catch {
  hasMkfifo = false;
}

describe('U069 [w86] symlink→special-file guard in resolveFilePath', { skip: !hasMkfifo ? 'mkfifo unavailable on this platform' : false }, () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w86-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('refuses a symlink that targets an in-workspace FIFO', () => {
    const fifo = path.join(dir, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const link = path.join(dir, 'link');
    fs.symlinkSync(fifo, link);
    const { error } = resolveFilePath(link, dir);
    assert.ok(error, 'a symlink→FIFO must be refused (would otherwise block a read)');
    assert.match(error!.output, /symlink targeting a special file \(FIFO\/socket\/device\)/);
  });

  it('refuses a symlink CHAIN that ultimately targets a FIFO', () => {
    const fifo = path.join(dir, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const mid = path.join(dir, 'mid');
    fs.symlinkSync(fifo, mid);
    const link = path.join(dir, 'link');
    fs.symlinkSync(mid, link);
    const { error } = resolveFilePath(link, dir);
    assert.ok(error, 'a symlink chain ending at a FIFO must be refused');
    assert.match(error!.output, /special file \(FIFO\/socket\/device\)/);
  });

  it('safeReadFile on a symlink→FIFO errors quickly (does not hang)', { timeout: 4000 }, () => {
    const fifo = path.join(dir, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const link = path.join(dir, 'link');
    fs.symlinkSync(fifo, link);
    const start = Date.now();
    const res = safeReadFile(link, dir);
    const elapsed = Date.now() - start;
    assert.ok(res.error, 'reading a symlink→FIFO must error, not hang');
    assert.ok(elapsed < 2000, `safeReadFile on symlink→FIFO took ${elapsed}ms — should be immediate`);
  });

  it('still follows an ordinary symlink to a regular in-workspace file', () => {
    const real = path.join(dir, 'real.txt');
    fs.writeFileSync(real, 'hello world');
    const link = path.join(dir, 'link.txt');
    fs.symlinkSync(real, link);
    const { resolved, error } = resolveFilePath(link, dir);
    assert.equal(error, undefined, error?.output);
    assert.equal(resolved, link);
    const res = safeReadFile(link, dir);
    assert.equal(res.error, undefined);
    assert.equal(res.content, 'hello world');
  });
});

describe('U069 [w86] e2e — file_read refuses a symlink→FIFO (does not hang)', { skip: !hasMkfifo ? 'mkfifo unavailable on this platform' : false }, () => {
  let ws: string;
  beforeEach(() => { ws = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w86-e2e-')); });
  afterEach(() => { fs.rmSync(ws, { recursive: true, force: true }); });

  it('returns an isError result immediately for a symlink→FIFO target', { timeout: 4000 }, async () => {
    const fifo = path.join(ws, 'pipe');
    execFileSync('mkfifo', [fifo]);
    const link = path.join(ws, 'link');
    fs.symlinkSync(fifo, link);
    const ctx: ToolContext = { cwd: ws, abortSignal: new AbortController().signal };
    const start = Date.now();
    const res = await fileReadTool.execute({ file_path: link }, ctx);
    const elapsed = Date.now() - start;
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /special file \(FIFO\/socket\/device\)/);
    assert.ok(elapsed < 3000, `file_read on a symlink→FIFO took ${elapsed}ms — must not hang`);
  });
});
