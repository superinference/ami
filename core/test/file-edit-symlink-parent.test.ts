import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { verifyWriteTarget } from '../src/tools/file-edit';

let tmpDir: string;
let outsideDir: string;

function setup() {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-symlink-parent-'));
  outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside-'));
}

function cleanup() {
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  try { fs.rmSync(outsideDir, { recursive: true, force: true }); } catch {}
}

describe('verifyWriteTarget — parent directory symlink escape protection', () => {
  afterEach(cleanup);

  it('blocks write when parent directory is a symlink to outside workspace', () => {
    setup();
    const linkDir = path.join(tmpDir, 'escape');
    fs.symlinkSync(outsideDir, linkDir);
    const targetFile = path.join(linkDir, 'secret.txt');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.ok(result, 'should return an error');
    assert.ok(result!.includes('parent directory'));
    assert.ok(result!.includes('outside the workspace'));
  });

  it('allows write when parent directory is a symlink within workspace', () => {
    setup();
    const subdir = path.join(tmpDir, 'real');
    fs.mkdirSync(subdir);
    const linkDir = path.join(tmpDir, 'link');
    fs.symlinkSync(subdir, linkDir);
    const targetFile = path.join(linkDir, 'file.txt');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.equal(result, undefined, 'should allow write within workspace');
  });

  it('allows write when no parent directories are symlinks', () => {
    setup();
    const subdir = path.join(tmpDir, 'sub', 'dir');
    fs.mkdirSync(subdir, { recursive: true });
    const targetFile = path.join(subdir, 'file.txt');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.equal(result, undefined);
  });

  it('blocks write through nested symlink chain escaping workspace', () => {
    setup();
    const midDir = path.join(tmpDir, 'level1');
    fs.mkdirSync(midDir);
    const nestedLink = path.join(midDir, 'escape');
    fs.symlinkSync(outsideDir, nestedLink);
    const targetFile = path.join(nestedLink, 'nested.txt');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.ok(result, 'should block write through nested symlink escape');
  });

  it('skips non-existent intermediate directories', () => {
    setup();
    const targetFile = path.join(tmpDir, 'nonexistent', 'sub', 'file.txt');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.equal(result, undefined);
  });

  it('blocks the /etc attack vector', () => {
    setup();
    const linkDir = path.join(tmpDir, 'symdir');
    fs.symlinkSync('/etc', linkDir);
    const targetFile = path.join(linkDir, 'passwd');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.ok(result, 'should block the parent-dir symlink escape attack');
    assert.ok(result!.includes('/etc'));
  });

  it('still blocks direct file symlinks escaping workspace', () => {
    setup();
    const outsideFile = path.join(outsideDir, 'target.txt');
    fs.writeFileSync(outsideFile, 'secret');
    const linkFile = path.join(tmpDir, 'link.txt');
    fs.symlinkSync(outsideFile, linkFile);
    const result = verifyWriteTarget(linkFile, tmpDir);
    assert.ok(result, 'should block direct symlink escape');
    assert.ok(result!.includes('symlink'));
  });

  it('allows write for regular files at workspace root', () => {
    setup();
    const targetFile = path.join(tmpDir, 'root-file.txt');
    fs.writeFileSync(targetFile, 'content');
    const result = verifyWriteTarget(targetFile, tmpDir);
    assert.equal(result, undefined);
  });
});
