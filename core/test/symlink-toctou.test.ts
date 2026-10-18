import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { resolveFilePath, isSensitivePath, safeReadFile, safeLstat } from '../src/tools/tool-utils';

function makeTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-symlink-'));
}

describe('isSensitivePath', () => {
  it('blocks /etc/shadow', () => assert.ok(isSensitivePath('/etc/shadow')));
  it('blocks /etc/passwd', () => assert.ok(isSensitivePath('/etc/passwd')));
  it('blocks /etc/sudoers', () => assert.ok(isSensitivePath('/etc/sudoers')));
  it('blocks /proc/self/environ', () => assert.ok(isSensitivePath('/proc/self/environ')));
  it('blocks /sys/class/net', () => assert.ok(isSensitivePath('/sys/class/net')));
  it('blocks ~/.ssh/id_rsa', () => {
    const home = process.env.HOME || process.env.USERPROFILE || '/home/test';
    assert.ok(isSensitivePath(path.join(home, '.ssh', 'id_rsa')));
  });
  it('allows /etc/hostname', () => assert.ok(!isSensitivePath('/etc/hostname')));
  it('allows /tmp/safe', () => assert.ok(!isSensitivePath('/tmp/safe')));
  it('allows home dir itself', () => {
    const home = process.env.HOME || process.env.USERPROFILE || '/home/test';
    assert.ok(!isSensitivePath(home));
  });
});

describe('resolveFilePath — symlink TOCTOU', () => {
  const dirs: string[] = [];
  after(() => { dirs.forEach(d => fs.rmSync(d, { recursive: true, force: true })); });

  it('allows normal files within cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'hello.txt'), 'hi');
    const { error } = resolveFilePath('hello.txt', cwd);
    assert.equal(error, undefined);
  });

  it('rejects paths outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const { error } = resolveFilePath('/etc/hostname', cwd);
    assert.ok(error);
    assert.ok(error.isError);
  });

  it('rejects sensitive system paths even if cwd is /', () => {
    const { error } = resolveFilePath('/etc/shadow', '/');
    assert.ok(error);
    assert.match(error!.output, /sensitive system path/);
  });

  it('rejects /proc/self/environ', () => {
    const { error } = resolveFilePath('/proc/self/environ', '/');
    assert.ok(error);
    assert.match(error!.output, /sensitive system path/);
  });

  it('rejects symlinks pointing outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const outside = makeTmp(); dirs.push(outside);
    fs.writeFileSync(path.join(outside, 'secret.txt'), 'secret');
    fs.symlinkSync(path.join(outside, 'secret.txt'), path.join(cwd, 'link.txt'));
    const { error } = resolveFilePath('link.txt', cwd);
    assert.ok(error);
    assert.match(error!.output, /symlink/);
  });

  it('allows symlinks pointing within cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'real.txt'), 'content');
    fs.symlinkSync(path.join(cwd, 'real.txt'), path.join(cwd, 'link.txt'));
    const { error } = resolveFilePath('link.txt', cwd);
    assert.equal(error, undefined);
  });

  it('rejects .. traversal outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const sub = path.join(cwd, 'sub');
    fs.mkdirSync(sub);
    const { error } = resolveFilePath('../../etc/hostname', sub);
    assert.ok(error);
  });

  it('allows non-existent files within cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const { error } = resolveFilePath('does-not-exist.txt', cwd);
    assert.equal(error, undefined);
  });

  it('rejects non-existent file whose parent dir symlinks outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const outside = makeTmp(); dirs.push(outside);
    fs.symlinkSync(outside, path.join(cwd, 'linked-dir'));
    const { error } = resolveFilePath('linked-dir/newfile.txt', cwd);
    assert.ok(error);
    assert.match(error!.output, /symlink/);
  });
});

// ---------------------------------------------------------------------------
// safeReadFile — lstat-based TOCTOU protection
// ---------------------------------------------------------------------------

describe('safeReadFile — TOCTOU protection', () => {
  const dirs: string[] = [];
  after(() => { dirs.forEach(d => fs.rmSync(d, { recursive: true, force: true })); });

  it('reads a normal file inside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'hello.txt'), 'hello world');
    const result = safeReadFile('hello.txt', cwd);
    assert.equal(result.error, undefined);
    assert.equal(result.content, 'hello world');
    assert.equal(result.encoding, 'utf-8');
  });

  it('rejects symlink pointing outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const outside = makeTmp(); dirs.push(outside);
    fs.writeFileSync(path.join(outside, 'secret.txt'), 'secret');
    fs.symlinkSync(path.join(outside, 'secret.txt'), path.join(cwd, 'link.txt'));
    const result = safeReadFile('link.txt', cwd);
    assert.ok(result.error);
    assert.ok(result.error!.isError);
    assert.match(result.error!.output, /symlink/);
  });

  it('allows symlink pointing inside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'real.txt'), 'real content');
    fs.symlinkSync(path.join(cwd, 'real.txt'), path.join(cwd, 'link.txt'));
    const result = safeReadFile('link.txt', cwd);
    assert.equal(result.error, undefined);
    assert.equal(result.content, 'real content');
  });

  it('returns error for non-existent file', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const result = safeReadFile('missing.txt', cwd);
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('rejects path outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const result = safeReadFile('/etc/hostname', cwd);
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('reads BOM-encoded files correctly', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const bom = Buffer.from([0xEF, 0xBB, 0xBF]);
    const text = Buffer.from('utf8 with bom', 'utf-8');
    fs.writeFileSync(path.join(cwd, 'bom.txt'), Buffer.concat([bom, text]));
    const result = safeReadFile('bom.txt', cwd);
    assert.equal(result.error, undefined);
    assert.equal(result.content, 'utf8 with bom');
    assert.equal(result.encoding, 'utf-8-bom');
  });
});

// ---------------------------------------------------------------------------
// safeLstat — non-following stat
// ---------------------------------------------------------------------------

describe('safeLstat — non-following stat', () => {
  const dirs: string[] = [];
  after(() => { dirs.forEach(d => fs.rmSync(d, { recursive: true, force: true })); });

  it('returns stats for a regular file', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'file.txt'), 'data');
    const result = safeLstat('file.txt', cwd);
    assert.equal(result.error, undefined);
    assert.ok(result.stat!.isFile());
    assert.ok(!result.stat!.isSymbolicLink());
  });

  it('identifies symlinks without following', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.writeFileSync(path.join(cwd, 'target.txt'), 'data');
    fs.symlinkSync(path.join(cwd, 'target.txt'), path.join(cwd, 'link.txt'));
    const result = safeLstat('link.txt', cwd);
    assert.equal(result.error, undefined);
    assert.ok(result.stat!.isSymbolicLink());
  });

  it('identifies directories', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    fs.mkdirSync(path.join(cwd, 'subdir'));
    const result = safeLstat('subdir', cwd);
    assert.equal(result.error, undefined);
    assert.ok(result.stat!.isDirectory());
  });

  it('rejects paths outside cwd', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const result = safeLstat('/etc/hostname', cwd);
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });

  it('returns error for non-existent path', () => {
    const cwd = makeTmp(); dirs.push(cwd);
    const result = safeLstat('ghost.txt', cwd);
    assert.ok(result.error);
    assert.ok(result.error!.isError);
  });
});
