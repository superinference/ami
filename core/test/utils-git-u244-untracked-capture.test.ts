import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import {
  captureUntrackedFiles,
  UNTRACKED_MAX_FILE_SIZE_BYTES,
  UNTRACKED_MAX_TOTAL_SIZE_BYTES,
  UNTRACKED_MAX_FILE_COUNT,
  UNTRACKED_SNIFF_BUFFER_SIZE,
} from '../src/utils/git';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function mkRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-untracked-')));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}

function byPath(files: { path: string; content: string }[]): Map<string, string> {
  return new Map(files.map((f) => [f.path, f.content]));
}

test('captures the reference-parity default constants', () => {
  assert.equal(UNTRACKED_MAX_FILE_SIZE_BYTES, 500 * 1024 * 1024);
  assert.equal(UNTRACKED_MAX_TOTAL_SIZE_BYTES, 5 * 1024 * 1024 * 1024);
  assert.equal(UNTRACKED_MAX_FILE_COUNT, 20000);
  assert.equal(UNTRACKED_SNIFF_BUFFER_SIZE, 64 * 1024);
});

test('returns [] when cwd is not a git repo', async () => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nogit-')));
  try {
    assert.deepEqual(await captureUntrackedFiles(dir), []);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('returns [] when there are no untracked files', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, 'tracked.txt'), 'x');
    git(dir, 'add', 'tracked.txt');
    git(dir, 'commit', '-q', '-m', 'init');
    assert.deepEqual(await captureUntrackedFiles(dir), []);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('captures untracked text file content', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, 'a.txt'), 'hello\nworld\n');
    fs.writeFileSync(path.join(dir, 'b.md'), '# title');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.get('a.txt'), 'hello\nworld\n');
    assert.equal(got.get('b.md'), '# title');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('empty untracked file uses the fast path (empty content, no crash)', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, 'empty.txt'), '');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('empty.txt'), true);
    assert.equal(got.get('empty.txt'), '');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('skips binary files by extension with zero I/O', async () => {
  const dir = mkRepo();
  try {
    // A .zip with text bytes still gets skipped — proves the extension pre-filter
    // runs before any read (zip is in the known-binary set).
    fs.writeFileSync(path.join(dir, 'arch.zip'), 'not really binary');
    fs.writeFileSync(path.join(dir, 'ok.txt'), 'keep');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('arch.zip'), false);
    assert.equal(got.get('ok.txt'), 'keep');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('skips files whose CONTENT is binary (non-binary extension)', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, 'data.txt'), Buffer.from([0x68, 0x00, 0x69])); // has NUL
    fs.writeFileSync(path.join(dir, 'clean.txt'), 'clean');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('data.txt'), false);
    assert.equal(got.get('clean.txt'), 'clean');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('respects .gitignore via --exclude-standard', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, '.gitignore'), 'secret.txt\n');
    fs.writeFileSync(path.join(dir, 'secret.txt'), 'nope');
    fs.writeFileSync(path.join(dir, 'visible.txt'), 'yes');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('secret.txt'), false);
    assert.equal(got.get('visible.txt'), 'yes');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('large text file beyond the sniff buffer falls back to full readFile', async () => {
  const dir = mkRepo();
  try {
    // 100 bytes of text, sniff buffer forced to 16 bytes => file > sniffSize path.
    const content = 'A'.repeat(100);
    fs.writeFileSync(path.join(dir, 'big.txt'), content);
    const got = byPath(await captureUntrackedFiles(dir, { sniffBufferSize: 16 }));
    assert.equal(got.get('big.txt'), content);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('per-file size gate skips an oversized file', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, 'big.txt'), 'A'.repeat(50));
    fs.writeFileSync(path.join(dir, 'small.txt'), 'ok');
    const got = byPath(await captureUntrackedFiles(dir, { maxFileSizeBytes: 10 }));
    assert.equal(got.has('big.txt'), false);
    assert.equal(got.get('small.txt'), 'ok');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('aggregate size gate stops once the ceiling would be crossed', async () => {
  const dir = mkRepo();
  try {
    // Two 30-byte files; a 50-byte total budget admits the first, stops at the second.
    fs.writeFileSync(path.join(dir, 'a-first.txt'), 'A'.repeat(30));
    fs.writeFileSync(path.join(dir, 'b-second.txt'), 'B'.repeat(30));
    const files = await captureUntrackedFiles(dir, { maxTotalSizeBytes: 50 });
    // Deterministic ls-files order is lexical; first file fits, second breaks the loop.
    assert.equal(files.length, 1);
    assert.equal(files[0].path, 'a-first.txt');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('file-count gate caps the number of captured files', async () => {
  const dir = mkRepo();
  try {
    fs.writeFileSync(path.join(dir, '1.txt'), 'one');
    fs.writeFileSync(path.join(dir, '2.txt'), 'two');
    fs.writeFileSync(path.join(dir, '3.txt'), 'three');
    const files = await captureUntrackedFiles(dir, { maxFileCount: 2 });
    assert.equal(files.length, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('captures files in nested untracked directories', async () => {
  const dir = mkRepo();
  try {
    fs.mkdirSync(path.join(dir, 'realdir'));
    fs.writeFileSync(path.join(dir, 'realdir', 'inner.txt'), 'deep');
    fs.writeFileSync(path.join(dir, 'top.txt'), 'top');
    const got = byPath(await captureUntrackedFiles(dir));
    // Nested untracked file is listed by git as realdir/inner.txt and captured.
    assert.equal(got.get('realdir/inner.txt'), 'deep');
    assert.equal(got.get('top.txt'), 'top');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('skips an untracked entry that stat resolves to a non-regular file', async () => {
  const dir = mkRepo();
  try {
    fs.mkdirSync(path.join(dir, 'targetdir'));
    // A symlink to a directory is listed by `ls-files --others`; stat() follows it
    // and reports a directory, so the !isFile() guard skips it without reading.
    fs.symlinkSync(path.join(dir, 'targetdir'), path.join(dir, 'dirlink'));
    fs.writeFileSync(path.join(dir, 'keep.txt'), 'keep');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('dirlink'), false);
    assert.equal(got.get('keep.txt'), 'keep');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('skips an untracked entry that cannot be read (deleted after listing is tolerated)', async () => {
  const dir = mkRepo();
  try {
    // A broken symlink: ls-files lists it, stat() throws ENOENT => caught + skipped.
    fs.symlinkSync(path.join(dir, 'does-not-exist'), path.join(dir, 'broken'));
    fs.writeFileSync(path.join(dir, 'present.txt'), 'present');
    const got = byPath(await captureUntrackedFiles(dir));
    assert.equal(got.has('broken'), false);
    assert.equal(got.get('present.txt'), 'present');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
