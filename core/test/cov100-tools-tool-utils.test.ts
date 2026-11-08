import { describe, it, afterEach, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  isBlockedPath,
  isSensitivePath,
  safeReadFile,
  safeLstat,
  assertReadableTarget,
  rejectEscapingPattern,
  toolCallFingerprint,
  deduplicateToolCalls,
  buildToolDescriptionContext,
  resolveFilePath,
  isNonUtfEncoding,
  convertToLineEnding,
  _toolUtilsHooks,
} from '../src/tools/tool-utils';

// Temp-dir bookkeeping shared by the fs-touching suites.
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'tool-utils-cov-'));
  tmpDirs.push(d);
  return d;
}

afterEach(() => {
  mock.restoreAll();
});

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// isBlockedPath — device / special-file blocklist
// ---------------------------------------------------------------------------

describe('isBlockedPath', () => {
  it('blocks an exact device path', () => {
    assert.equal(isBlockedPath('/dev/null'), true);
    assert.equal(isBlockedPath('/dev/zero'), true);
  });

  it('blocks a path that starts with a directory-style blocked prefix', () => {
    assert.equal(isBlockedPath('/dev/fd/3'), true);
    assert.equal(isBlockedPath('/proc/self/fd/0'), true);
  });

  it('normalizes backslashes before matching', () => {
    assert.equal(isBlockedPath('\\dev\\null'), true);
  });

  it('allows an ordinary path', () => {
    assert.equal(isBlockedPath('/home/user/file.txt'), false);
    assert.equal(isBlockedPath('/dev/sda'), false);
  });
});

// ---------------------------------------------------------------------------
// isSensitivePath — system prefixes + HOME-relative dirs/files
// ---------------------------------------------------------------------------

describe('isSensitivePath', () => {
  const origHome = process.env.HOME;
  const origUserProfile = process.env.USERPROFILE;

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = origUserProfile;
  });

  it('flags a system prefix path exactly and by prefix', () => {
    assert.equal(isSensitivePath('/etc/shadow'), true);
    assert.equal(isSensitivePath('/proc/1/status'), true);
    assert.equal(isSensitivePath('/sys/kernel'), true);
  });

  it('flags a HOME sensitive directory (exact and nested)', () => {
    const home = mkTmp();
    process.env.HOME = home;
    assert.equal(isSensitivePath(path.join(home, '.ssh')), true);
    assert.equal(isSensitivePath(path.join(home, '.ssh', 'id_rsa')), true);
    assert.equal(isSensitivePath(path.join(home, '.config', 'gh')), true);
  });

  it('flags a HOME sensitive file by exact match only', () => {
    const home = mkTmp();
    process.env.HOME = home;
    assert.equal(isSensitivePath(path.join(home, '.npmrc')), true);
    assert.equal(isSensitivePath(path.join(home, '.kube', 'config')), true);
    // A different file in HOME is not sensitive.
    assert.equal(isSensitivePath(path.join(home, 'notes.txt')), false);
  });

  it('returns false when no HOME/USERPROFILE is set and path is not a system prefix', () => {
    delete process.env.HOME;
    delete process.env.USERPROFILE;
    assert.equal(isSensitivePath('/tmp/whatever/file.txt'), false);
  });
});

// ---------------------------------------------------------------------------
// assertReadableTarget — fail-loud guard for search tools
// ---------------------------------------------------------------------------

describe('assertReadableTarget', () => {
  it('returns null for a readable path', () => {
    const dir = mkTmp();
    const f = path.join(dir, 'a.txt');
    fs.writeFileSync(f, 'hi');
    assert.equal(assertReadableTarget(f), null);
  });

  it('returns an error ToolResult for a missing path', () => {
    const dir = mkTmp();
    const missing = path.join(dir, 'does-not-exist');
    const res = assertReadableTarget(missing);
    assert.ok(res);
    assert.equal(res!.isError, true);
    assert.match(res!.output, /search failed — cannot read/);
    assert.ok(res!.output.includes(missing));
  });
});

// ---------------------------------------------------------------------------
// rejectEscapingPattern — glob escape guard
// ---------------------------------------------------------------------------

describe('rejectEscapingPattern', () => {
  it('rejects a pattern containing a .. segment', () => {
    const res = rejectEscapingPattern('../../**');
    assert.ok(res);
    assert.equal(res!.isError, true);
    assert.match(res!.output, /must not contain "\.\." path segments/);
  });

  it('rejects a backslash-separated .. segment after normalization', () => {
    const res = rejectEscapingPattern('..\\secret\\*');
    assert.ok(res);
    assert.equal(res!.isError, true);
  });

  it('allows a safe relative-down pattern', () => {
    assert.equal(rejectEscapingPattern('**/*.ts'), null);
    assert.equal(rejectEscapingPattern('src/**'), null);
    // A filename that merely contains dots (not a .. segment) is allowed.
    assert.equal(rejectEscapingPattern('a..b/*.ts'), null);
  });
});

// ---------------------------------------------------------------------------
// toolCallFingerprint / deduplicateToolCalls
// ---------------------------------------------------------------------------

describe('toolCallFingerprint', () => {
  it('is order-independent over input keys', () => {
    const a = toolCallFingerprint('bash', { command: 'ls', cwd: '/x' });
    const b = toolCallFingerprint('bash', { cwd: '/x', command: 'ls' });
    assert.equal(a, b);
  });

  it('differs when a value differs', () => {
    const a = toolCallFingerprint('bash', { command: 'ls' });
    const b = toolCallFingerprint('bash', { command: 'pwd' });
    assert.notEqual(a, b);
  });
});

describe('deduplicateToolCalls', () => {
  const mk = (id: string, name: string, args: string) => ({
    id,
    type: 'function' as const,
    function: { name, arguments: args },
  });

  it('removes a later identical call and reports its id', () => {
    const calls = [
      mk('1', 'bash', '{"command":"ls"}'),
      mk('2', 'bash', '{"command":"ls"}'),
      mk('3', 'bash', '{"command":"pwd"}'),
    ];
    const r = deduplicateToolCalls(calls as never);
    assert.equal(r.deduplicated.length, 2);
    assert.equal(r.removedCount, 1);
    assert.deepEqual(r.removedIds, ['2']);
  });

  it('falls back to raw-argument fingerprint when arguments are not valid JSON', () => {
    const calls = [
      mk('1', 'bash', 'not-json'),
      mk('2', 'bash', 'not-json'),
    ];
    const r = deduplicateToolCalls(calls as never);
    // Both have identical raw args, so the second is a duplicate.
    assert.equal(r.deduplicated.length, 1);
    assert.deepEqual(r.removedIds, ['2']);
  });
});

// ---------------------------------------------------------------------------
// buildToolDescriptionContext — defensive catch arms via the existsSync seam
// ---------------------------------------------------------------------------

describe('buildToolDescriptionContext', () => {
  it('reports hasGit/hasPackageJson from existsSync results', () => {
    const dir = mkTmp();
    fs.mkdirSync(path.join(dir, '.git'));
    fs.writeFileSync(path.join(dir, 'package.json'), '{}');
    const ctx = buildToolDescriptionContext(dir);
    assert.equal(ctx.hasGit, true);
    assert.equal(ctx.hasPackageJson, true);
    assert.equal(ctx.projectName, path.basename(dir));
  });

  it('falls back to false when existsSync throws for both probes', () => {
    mock.method(_toolUtilsHooks, 'existsSync', () => { throw new Error('boom'); });
    const ctx = buildToolDescriptionContext('/some/dir');
    assert.equal(ctx.hasGit, false);
    assert.equal(ctx.hasPackageJson, false);
  });
});

// ---------------------------------------------------------------------------
// resolveFilePath — symlinked-parent escape for a not-yet-existing file
// ---------------------------------------------------------------------------

describe('resolveFilePath — parent symlink escape', () => {
  it('rejects a new file whose parent dir is a symlink resolving outside the workspace', () => {
    const outside = mkTmp();
    const cwd = mkTmp();
    // cwd/escape -> outside (a directory symlink pointing out of the workspace)
    const linkDir = path.join(cwd, 'escape');
    fs.symlinkSync(outside, linkDir);
    // Target file does not exist yet, so resolveFilePath hits the parent-dir realpath branch.
    const res = resolveFilePath('escape/newfile.txt', cwd);
    assert.ok(res.error);
    assert.equal(res.error!.isError, true);
    assert.match(res.error!.output, /resolves outside the workspace via symlink/);
  });

  it('accepts a new file whose parent is a real in-workspace dir', () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, 'sub'));
    const res = resolveFilePath('sub/newfile.txt', cwd);
    assert.equal(res.error, undefined);
    assert.equal(res.resolved, path.join(cwd, 'sub', 'newfile.txt'));
  });
});

// ---------------------------------------------------------------------------
// safeReadFile — symlink re-check branches driven by the realpathSync seam
// ---------------------------------------------------------------------------

describe('safeReadFile', () => {
  it('reads a plain in-workspace file', () => {
    const cwd = mkTmp();
    fs.writeFileSync(path.join(cwd, 'f.txt'), 'hello');
    const r = safeReadFile('f.txt', cwd);
    assert.equal(r.error, undefined);
    assert.equal(r.content, 'hello');
    assert.equal(r.encoding, 'utf-8');
  });

  it('blocks a symlink whose TOCTOU-resolved target is a credential path', () => {
    const cwd = mkTmp();
    const realTarget = path.join(cwd, 'target.txt');
    fs.writeFileSync(realTarget, 'secretish');
    const link = path.join(cwd, 'link.txt');
    fs.symlinkSync(realTarget, link);
    // resolveFilePath's real realpathSync keeps the link inside the workspace;
    // override the post-resolve re-check seam to simulate a swap to a credential path.
    mock.method(_toolUtilsHooks, 'realpathSync', () => '/home/victim/.ssh/id_rsa');
    const r = safeReadFile('link.txt', cwd);
    assert.ok(r.error);
    assert.equal(r.error!.isError, true);
    assert.match(r.error!.output, /symlink targeting a sensitive file/);
  });

  it('blocks a symlink whose TOCTOU-resolved target escapes the workspace', () => {
    const cwd = mkTmp();
    const realTarget = path.join(cwd, 'target.txt');
    fs.writeFileSync(realTarget, 'data');
    const link = path.join(cwd, 'link.txt');
    fs.symlinkSync(realTarget, link);
    mock.method(_toolUtilsHooks, 'realpathSync', () => '/var/outside/workspace.txt');
    const r = safeReadFile('link.txt', cwd);
    assert.ok(r.error);
    assert.equal(r.error!.isError, true);
    assert.match(r.error!.output, /symlink resolving outside the workspace/);
  });

  it('allows a symlink whose resolved target stays inside the workspace', () => {
    const cwd = mkTmp();
    const realTarget = path.join(cwd, 'target.txt');
    fs.writeFileSync(realTarget, 'inside');
    const link = path.join(cwd, 'link.txt');
    fs.symlinkSync(realTarget, link);
    const r = safeReadFile('link.txt', cwd);
    assert.equal(r.error, undefined);
    assert.equal(r.content, 'inside');
  });

  it('returns an error when the resolve step rejects the path (outside workspace)', () => {
    const cwd = mkTmp();
    const other = mkTmp();
    const r = safeReadFile(path.join(other, 'x.txt'), cwd);
    assert.ok(r.error);
    assert.equal(r.error!.isError, true);
  });

  it('returns a read error for a missing in-workspace file', () => {
    const cwd = mkTmp();
    const r = safeReadFile('missing.txt', cwd);
    assert.ok(r.error);
    assert.match(r.error!.output, /cannot read/);
  });
});

// ---------------------------------------------------------------------------
// safeLstat — outside-workspace guard + stat error + success
// ---------------------------------------------------------------------------

describe('safeLstat', () => {
  it('rejects a path outside the workspace', () => {
    const cwd = mkTmp();
    const other = mkTmp();
    const r = safeLstat(path.join(other, 'x.txt'), cwd);
    assert.ok(r.error);
    assert.match(r.error!.output, /outside the workspace/);
  });

  it('returns stats for an existing in-workspace file', () => {
    const cwd = mkTmp();
    fs.writeFileSync(path.join(cwd, 'f.txt'), 'x');
    const r = safeLstat('f.txt', cwd);
    assert.equal(r.error, undefined);
    assert.equal(r.stat!.isFile(), true);
  });

  it('returns an error when the file cannot be stat-ed', () => {
    const cwd = mkTmp();
    const r = safeLstat('nope.txt', cwd);
    assert.ok(r.error);
    assert.match(r.error!.output, /cannot stat/);
  });
});

// ---------------------------------------------------------------------------
// isNonUtfEncoding — invalid 3-byte lead sequence branch
// ---------------------------------------------------------------------------

describe('isNonUtfEncoding — invalid multibyte lead bytes', () => {
  it('flags repeated invalid 3-byte lead sequences as non-UTF-8', () => {
    // 0xE0 is a 3-byte lead; follow each with ASCII (invalid continuation) so the
    // E0-branch increments invalidUtf8Runs without a valid continuation.
    const buf = Buffer.from([0xE0, 0x41, 0xE0, 0x42, 0xE0, 0x43]);
    assert.equal(isNonUtfEncoding(buf), true);
  });

  it('flags repeated invalid 2-byte lead sequences as non-UTF-8', () => {
    // 0xC0 is a 2-byte lead; follow each with ASCII (invalid continuation).
    const buf = Buffer.from([0xC0, 0x41, 0xC0, 0x42, 0xC0, 0x43]);
    assert.equal(isNonUtfEncoding(buf), true);
  });

  it('flags repeated invalid 4-byte lead sequences as non-UTF-8', () => {
    const buf = Buffer.from([0xF0, 0x41, 0xF0, 0x42, 0xF0, 0x43]);
    assert.equal(isNonUtfEncoding(buf), true);
  });

  it('does not flag valid multibyte UTF-8', () => {
    // Three valid 3-byte sequences (e.g. U+20AC euro sign).
    const buf = Buffer.from('€€€€', 'utf-8');
    assert.equal(isNonUtfEncoding(buf), false);
  });
});

// ---------------------------------------------------------------------------
// convertToLineEnding — both branches
// ---------------------------------------------------------------------------

describe('convertToLineEnding — branch coverage', () => {
  it('converts normalized LF to CRLF', () => {
    assert.equal(convertToLineEnding('a\r\nb\nc', '\r\n'), 'a\r\nb\r\nc');
  });

  it('returns normalized LF unchanged when ending is LF', () => {
    assert.equal(convertToLineEnding('a\r\nb\r\n', '\n'), 'a\nb\n');
  });
});
