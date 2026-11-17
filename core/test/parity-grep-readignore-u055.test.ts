/**
 * U055 — Per-file read-ignore / read-deny exclusions in search (parity).
 *
 * Claude Code injects the permission layer's file-READ deny patterns into ripgrep as
 * negated globs (`--glob !pattern`) so a file the user forbade reading (e.g. `.env`,
 * credentials) never surfaces in Grep output — in EVERY output mode, including content
 * mode which would otherwise print its matching LINES. AMI previously injected only a
 * fixed VCS-dir exclusion list and consulted no read-deny patterns at all, so a
 * content-mode grep could leak a denied file's contents.
 *
 * This suite covers the new `read-ignore` helpers, PermissionManager.getFileReadIgnorePatterns,
 * and that grep honors `context.readIgnorePatterns` on the rg path (arg injection) AND on
 * the in-process fallback (the file is never read). The RED state: without the fix the
 * in-process fallback returns the denied file's matching line.
 *
 * Run: cd core/core && node --import tsx --test test/parity-grep-readignore-u055.test.ts
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, searchFilesForPattern } from '../src/tools/grep';
import { toRgIgnoreGlob, toGrepExcludeGlob, matchesReadIgnore, toAnyDepthGlob } from '../src/tools/read-ignore';
import { PermissionManager } from '../src/permissions';
import type { ToolContext } from '../src/types';

let tmpDir: string;
function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-ri-')); });
afterEach(() => { mock.restoreAll(); fs.rmSync(tmpDir, { recursive: true, force: true }); });

// ---------------------------------------------------------------------------
describe('read-ignore helpers', () => {
  it('toRgIgnoreGlob negates and anchors per ripgrep cwd-relative ignore rule', () => {
    assert.equal(toRgIgnoreGlob('.env'), '!**/.env');
    assert.equal(toRgIgnoreGlob('*.key'), '!**/*.key');
    assert.equal(toRgIgnoreGlob('/abs/secret'), '!/abs/secret');
    assert.equal(toRgIgnoreGlob('**/.env'), '!**/.env', 'already any-depth → no double prefix');
  });

  it('toAnyDepthGlob is the positive form used by the glob tool (no leading !)', () => {
    assert.equal(toAnyDepthGlob('.env'), '**/.env');
    assert.equal(toAnyDepthGlob('**/.env'), '**/.env');
    assert.equal(toAnyDepthGlob('/abs/secret'), '/abs/secret');
    assert.equal(toRgIgnoreGlob('.env'), `!${toAnyDepthGlob('.env')}`, 'rg form is just the negated any-depth glob');
  });

  it('toGrepExcludeGlob reduces a path glob to a basename glob for GNU grep --exclude', () => {
    assert.equal(toGrepExcludeGlob('**/.env'), '.env');
    assert.equal(toGrepExcludeGlob('/secrets/.env'), '.env');
    assert.equal(toGrepExcludeGlob('*.key'), '*.key');
    assert.equal(toGrepExcludeGlob('config/*.pem'), '*.pem');
  });

  it('matchesReadIgnore matches on full relative path and basename', () => {
    assert.equal(matchesReadIgnore('.env', ['**/.env']), true);
    assert.equal(matchesReadIgnore('src/app/.env', ['**/.env']), true);
    assert.equal(matchesReadIgnore('src/app/.env', ['.env']), true);
    assert.equal(matchesReadIgnore('src/keys/id.key', ['src/keys/*.key']), true);
    assert.equal(matchesReadIgnore('src/app.ts', ['**/.env']), false);
    assert.equal(matchesReadIgnore('notes.txt', []), false);
  });

  it('matchesReadIgnore normalizes Windows separators', () => {
    assert.equal(matchesReadIgnore('src\\config\\.env', ['**/.env']), true);
  });
});

// ---------------------------------------------------------------------------
describe('PermissionManager.getFileReadIgnorePatterns', () => {
  it('collects read-deny path globs and ignores allow/ask/non-read/param rules', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'file_read', pattern: '**/.env', action: 'deny' },
      { tool: 'Read', pattern: '*.key', action: 'deny' },            // Claude alias
      { tool: 'file_read', pattern: '**/.env', action: 'deny' },     // dup → deduped
      { tool: 'file_read', pattern: 'ok.txt', action: 'allow' },     // allow → skip
      { tool: 'bash', pattern: 'rm *', action: 'deny' },             // non-read → skip
      { tool: 'file_read', action: 'deny' },                         // no pattern → skip
      { tool: 'file_read', paramPattern: { path: 'x' }, action: 'deny' }, // param → skip
    ]);
    assert.deepEqual(pm.getFileReadIgnorePatterns(), ['**/.env', '*.key']);
  });

  it('returns empty when no read-deny rules exist', () => {
    assert.deepEqual(new PermissionManager('auto-allow').getFileReadIgnorePatterns(), []);
  });
});

// ---------------------------------------------------------------------------
describe('grep – read-ignore injected into the ripgrep invocation', () => {
  it('passes --glob !<pattern> for each read-ignore entry', async () => {
    let captured: string[] = [];
    mock.method(grepHooks, 'spawn', (_cmd: string, args: string[]) => {
      captured = args;
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      setImmediate(() => child.emit('close', 1, null)); // exit 1 = no matches
      return child;
    });
    await grepTool.execute!({ pattern: 'TOKEN' }, ctx({ readIgnorePatterns: ['**/.env', '*.key'] }));
    // The negated globs must be present as --glob values alongside the VCS exclusions.
    const pairs = captured.map((a, i) => (a === '--glob' ? captured[i + 1] : null)).filter(Boolean);
    assert.ok(pairs.includes('!**/.env'), `expected !**/.env in ${JSON.stringify(pairs)}`);
    assert.ok(pairs.includes('!**/*.key'), `expected !**/*.key in ${JSON.stringify(pairs)}`);
  });
});

// ---------------------------------------------------------------------------
describe('grep – in-process fallback honors read-ignore (content leak closed)', () => {
  // Force BOTH rg and grep to be "unavailable" so the in-process walker runs.
  function forceInProcess(): void {
    mock.method(grepHooks, 'spawn', () => { throw Object.assign(new Error('nope'), { code: 'ENOENT' }); });
  }

  it('excludes a denied file\'s matching lines in content mode, keeps allowed files', async () => {
    fs.writeFileSync(path.join(tmpDir, '.env'), 'API_SECRET=supersecret_value\n');
    fs.writeFileSync(path.join(tmpDir, 'app.ts'), 'const x = "supersecret_value";\n');
    forceInProcess();
    const res = await grepTool.execute!(
      { pattern: 'supersecret_value', output_mode: 'content' },
      ctx({ readIgnorePatterns: ['**/.env'] }),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /app\.ts/, 'allowed file must still match');
    assert.doesNotMatch(res.output, /\.env/, 'denied file must not appear');
    assert.doesNotMatch(res.output, /API_SECRET/, 'denied file contents must not leak');
  });

  it('RED-guard: without read-ignore the denied file IS surfaced (proves the exclusion discriminates)', async () => {
    fs.writeFileSync(path.join(tmpDir, '.env'), 'API_SECRET=supersecret_value\n');
    forceInProcess();
    const res = await grepTool.execute!(
      { pattern: 'supersecret_value', output_mode: 'content' },
      ctx(), // no readIgnorePatterns
    );
    assert.match(res.output, /\.env/, 'control: .env is found when no read-ignore is configured');
  });

  it('a denied file passed directly as the search path yields no matches', async () => {
    const env = path.join(tmpDir, '.env');
    fs.writeFileSync(env, 'API_SECRET=leak\n');
    const out = searchFilesForPattern(env, 'leak', { readIgnore: ['**/.env'] });
    assert.equal(out, '');
  });
});
