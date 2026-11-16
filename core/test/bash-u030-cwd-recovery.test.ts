import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  bashTool,
  resolveWorkingDirectory,
  _bashHooks,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// U030 — resolveWorkingDirectory (gaps: TOCTOU-safe realpath resolution +
// delete-recovery to the launch directory)
// ---------------------------------------------------------------------------

describe('resolveWorkingDirectory', () => {
  afterEach(() => mock.restoreAll());

  it('resolves an existing directory (no recovery) and canonicalizes it', () => {
    const r = resolveWorkingDirectory(process.cwd());
    assert.ok(r);
    assert.equal(r!.recovered, false);
    assert.equal(r!.dir, fs.realpathSync(process.cwd(), 'utf8'));
  });

  it('canonicalizes a symlinked directory', () => {
    const real = fs.mkdtempSync(path.join(os.tmpdir(), 'u030-real-'));
    const link = path.join(os.tmpdir(), `u030-link-${Date.now()}`);
    fs.symlinkSync(real, link, 'dir');
    try {
      const r = resolveWorkingDirectory(link);
      assert.ok(r);
      assert.equal(r!.recovered, false);
      assert.equal(r!.dir, fs.realpathSync(real, 'utf8'));
    } finally {
      fs.rmSync(link, { force: true });
      fs.rmSync(real, { recursive: true, force: true });
    }
  });

  it('recovers to the fallback launch dir when the cwd is gone', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u030-gone-'));
    fs.rmSync(tmp, { recursive: true, force: true });
    const fallback = fs.mkdtempSync(path.join(os.tmpdir(), 'u030-fallback-'));
    try {
      const r = resolveWorkingDirectory(tmp, fallback);
      assert.ok(r);
      assert.equal(r!.recovered, true);
      assert.equal(r!.dir, fs.realpathSync(fallback, 'utf8'));
    } finally {
      fs.rmSync(fallback, { recursive: true, force: true });
    }
  });

  it('returns null when the cwd is gone and equals the fallback', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u030-same-'));
    fs.rmSync(tmp, { recursive: true, force: true });
    assert.equal(resolveWorkingDirectory(tmp, tmp), null);
  });

  it('returns null when neither cwd nor fallback resolves', () => {
    mock.method(_bashHooks, 'realpathSync', () => {
      const e = new Error('nope') as NodeJS.ErrnoException;
      e.code = 'ENOENT';
      throw e;
    });
    assert.equal(resolveWorkingDirectory('/a/b/gone', '/c/d/also-gone'), null);
  });

  it('returns null (does not recover) on a non-ENOENT error', () => {
    mock.method(_bashHooks, 'realpathSync', () => {
      const e = new Error('denied') as NodeJS.ErrnoException;
      e.code = 'EACCES';
      throw e;
    });
    assert.equal(resolveWorkingDirectory('/protected/dir'), null);
  });
});

// ---------------------------------------------------------------------------
// U030 — end-to-end through bashTool.execute
// ---------------------------------------------------------------------------

describe('bashTool — cwd delete-recovery (e2e)', () => {
  afterEach(() => mock.restoreAll());

  it('recovers to the launch dir and runs the command when cwd was deleted', async () => {
    const gone = fs.mkdtempSync(path.join(os.tmpdir(), 'u030-e2e-gone-'));
    fs.rmSync(gone, { recursive: true, force: true });
    const result = await bashTool.execute(
      { command: 'echo recovered-here' },
      ctx({ cwd: gone }),
    );
    assert.ok(!result.isError, `expected recovery success, got: ${result.output}`);
    assert.ok(result.output.includes('recovered-here'));
    assert.ok(result.output.includes('Shell cwd was reset to'), `expected reset note, got: ${result.output}`);
    assert.ok(result.output.includes(gone), 'reset note should name the vanished cwd');
  });

  it('errors with /add-dir guidance only when recovery is impossible', async () => {
    // Force both the cwd and the fallback to be unresolvable.
    mock.method(_bashHooks, 'realpathSync', () => {
      const e = new Error('gone') as NodeJS.ErrnoException;
      e.code = 'ENOENT';
      throw e;
    });
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ cwd: '/tmp/u030-vanished' }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('does not exist'));
    assert.ok(result.output.includes('/add-dir'));
  });
});
