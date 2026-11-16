import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// [functional change — U030] The bash tool no longer fails closed when its cwd
// has been deleted out from under the session. It now resolves the working
// directory TOCTOU-safely (realpath, no existsSync pre-check) and RECOVERS to the
// launch directory (process.cwd()) when the cwd is gone, surfacing a
// "Shell cwd was reset to …" note. A hard error is returned only when neither the
// cwd nor the fallback can be resolved. Parity: Claude Code Shell.ts setCwd +
// delete-recovery.
describe('bashTool — cwd validation / recovery', () => {
  afterEach(() => mock.restoreAll());

  it('succeeds when cwd exists', async () => {
    const result = await bashTool.execute(
      { command: 'echo ok' },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(!result.isError, `expected success, got: ${result.output}`);
    assert.ok(result.output.includes('ok'));
  });

  it('recovers to the launch dir when cwd does not exist', async () => {
    const gone = path.join(os.tmpdir(), `bash-cwd-test-gone-${Date.now()}`);
    const result = await bashTool.execute(
      { command: 'echo recovered' },
      ctx({ cwd: gone }),
    );
    assert.ok(!result.isError, `expected recovery, got: ${result.output}`);
    assert.ok(result.output.includes('recovered'));
    assert.ok(result.output.includes('Shell cwd was reset to'));
  });

  it('names the missing path in the reset note', async () => {
    const gone = `/tmp/nonexistent-cwd-${Date.now()}`;
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ cwd: gone }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes(gone));
  });

  it('errors with /add-dir guidance only when recovery is impossible', async () => {
    mock.method(_bashHooks, 'realpathSync', () => {
      const e = new Error('gone') as NodeJS.ErrnoException;
      e.code = 'ENOENT';
      throw e;
    });
    const gone = `/tmp/vanished-dir-${Date.now()}`;
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('does not exist'));
    assert.ok(result.output.includes('/add-dir'));
  });

  it('empty command check fires before cwd resolution', async () => {
    const gone = `/tmp/no-such-dir-${Date.now()}`;
    const result = await bashTool.execute(
      { command: '' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('recovers for a dir that existed then was removed', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'bash-cwd-'));
    fs.rmSync(tmp, { recursive: true });
    const result = await bashTool.execute(
      { command: 'echo back-online' },
      ctx({ cwd: tmp }),
    );
    assert.ok(!result.isError, `expected recovery, got: ${result.output}`);
    assert.ok(result.output.includes('back-online'));
    assert.ok(result.output.includes(tmp));
  });

  it('works with a valid temp dir that exists', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'bash-cwd-ok-'));
    try {
      const result = await bashTool.execute(
        { command: 'echo from-temp' },
        ctx({ cwd: tmp }),
      );
      assert.ok(!result.isError, `expected success, got: ${result.output}`);
      assert.ok(result.output.includes('from-temp'));
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
