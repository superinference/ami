import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('bashTool — cwd validation', () => {
  it('succeeds when cwd exists', async () => {
    const result = await bashTool.execute(
      { command: 'echo ok' },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(!result.isError, `expected success, got: ${result.output}`);
    assert.ok(result.output.includes('ok'));
  });

  it('returns error when cwd does not exist', async () => {
    const gone = path.join(os.tmpdir(), `bash-cwd-test-gone-${Date.now()}`);
    const result = await bashTool.execute(
      { command: 'echo should-not-run' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('does not exist'));
  });

  it('error message includes the missing path', async () => {
    const gone = `/tmp/nonexistent-cwd-${Date.now()}`;
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes(gone));
  });

  it('error message suggests /add-dir', async () => {
    const gone = `/tmp/vanished-dir-${Date.now()}`;
    const result = await bashTool.execute(
      { command: 'echo nope' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('/add-dir'));
  });

  it('empty command check fires before cwd check', async () => {
    const gone = `/tmp/no-such-dir-${Date.now()}`;
    const result = await bashTool.execute(
      { command: '' },
      ctx({ cwd: gone }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('detects a dir that existed then was removed', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'bash-cwd-'));
    fs.rmSync(tmp, { recursive: true });
    const result = await bashTool.execute(
      { command: 'echo should-fail' },
      ctx({ cwd: tmp }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('does not exist'));
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
