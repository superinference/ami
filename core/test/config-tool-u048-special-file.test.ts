import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';

import { configTool } from '../src/tools/config-tool';
import type { ToolContext } from '../src/types';

/**
 * U048 parity (ConfigTool / safeResolvePath special-file guard):
 * Claude's settings reader runs safeResolvePath before reading a settings file so
 * it never calls realpathSync / readFileSync on a FIFO/socket/device (a FIFO read
 * blocks forever waiting for a writer — a silent hang). AMI's config tool must
 * likewise refuse to read a config path that is not a regular file, on both get
 * and set, instead of hanging.
 *
 * These tests create a REAL named pipe (FIFO) at the config path. Without the
 * guard, `fs.readFileSync` on the pipe blocks indefinitely and the whole suite
 * hangs; with the guard the tool returns a clear error immediately. We bound each
 * case with a short timeout so a regression surfaces as a failure, not a hang.
 */

const hasMkfifo = (() => {
  if (process.platform === 'win32') return false;
  try {
    execFileSync('mkfifo', ['--version'], { stdio: 'ignore' });
    return true;
  } catch {
    // Some BSD/macOS mkfifo lack --version; probe by creating one in a temp dir.
    try {
      const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mkfifo-probe-'));
      const p = path.join(d, 'f');
      execFileSync('mkfifo', [p]);
      fs.rmSync(d, { recursive: true, force: true });
      return true;
    } catch {
      return false;
    }
  }
})();

describe('configTool U048 — special-file (FIFO) config path guard', { skip: !hasMkfifo ? 'mkfifo unavailable on this platform' : false }, () => {
  let tmpDir: string;

  before(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'config-tool-fifo-')); });
  after(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  const fifoAtConfigPath = (): ToolContext => {
    const cwd = fs.mkdtempSync(path.join(tmpDir, 'c-'));
    const amiDir = path.join(cwd, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    execFileSync('mkfifo', [path.join(amiDir, 'config.json')]);
    return { cwd } as ToolContext;
  };

  it('get refuses a FIFO config path instead of hanging on the read', { timeout: 4000 }, async () => {
    const ctx = fifoAtConfigPath();
    const res = await configTool.execute({ action: 'get' }, ctx);
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /not a regular file|named pipe|refusing to read/i);
  });

  it('get of a single key also refuses a FIFO config path', { timeout: 4000 }, async () => {
    const ctx = fifoAtConfigPath();
    const res = await configTool.execute({ action: 'get', key: 'model' }, ctx);
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /not a regular file|named pipe|refusing to read/i);
  });

  it('set refuses a FIFO config path before attempting a write', { timeout: 4000 }, async () => {
    const ctx = fifoAtConfigPath();
    const res = await configTool.execute({ action: 'set', key: 'provider', value: 'openai' }, ctx);
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /Refusing to write/i);
    assert.match(res.output, /not a regular file|named pipe/i);
  });

  it('a regular-file config is unaffected by the guard (read still works)', { timeout: 4000 }, async () => {
    const cwd = fs.mkdtempSync(path.join(tmpDir, 'ok-'));
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), JSON.stringify({ provider: 'anthropic' }));
    const res = await configTool.execute({ action: 'get', key: 'provider' }, { cwd } as ToolContext);
    assert.equal(res.isError, undefined, res.output);
    assert.match(res.output, /anthropic/);
  });
});
