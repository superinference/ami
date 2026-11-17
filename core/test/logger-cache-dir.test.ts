/**
 * Claude-parity (U058): the debug logger honors a platform CACHE location for its
 * transient logs when the user opts in via AMI_CACHE_DIR / XDG_CACHE_HOME (posix) /
 * %LOCALAPPDATA% (win32) — parity with cachePaths.ts `envPaths().cache`. Without one
 * of those the log dir stays the historical os.tmpdir() default (behavior unchanged).
 *
 * Run: cd core/core && node --import tsx --test test/logger-cache-dir.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { getLogPath, __resetLoggerForTest } from '../src/logger';

const SAVED = {
  AMI_CACHE_DIR: process.env.AMI_CACHE_DIR,
  XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,
  AMI_DEBUG_LOGS_DIR: process.env.AMI_DEBUG_LOGS_DIR,
  AMI_DEBUG_FILE: process.env.AMI_DEBUG_FILE,
  CLAUDE_CODE_DEBUG_LOGS_DIR: process.env.CLAUDE_CODE_DEBUG_LOGS_DIR,
};
function clearEnv(): void {
  delete process.env.AMI_CACHE_DIR;
  delete process.env.XDG_CACHE_HOME;
  delete process.env.AMI_DEBUG_LOGS_DIR;
  delete process.env.AMI_DEBUG_FILE;
  delete process.env.CLAUDE_CODE_DEBUG_LOGS_DIR;
}
afterEach(() => {
  for (const [k, v] of Object.entries(SAVED)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  __resetLoggerForTest();
});

describe('logger cache-dir honoring (U058)', () => {
  it('without any cache opt-in, the default log dir stays under os.tmpdir()', () => {
    clearEnv();
    __resetLoggerForTest();
    const p = getLogPath();
    assert.ok(p.startsWith(os.tmpdir()), `expected tmpdir default, got ${p}`);
    assert.ok(p.includes(path.join('superinference', 'core')));
  });

  it('XDG_CACHE_HOME opts transient logs into the platform cache dir', () => {
    clearEnv();
    process.env.XDG_CACHE_HOME = '/xdg/cache';
    __resetLoggerForTest();
    const p = getLogPath();
    assert.equal(p, path.join('/xdg/cache', 'superinference', 'core', 'engine.log'));
  });

  it('AMI_CACHE_DIR is the explicit cache knob and wins over the tmpdir default', () => {
    clearEnv();
    process.env.AMI_CACHE_DIR = '/custom/cache';
    __resetLoggerForTest();
    assert.equal(getLogPath(), path.join('/custom/cache', 'core', 'engine.log'));
  });

  it('AMI_DEBUG_LOGS_DIR still wins over the cache dir (explicit log-dir override)', () => {
    clearEnv();
    process.env.XDG_CACHE_HOME = '/xdg/cache';
    process.env.AMI_DEBUG_LOGS_DIR = '/explicit/logs';
    __resetLoggerForTest();
    assert.equal(getLogPath(), path.join('/explicit/logs', 'engine.log'));
  });
});
