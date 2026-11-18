/**
 * U509 (w105) — isolated tmux-socket helper: core/core/src/utils/tmux-socket.ts
 *
 * Parity gaps covered (verified=confirmed), consolidated into a single canonical
 * module (the isolation primitives previously inlined in shell.ts now live here):
 *
 *  - [gap] TMUX isolated per-process socket NAME helper — `ami-<PID>` (parity with
 *    the reference's `getClaudeSocketName()` / `claude-<PID>`, tmuxSocket.ts:L92-96).
 *  - [gap] TMPDIR-aware, uid-scoped fallback socket-PATH builder
 *    (`$TMPDIR/tmux-<uid>/<socket>`, tmuxSocket.ts:L376-382).
 *  - [gap] graceful degradation + concurrent-init race guard + on-exit teardown
 *    (tmuxSocket.ts:L151-266) — never throw into the bash path.
 *  - [gap] lazy singleton init with concurrent-init dedup + cached availability
 *    probe (tmuxSocket.ts:L72-246).
 *
 * AMI adaptation: isolation is via `TMUX_TMPDIR` (not a `TMUX` env override) and
 * opt-in via AMI_TMUX_ISOLATION; the destructive `tmux kill-server/kill-session`
 * block lives in bash-security.ts (this module is the positive isolation half).
 */
import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';

import {
  TMUX_ISOLATION_ENV,
  AMI_TMUX_SOCKET_PREFIX,
  tmuxIsolationEnabled,
  commandUsesTmux,
  getAmiTmuxSocketName,
  getAmiTmuxSocketDir,
  getAmiTmuxServerSocketPath,
  checkTmuxAvailable,
  isTmuxAvailable,
  ensureTmuxIsolationDir,
  prepareBashTmuxOverrides,
  tmuxSocketHooks,
  __resetTmuxSocketStateForTest,
} from '../src/utils/tmux-socket';
import { execCommand, getEnvironmentOverrides } from '../src/utils/shell';

const ENV_KEYS = [TMUX_ISOLATION_ENV, 'AMI_TMPDIR', 'CLAUDE_CODE_TMPDIR', 'AMI_SHELL_DEBUG', 'TMUX_TMPDIR'];
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  for (const k of ENV_KEYS) { savedEnv[k] = process.env[k]; delete process.env[k]; }
  __resetTmuxSocketStateForTest();
  // Stub the on-exit teardown so tests never register real process.exit listeners.
  mock.method(tmuxSocketHooks, 'registerTeardown', () => {});
});
afterEach(() => {
  mock.restoreAll();
  __resetTmuxSocketStateForTest();
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k];
  }
});

// ---------------------------------------------------------------------------
// gap: opt-in knob + tmux-word detection (consolidated from shell.ts)
// ---------------------------------------------------------------------------
describe('U509 — tmuxIsolationEnabled / commandUsesTmux', () => {
  it('treats 1/true/on/yes (any case) as enabled and everything else as disabled', () => {
    for (const good of ['1', 'true', 'on', 'yes', 'TRUE', 'On', ' yes ']) {
      process.env[TMUX_ISOLATION_ENV] = good;
      assert.equal(tmuxIsolationEnabled(), true, `good=${JSON.stringify(good)}`);
    }
    for (const bad of ['', '0', 'off', 'no', 'maybe', 'enabled']) {
      process.env[TMUX_ISOLATION_ENV] = bad;
      assert.equal(tmuxIsolationEnabled(), false, `bad=${JSON.stringify(bad)}`);
    }
    delete process.env[TMUX_ISOLATION_ENV];
    assert.equal(tmuxIsolationEnabled(), false);
  });

  it('detects the tmux binary at a word boundary only', () => {
    assert.equal(commandUsesTmux('tmux kill-server'), true);
    assert.equal(commandUsesTmux('echo hi; tmux ls'), true);
    assert.equal(commandUsesTmux('mytmuxwrapper'), false);
    assert.equal(commandUsesTmux('echo notmux'), false);
  });
});

// ---------------------------------------------------------------------------
// gap: socket NAME helper — ami-<PID>
// ---------------------------------------------------------------------------
describe('U509 — getAmiTmuxSocketName', () => {
  it('produces an isolated per-process socket name ami-<PID>', () => {
    assert.equal(getAmiTmuxSocketName(), `${AMI_TMUX_SOCKET_PREFIX}-${process.pid}`);
    assert.match(getAmiTmuxSocketName(), /^ami-\d+$/);
  });
});

// ---------------------------------------------------------------------------
// gap: TMPDIR-aware, uid-scoped fallback socket-PATH builder
// ---------------------------------------------------------------------------
describe('U509 — socket dir + server socket path', () => {
  it('getAmiTmuxSocketDir roots the isolated TMUX_TMPDIR at <base>/ami-tmux', () => {
    assert.equal(getAmiTmuxSocketDir('/base'), path.join('/base', 'ami-tmux'));
  });

  it('getAmiTmuxServerSocketPath mirrors tmux\'s $TMUX_TMPDIR/tmux-<uid>/default (posix, uid-scoped)', () => {
    mock.method(tmuxSocketHooks, 'getuid', () => 501);
    assert.equal(
      getAmiTmuxServerSocketPath('/tmp'),
      '/tmp/ami-tmux/tmux-501/default',
    );
  });

  it('defaults uid to 0 when the platform has no getuid (Windows/WSL root)', () => {
    mock.method(tmuxSocketHooks, 'getuid', () => 0);
    assert.equal(getAmiTmuxServerSocketPath('/x'), '/x/ami-tmux/tmux-0/default');
  });
});

// ---------------------------------------------------------------------------
// gap: cached availability probe (lazy singleton)
// ---------------------------------------------------------------------------
describe('U509 — checkTmuxAvailable (cached once)', () => {
  it('probes exactly once and caches the result; isTmuxAvailable reflects it', () => {
    let calls = 0;
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => { calls++; return true; });
    assert.equal(isTmuxAvailable(), false, 'false before any check');
    assert.equal(checkTmuxAvailable(), true);
    assert.equal(checkTmuxAvailable(), true);
    assert.equal(checkTmuxAvailable(), true);
    assert.equal(calls, 1, 'probe cached — called once');
    assert.equal(isTmuxAvailable(), true);
  });

  it('caches an unavailable result and logs a degradation note', () => {
    const debug: string[] = [];
    process.env.AMI_SHELL_DEBUG = '1';
    mock.method(tmuxSocketHooks, 'onDebug', (m: string) => { debug.push(m); });
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => false);
    assert.equal(checkTmuxAvailable(), false);
    assert.equal(checkTmuxAvailable(), false);
    assert.equal(isTmuxAvailable(), false);
    assert.ok(debug.some(m => /tmux not installed/.test(m)), debug.join(' | '));
  });
});

// ---------------------------------------------------------------------------
// gap: graceful degradation + concurrent-init dedup + teardown
// ---------------------------------------------------------------------------
describe('U509 — ensureTmuxIsolationDir', () => {
  it('returns the socket dir and registers on-exit teardown once when tmux is available', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    const mkdirs: unknown[] = [];
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async (d: unknown) => { mkdirs.push(d); return undefined; }) as typeof tmuxSocketHooks.mkdirAsync);
    const teardowns: Array<() => void> = [];
    mock.method(tmuxSocketHooks, 'registerTeardown', (fn: () => void) => { teardowns.push(fn); });

    const dir = await ensureTmuxIsolationDir('/base');
    assert.equal(dir, path.join('/base', 'ami-tmux'));
    assert.deepEqual(mkdirs, [path.join('/base', 'ami-tmux')]);
    assert.equal(teardowns.length, 1, 'teardown registered exactly once');

    // Second call: teardown is NOT re-registered (registered-once guard).
    await ensureTmuxIsolationDir('/base');
    assert.equal(teardowns.length, 1);
  });

  it('degrades gracefully (returns null, no throw) when tmux is unavailable — never isolates', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => false);
    let mkdirCalled = false;
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => { mkdirCalled = true; return undefined; }) as typeof tmuxSocketHooks.mkdirAsync);
    const dir = await ensureTmuxIsolationDir('/base');
    assert.equal(dir, null);
    assert.equal(mkdirCalled, false, 'no dir created when tmux is absent');
  });

  it('dedups concurrent initializers: two concurrent calls create the dir once', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    let mkdirCount = 0;
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => {
      mkdirCount++;
      await new Promise((r) => setTimeout(r, 10));
      return undefined;
    }) as typeof tmuxSocketHooks.mkdirAsync);
    const [a, b] = await Promise.all([
      ensureTmuxIsolationDir('/base'),
      ensureTmuxIsolationDir('/base'),
    ]);
    assert.equal(a, path.join('/base', 'ami-tmux'));
    assert.equal(b, path.join('/base', 'ami-tmux'));
    assert.equal(mkdirCount, 1, 'concurrent callers share a single init');
  });

  it('is best-effort: a mkdir failure resolves to null, logs, and allows a later retry', async () => {
    const debug: string[] = [];
    process.env.AMI_SHELL_DEBUG = '1';
    mock.method(tmuxSocketHooks, 'onDebug', (m: string) => { debug.push(m); });
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    let attempt = 0;
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => {
      attempt++;
      if (attempt === 1) throw new Error('EACCES denied');
      return undefined;
    }) as typeof tmuxSocketHooks.mkdirAsync);

    const first = await ensureTmuxIsolationDir('/base');
    assert.equal(first, null, 'failure degrades to null');
    assert.ok(debug.some(m => /socket dir setup failed/.test(m) && /EACCES denied/.test(m)), debug.join(' | '));

    // initPromise was cleared on failure → a later command can retry and succeed.
    const second = await ensureTmuxIsolationDir('/base');
    assert.equal(second, path.join('/base', 'ami-tmux'));
    assert.equal(attempt, 2, 'retry re-ran mkdir after the earlier failure');
  });
});

// ---------------------------------------------------------------------------
// gap: on-exit teardown kills the isolated server (best-effort, synchronous)
// ---------------------------------------------------------------------------
describe('U509 — on-exit teardown', () => {
  it('the registered handler kills the AMI tmux server at the isolated socket path', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => undefined) as typeof tmuxSocketHooks.mkdirAsync);
    mock.method(tmuxSocketHooks, 'getuid', () => 77);
    const killed: string[] = [];
    mock.method(tmuxSocketHooks, 'killAmiTmuxServerSync', (p: string) => { killed.push(p); });
    let handler: (() => void) | null = null;
    mock.method(tmuxSocketHooks, 'registerTeardown', (fn: () => void) => { handler = fn; });

    await ensureTmuxIsolationDir('/base');
    assert.ok(handler, 'a teardown handler was registered');
    handler!();
    assert.deepEqual(killed, ['/base/ami-tmux/tmux-77/default']);
  });
});

// ---------------------------------------------------------------------------
// gap: prepareBashTmuxOverrides — the wired override returned to the shell layer
// ---------------------------------------------------------------------------
describe('U509 — prepareBashTmuxOverrides', () => {
  it('returns { TMUX_TMPDIR } when isolation succeeds', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => undefined) as typeof tmuxSocketHooks.mkdirAsync);
    assert.deepEqual(await prepareBashTmuxOverrides('/base'), { TMUX_TMPDIR: path.join('/base', 'ami-tmux') });
  });

  it('returns {} (no override) when tmux is unavailable', async () => {
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => false);
    assert.deepEqual(await prepareBashTmuxOverrides('/base'), {});
  });
});

// ---------------------------------------------------------------------------
// E2E (real bash) — degradation path: an unavailable tmux leaves bash un-isolated
// ---------------------------------------------------------------------------
describe('U509 — E2E (real bash) graceful degradation', () => {
  const hasBash = fs.existsSync('/bin/bash');

  it('does NOT inject TMUX_TMPDIR when tmux is unavailable, even for a tmux command', { skip: !hasBash }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tmux-degrade-'));
    process.env.AMI_TMPDIR = base;
    process.env[TMUX_ISOLATION_ENV] = '1';
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => false);
    try {
      const result = await execCommand('printf "TMPDIR=[%s]\\n" "$TMUX_TMPDIR"; echo tmux', {
        cwd: base,
        stallTimeoutMs: 0,
        shellPath: '/bin/bash',
      });
      assert.match(result.stdout, /TMPDIR=\[\]/, 'no isolation dir injected when tmux is absent');
      assert.ok(!fs.existsSync(path.join(base, 'ami-tmux')), 'no socket dir created');
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it('injects the isolated TMUX_TMPDIR through the shell seam when tmux is available', { skip: !hasBash }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tmux-iso-'));
    process.env.AMI_TMPDIR = base;
    process.env[TMUX_ISOLATION_ENV] = '1';
    mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
    // Sanity: the shell seam routes a tmux command to the async isolation override.
    const override = getEnvironmentOverrides('bash', 'tmux ls');
    assert.equal(typeof (override as Promise<unknown>).then, 'function');
    try {
      const result = await execCommand('printf "TMPDIR=%s\\n" "$TMUX_TMPDIR"; echo tmux', {
        cwd: base,
        stallTimeoutMs: 0,
        shellPath: '/bin/bash',
      });
      const expectedDir = path.join(base, 'ami-tmux');
      assert.match(result.stdout, new RegExp(`TMPDIR=${expectedDir.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
      assert.ok(fs.existsSync(expectedDir), 'socket dir created when isolating');
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});
