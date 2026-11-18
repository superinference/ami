/**
 * U395 (w113) — shell-snapshot parity hardening.
 *
 * Two confirmed gaps against Claude Code's ShellSnapshot.ts:
 *
 *  1. Output-size boundary + post-creation verification (ShellSnapshot.ts:L469,
 *     L521-568). The capture subprocess is capped at maxBuffer=1MB, and after a
 *     successful (exit-0) capture the artifact is re-verified via `stat().size`:
 *     a missing file is treated as failure (logs the snapshot directory's
 *     contents so the "process succeeded but produced nothing" edge is
 *     diagnosable) and the byte size is reported on success.
 *
 *  2. bash function capture base64-encodes each `declare -f` and wraps it in
 *     `eval "$(… base64 -d)"` so special characters survive the capture intact
 *     (ShellSnapshot.ts:L203-232); zsh writes `typeset -f` bodies verbatim.
 *
 * These extend the existing shell-snapshot mechanism in `core/src/utils/shell.ts`
 * (no parallel module — DEDUP-first).
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as child_process from 'child_process';

import {
  createShellSnapshot,
  resetShellSnapshotCache,
  shellHooks,
} from '../src/utils/shell';

afterEach(() => {
  mock.restoreAll();
  resetShellSnapshotCache();
});

const REAL_BASH = ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash'].find(p => fs.existsSync(p)) ?? null;

// ---------------------------------------------------------------------------
// Gap 2 — base64 function encoding for bash; verbatim typeset for zsh.
// ---------------------------------------------------------------------------
describe('U395 — bash function capture is base64-encoded (special-char safe)', () => {
  it('wraps each bash function in `eval "$(echo <b64> | base64 -d)"`', () => {
    let captured = '';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'statSync', (() => ({ size: 10 }) as fs.Stats) as typeof shellHooks.statSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => {
      captured = args[args.length - 1];
      return '';
    }) as typeof shellHooks.execFileSync);
    assert.ok(createShellSnapshot('/bin/bash'));
    // Names still enumerated via declare -F, bodies piped through base64.
    assert.match(captured, /declare -F \| cut -d' ' -f3 \| grep -vE '\^_\[\^_\]'/);
    assert.match(captured, /encoded_func=\$\(declare -f "\$func" \| base64\)/);
    assert.match(captured, /eval \\"\\\$\(echo '\$encoded_func' \| base64 -d\)\\" > \/dev\/null 2>&1/);
  });

  it('keeps zsh function bodies verbatim (no base64 eval wrapper)', () => {
    let captured = '';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'statSync', (() => ({ size: 10 }) as fs.Stats) as typeof shellHooks.statSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => {
      captured = args[args.length - 1];
      return '';
    }) as typeof shellHooks.execFileSync);
    assert.ok(createShellSnapshot('/usr/bin/zsh'));
    assert.match(captured, /typeset -f "\$func" >> "\$SNAPSHOT_FILE"/);
    assert.doesNotMatch(captured, /base64/);
  });

  it('E2E: a bash function with special characters round-trips through the snapshot', { skip: REAL_BASH ? false : 'no bash' }, () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u395-home-'));
    const prevHome = process.env.HOME;
    try {
      // A function whose body is full of characters that would corrupt a naive
      // verbatim capture: double/single quotes, $(...), backslashes, `;` and `&`.
      fs.writeFileSync(
        path.join(home, '.bashrc'),
        'u395_greet() {\n  echo "Hi, $1! \\"q\\" \'s\' $(echo sub) a&b;c \\\\done";\n}\n',
      );
      process.env.HOME = home;
      resetShellSnapshotCache();
      const snapshot = createShellSnapshot(REAL_BASH!);
      assert.ok(snapshot, 'snapshot created');
      const body = fs.readFileSync(snapshot!, 'utf8');
      // The function body is NOT present verbatim; it is base64-wrapped. (GNU
      // `base64` may wrap at 76 cols, so the payload can span lines — assert the
      // wrapper markers, not a single-line blob.)
      assert.match(body, /eval "\$\(echo '/);
      assert.match(body, /\| base64 -d\)"/);
      assert.doesNotMatch(body, /u395_greet \(\)\s*\n\{/, 'body is encoded, not verbatim');
      // Source the snapshot in a fresh bash and call the restored function.
      const out = child_process.execFileSync(
        REAL_BASH!,
        ['-c', `source ${JSON.stringify(snapshot)} 2>/dev/null; u395_greet World`],
        { encoding: 'utf8' },
      );
      assert.equal(out.trim(), 'Hi, World! "q" \'s\' sub a&b;c \\done');
    } finally {
      if (prevHome === undefined) delete process.env.HOME;
      else process.env.HOME = prevHome;
      fs.rmSync(home, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Gap 1 — post-creation stat().size verification + directory diagnostics.
// ---------------------------------------------------------------------------
describe('U395 — post-creation snapshot verification via stat().size', () => {
  it('caps the capture subprocess at maxBuffer=1MB with a 10s timeout', () => {
    let opts: child_process.ExecFileSyncOptions | undefined;
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'statSync', (() => ({ size: 42 }) as fs.Stats) as typeof shellHooks.statSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, _a: string[], o: child_process.ExecFileSyncOptions) => {
      opts = o;
      return '';
    }) as typeof shellHooks.execFileSync);
    assert.ok(createShellSnapshot('/bin/bash'));
    assert.equal(opts?.maxBuffer, 1024 * 1024);
    assert.equal(opts?.timeout, 10_000);
  });

  it('reports the artifact byte size on success', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'statSync', (() => ({ size: 2048 }) as fs.Stats) as typeof shellHooks.statSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    assert.ok(createShellSnapshot('/bin/bash'));
    assert.ok(dbg.mock.calls.some(c => /Shell snapshot created successfully \(2048 bytes\)/.test(String(c.arguments[0]))));
  });

  it('treats exit-0-but-file-missing as failure and logs the snapshot directory contents', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    // config file exists (first existsSync), snapshot artifact missing (second).
    let calls = 0;
    mock.method(shellHooks, 'existsSync', (() => { calls += 1; return calls === 1; }) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'readdirSync', (() => ['leftover.sh', 'other.sh'] as unknown as fs.Dirent[]) as typeof shellHooks.readdirSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    assert.equal(createShellSnapshot('/bin/bash'), null);
    assert.ok(dbg.mock.calls.some(c => /Snapshot file was not created/.test(String(c.arguments[0]))));
    assert.ok(dbg.mock.calls.some(c => /contains 2 file\(s\)/.test(String(c.arguments[0]))));
  });

  it('logs "not accessible" when the snapshot directory vanished', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    let calls = 0;
    mock.method(shellHooks, 'existsSync', (() => { calls += 1; return calls === 1; }) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'readdirSync', (() => { throw new Error('ENOENT'); }) as typeof shellHooks.readdirSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    assert.equal(createShellSnapshot('/bin/bash'), null);
    assert.ok(dbg.mock.calls.some(c => /Snapshot directory not accessible/.test(String(c.arguments[0]))));
  });

  it('falls back to "size unknown" when the file races away between existsSync and stat', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'statSync', (() => { throw new Error('ENOENT'); }) as typeof shellHooks.statSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    assert.ok(createShellSnapshot('/bin/bash'));
    assert.ok(dbg.mock.calls.some(c => /Shell snapshot created \(size unknown\)/.test(String(c.arguments[0]))));
  });
});
