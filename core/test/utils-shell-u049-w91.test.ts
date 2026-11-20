/**
 * U049 (w91) — ripgrep shell integration in the shell snapshot.
 *
 * Covers `buildRipgrepShellIntegration` (parity with the reference's
 * `createRipgrepShellIntegration` / `createArgv0ShellFunction`, ShellSnapshot.ts)
 * and its wiring into `createShellSnapshot` so a model-typed `rg` in a sourced
 * snapshot dispatches to the SAME ripgrep binary AMI's grep/glob tools use.
 *
 * Includes real-bash E2E runs proving: (a) the generated function routes `rg`
 * to the configured binary when no system `rg` is present, (b) a present system
 * `rg` on PATH wins (the guard yields), and (c) the ARGV0 dispatch sets argv[0]
 * and injects the configured prepend args.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as child_process from 'child_process';

import {
  buildRipgrepShellIntegration,
  createShellSnapshot,
  shellHooks,
} from '../src/utils/shell';
import { RIPGREP_PATH_ENV, USE_BUILTIN_RIPGREP_ENV, type RipgrepConfig } from '../src/tools/ripgrep-config';

const savedEnv: Record<string, string | undefined> = {};
afterEach(() => {
  mock.restoreAll();
  for (const k of [RIPGREP_PATH_ENV, USE_BUILTIN_RIPGREP_ENV]) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
    delete savedEnv[k];
  }
});
function stashEnv(k: string, v: string | undefined): void {
  savedEnv[k] = process.env[k];
  if (v === undefined) delete process.env[k];
  else process.env[k] = v;
}

const bashPath = fs.existsSync('/bin/bash') ? '/bin/bash' : (process.env.SHELL || '/bin/bash');
function runBash(script: string, env: NodeJS.ProcessEnv): string {
  return child_process.execFileSync(bashPath, ['-c', script], { encoding: 'utf8', env });
}
function mkExec(dir: string, name: string, body: string): string {
  const p = path.join(dir, name);
  fs.writeFileSync(p, `#!/bin/sh\n${body}\n`);
  fs.chmodSync(p, 0o755);
  return p;
}

describe('U049 w91 — buildRipgrepShellIntegration', () => {
  it('returns null in system mode (nothing to dispatch)', () => {
    stashEnv(RIPGREP_PATH_ENV, undefined);
    assert.equal(buildRipgrepShellIntegration(), null);
    assert.equal(buildRipgrepShellIntegration({ mode: 'system', command: 'rg' }), null);
  });

  it('emits a guarded function execing the configured path (builtin, no argv0)', () => {
    const snippet = buildRipgrepShellIntegration({ mode: 'builtin', command: '/opt/rg bin/rg' });
    assert.ok(snippet);
    // System-rg-absent guard with the unalias-first probe (the reference parity).
    assert.match(snippet!, /if ! \(unalias rg 2>\/dev\/null; command -v rg\) >\/dev\/null 2>&1; then/);
    assert.match(snippet!, /^fi$/m);
    // Path POSIX-quoted (contains a space) and NOT the ARGV0 form.
    assert.match(snippet!, /function rg \{/);
    assert.match(snippet!, /'\/opt\/rg bin\/rg' "\$@"/);
    assert.doesNotMatch(snippet!, /ARGV0=/);
    assert.doesNotMatch(snippet!, /exec -a/);
  });

  it('quotes config-level extra args in the non-argv0 function form', () => {
    const snippet = buildRipgrepShellIntegration({ mode: 'config', command: '/bin/rg', args: ['--hidden', "a'b"] });
    assert.match(snippet!, /'\/bin\/rg' '--hidden' 'a'\\''b' "\$@"/);
  });

  it('emits the four-branch ARGV0 dispatch function for a multi-call binary', () => {
    const cfg: RipgrepConfig = { mode: 'config', command: '/busybox/rg', args: ['--smart-case'], argv0: 'rg' };
    const snippet = buildRipgrepShellIntegration(cfg)!;
    assert.match(snippet, /if \[\[ -n \$ZSH_VERSION \]\]; then/);
    assert.match(snippet, /elif \[\[ "\$OSTYPE" == "msys" \]\]/);
    assert.match(snippet, /elif \[\[ \$BASHPID != \$\$ \]\]; then/);
    assert.match(snippet, /exec -a rg '\/busybox\/rg' --smart-case "\$@"/);
    assert.match(snippet, /\(exec -a rg '\/busybox\/rg' --smart-case "\$@"\)/);
    // ARGV0 env-var branch for zsh / windows.
    assert.match(snippet, /ARGV0=rg '\/busybox\/rg' --smart-case "\$@"/);
  });
});

describe('U049 w91 — createShellSnapshot wiring', () => {
  it('embeds the ripgrep integration heredoc in the capture script when configured', () => {
    stashEnv(RIPGREP_PATH_ENV, '/custom/rg');
    stashEnv(USE_BUILTIN_RIPGREP_ENV, '1');
    let captured = '';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => {
      captured = args[args.length - 1];
      return '';
    }) as typeof shellHooks.execFileSync);
    const file = createShellSnapshot('/bin/bash');
    assert.ok(file, 'snapshot path returned');
    assert.match(captured, /cat >> "\$SNAPSHOT_FILE" <<'AMI_RG_SNIPPET_EOF'/);
    assert.match(captured, /# AMI ripgrep shell integration/);
    assert.match(captured, /'\/custom\/rg' "\$@"/);
  });

  it('embeds the no-integration comment in the capture script for system ripgrep', () => {
    stashEnv(RIPGREP_PATH_ENV, undefined);
    let captured = '';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => {
      captured = args[args.length - 1];
      return '';
    }) as typeof shellHooks.execFileSync);
    createShellSnapshot('/bin/bash');
    assert.match(captured, /# No ripgrep shell integration \(system ripgrep\)/);
    assert.doesNotMatch(captured, /AMI_RG_SNIPPET_EOF/);
  });
});

describe('U049 w91 — ripgrep integration E2E (real bash)', () => {
  it('routes rg to the configured binary when no system rg is present', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-e2e-'));
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-empty-'));
    try {
      const fake = mkExec(dir, 'myrg', 'echo "ripgrep CONFIG $*"');
      const snippet = buildRipgrepShellIntegration({ mode: 'builtin', command: fake })!;
      const out = runBash(`${snippet}\nrg --version`, { PATH: empty });
      assert.match(out, /ripgrep CONFIG --version/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });

  it('yields to a system rg already present on PATH (guard skips the function)', () => {
    const cfgDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-cfg-'));
    const sysDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-sys-'));
    try {
      const fake = mkExec(cfgDir, 'myrg', 'echo "CONFIG"');
      mkExec(sysDir, 'rg', 'echo "SYSTEM"');
      const snippet = buildRipgrepShellIntegration({ mode: 'builtin', command: fake })!;
      const out = runBash(`${snippet}\nrg x`, { PATH: sysDir });
      assert.match(out, /SYSTEM/);
      assert.doesNotMatch(out, /CONFIG/);
    } finally {
      fs.rmSync(cfgDir, { recursive: true, force: true });
      fs.rmSync(sysDir, { recursive: true, force: true });
    }
  });

  it('dispatches through the ARGV0 function, injecting prepend + user args', () => {
    // A `#!/bin/sh` fake cannot observe argv[0] (the kernel resets `$0` to the
    // script path), so the ARGV0 *value* is asserted structurally in the unit
    // test above; here we prove the ARGV0 function path actually EXECUTES the
    // configured binary and forwards `<prependArgs> "$@"`.
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-argv0-'));
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rg-a0empty-'));
    try {
      const fake = mkExec(dir, 'multicall', 'echo "args=$*"');
      const snippet = buildRipgrepShellIntegration({ mode: 'config', command: fake, args: ['--smart-case'], argv0: 'rg' })!;
      const out = runBash(`${snippet}\nrg query`, { PATH: empty });
      assert.match(out, /args=--smart-case query/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });
});
