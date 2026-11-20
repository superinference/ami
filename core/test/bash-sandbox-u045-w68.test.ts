import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildSandboxTmpDir,
  wrapWithSandbox,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 (w68) — sandbox temp-dir containment parity.
//
// Claude's shell providers, when running sandboxed, pin not only TMPDIR but also
// TMPPREFIX (zsh heredoc temp files use $TMPPREFIX, default /tmp/zsh — NOT
// TMPDIR) and expose the scratch dir to child processes via CLAUDE_CODE_TMPDIR
// (AMI's AMI_SANDBOX_TMPDIR). Without TMPPREFIX a sandboxed zsh heredoc writes
// scratch files to the host /tmp, leaking outside the sandbox scratch dir.
// Ref: references/claude/claude-code-main/src/utils/shell/bashProvider.ts:L235-L247
// ---------------------------------------------------------------------------

function baseCfg(overrides: Partial<SandboxConfig> = {}): SandboxConfig {
  return {
    allowNetwork: false,
    readOnlyPaths: [],
    writablePaths: ['/tmp'],
    maxMemoryMB: 512,
    maxProcesses: 8192,
    timeout: 30000,
    ...overrides,
  };
}

function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    Object.defineProperty(process, 'platform', orig);
  }
}

describe('buildSandboxTmpDir — TMPPREFIX + exposed scratch dir', () => {
  it('is a no-op when no sandboxTmpDir is set', () => {
    assert.equal(buildSandboxTmpDir(baseCfg()), '');
  });

  it('pins TMPPREFIX inside the scratch dir so zsh heredoc temp files stay contained', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: '/var/sbx-tmp' }));
    // zsh uses TMPPREFIX (not TMPDIR) for heredoc temp files; it must be a path
    // INSIDE the scratch dir, mirroring the reference's posixJoin(tmpDir, 'zsh').
    assert.ok(
      out.includes("TMPPREFIX='/var/sbx-tmp/zsh'"),
      `expected TMPPREFIX pinned under scratch dir, got: ${out}`,
    );
  });

  it('exposes the scratch dir to child processes via AMI_SANDBOX_TMPDIR', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: '/var/sbx-tmp' }));
    assert.ok(
      out.includes("AMI_SANDBOX_TMPDIR='/var/sbx-tmp'"),
      `expected AMI_SANDBOX_TMPDIR exported, got: ${out}`,
    );
  });

  it('keeps pinning TMPDIR/TMP/TEMP (no regression)', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: '/var/sbx-tmp' }));
    assert.ok(out.includes("TMPDIR='/var/sbx-tmp' TMP='/var/sbx-tmp' TEMP='/var/sbx-tmp'"));
    assert.ok(out.includes("mkdir -p '/var/sbx-tmp'"));
  });

  it('shell-escapes a scratch dir containing a single quote across every var', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: "/t/a'b" }));
    // Each export must use the same escaped form; TMPPREFIX appends /zsh after it.
    assert.ok(out.includes("TMPPREFIX='/t/a'\\''b/zsh'"), out);
    assert.ok(out.includes("AMI_SANDBOX_TMPDIR='/t/a'\\''b'"), out);
  });

  it('end-to-end: wrapWithSandbox threads TMPPREFIX into the wrapped command', () => {
    withPlatform('darwin', () => {
      // On non-linux the resource-limits fallback still applies the scratch-dir
      // env (no mount namespace needed), so the wiring is observable here too.
      const wrapped = wrapWithSandbox('echo hi', '/work', { sandboxTmpDir: '/var/sbx-tmp' });
      assert.ok(wrapped.includes("TMPPREFIX='/var/sbx-tmp/zsh'"), wrapped);
      assert.ok(wrapped.includes("AMI_SANDBOX_TMPDIR='/var/sbx-tmp'"), wrapped);
    });
  });
});
