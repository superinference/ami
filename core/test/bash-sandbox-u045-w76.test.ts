import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  wrapWithUnshare,
  wrapWithSandbox,
  buildSandboxRunExpr,
  isSandboxEncodeCommandEnabled,
  isSandboxIndicatorEnabled,
  sandboxBashDisplayName,
  shouldWarnSocketBlocking,
  SOCKET_BLOCK_UNAVAILABLE_WARNING,
  SANDBOX_ENCODE_COMMAND_ENV,
  BASH_SANDBOX_SHOW_INDICATOR_ENV,
  getSandboxStatus,
  resolveSandboxMethod,
  resetSandboxCache,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 (w76) — four additive host-sandbox parity knobs on bash-sandbox.ts:
//   - encoded-command seam (Claude PowerShell -EncodedCommand): base64 the user
//     command so NO nested shell-quoting layer can corrupt `!$?` etc.   [medium]
//   - dependency-aware socket-warning suppression (Claude's
//     showSocketWarning = hasWarnings && !allowAllUnixSockets)             [low]
//   - CLAUDE_CODE_BASH_SANDBOX_SHOW_INDICATOR → SandboxedBash indicator    [low]
// ---------------------------------------------------------------------------

/** Override process.platform for the duration of fn, then restore. */
function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    Object.defineProperty(process, 'platform', orig);
  }
}

/** Run fn with one env var set to `value`, restoring afterwards. */
function withEnv(name: string, value: string | undefined, fn: () => void): void {
  const saved = process.env[name];
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
  try {
    fn();
  } finally {
    if (saved === undefined) delete process.env[name];
    else process.env[name] = saved;
  }
}

const baseCfg: SandboxConfig = {
  allowNetwork: false,
  readOnlyPaths: [],
  writablePaths: [],
  maxMemoryMB: 512,
  maxProcesses: 8192,
  timeout: 30000,
};

afterEach(() => resetSandboxCache());

// ---------------------------------------------------------------------------
// encoded-command seam
// ---------------------------------------------------------------------------
describe('buildSandboxRunExpr (encoded-command seam)', () => {
  it('returns the pre-escaped command unchanged when encoding is off', () => {
    assert.equal(buildSandboxRunExpr('echo hi', "echo hi", false), 'echo hi');
  });

  it('base64-encodes the RAW command and pipes it through base64 -d | bash when on', () => {
    const raw = 'echo "$?" && test ! -f x';
    const expr = buildSandboxRunExpr(raw, 'IGNORED-ESCAPED', true);
    const b64 = Buffer.from(raw, 'utf8').toString('base64');
    assert.equal(expr, `printf %s '${b64}' | base64 -d | bash`);
    // The payload is base64 only — no raw metacharacters survive to be corrupted.
    assert.ok(/^printf %s '[A-Za-z0-9+/=]+' \| base64 -d \| bash$/.test(expr));
    assert.ok(!expr.includes('$?'));
  });

  it('round-trips the exact command bytes through the base64 payload', () => {
    const raw = "weird ! $ ? \\ ' command";
    const expr = buildSandboxRunExpr(raw, 'x', true);
    const m = expr.match(/^printf %s '([A-Za-z0-9+/=]+)'/)!;
    assert.equal(Buffer.from(m[1], 'base64').toString('utf8'), raw);
  });
});

describe('isSandboxEncodeCommandEnabled (env gate)', () => {
  it('is false by default / for falsey values', () => {
    withEnv(SANDBOX_ENCODE_COMMAND_ENV, undefined, () =>
      assert.equal(isSandboxEncodeCommandEnabled(), false));
    for (const v of ['', '0', 'false', 'no', 'off', 'nope']) {
      assert.equal(isSandboxEncodeCommandEnabled({ [SANDBOX_ENCODE_COMMAND_ENV]: v }), false);
    }
  });
  it('is true for truthy values (case/whitespace insensitive)', () => {
    for (const v of ['1', 'true', 'yes', 'on', ' TRUE ', 'On']) {
      assert.equal(isSandboxEncodeCommandEnabled({ [SANDBOX_ENCODE_COMMAND_ENV]: v }), true, v);
    }
  });
});

describe('wrapWithUnshare honors encodeCommand', () => {
  it('wraps the command base64-encoded when encodeCommand is set', () => {
    const raw = 'curl https://example.com && echo "$?"';
    const wrapped = wrapWithUnshare(raw, '/work', { ...baseCfg, encodeCommand: true });
    const b64 = Buffer.from(raw, 'utf8').toString('base64');
    assert.ok(wrapped.includes(`printf %s '${b64}' | base64 -d | bash`));
    // The raw quoting-sensitive command must NOT appear verbatim in the wrapper.
    assert.ok(!wrapped.includes('"$?"'));
  });

  it('wraps the raw single-quote-escaped command when encodeCommand is off', () => {
    const wrapped = wrapWithUnshare('echo hi', '/work', { ...baseCfg });
    assert.ok(wrapped.includes('cd ') && wrapped.includes('&& echo hi'));
    assert.ok(!wrapped.includes('base64 -d'));
  });
});

describe('wrapWithSandbox self-wires the encode env gate (host-dependent)', () => {
  it('encodes the command via the env gate when the unshare backend is in use', () => {
    if (resolveSandboxMethod() !== 'unshare') return; // deterministic only where ns works
    const raw = 'echo encoded_probe_$?';
    const b64 = Buffer.from(raw, 'utf8').toString('base64');
    withEnv(SANDBOX_ENCODE_COMMAND_ENV, '1', () => {
      const wrapped = wrapWithSandbox(raw, '/work', { ...baseCfg });
      assert.ok(wrapped.includes(`printf %s '${b64}' | base64 -d | bash`));
    });
    withEnv(SANDBOX_ENCODE_COMMAND_ENV, undefined, () => {
      const wrapped = wrapWithSandbox(raw, '/work', { ...baseCfg });
      assert.ok(!wrapped.includes('base64 -d'));
    });
  });

  it('an explicit encodeCommand:false is NOT overridden by the env gate', () => {
    if (resolveSandboxMethod() !== 'unshare') return;
    withEnv(SANDBOX_ENCODE_COMMAND_ENV, '1', () => {
      const wrapped = wrapWithSandbox('echo x', '/work', { ...baseCfg, encodeCommand: false });
      assert.ok(!wrapped.includes('base64 -d'));
    });
  });
});

// ---------------------------------------------------------------------------
// dependency-aware socket-warning suppression
// ---------------------------------------------------------------------------
describe('shouldWarnSocketBlocking (parity: hasWarnings && !allowAllUnixSockets)', () => {
  it('warns when socket blocking is unavailable and the user did not opt into all sockets', () => {
    assert.equal(shouldWarnSocketBlocking(false, undefined), true);
    assert.equal(shouldWarnSocketBlocking(false, false), true);
  });
  it('is suppressed by an explicit allowAllUnixSockets opt-in', () => {
    assert.equal(shouldWarnSocketBlocking(false, true), false);
  });
  it('does not warn when socket blocking IS available (full mount namespace)', () => {
    assert.equal(shouldWarnSocketBlocking(true, undefined), false);
    assert.equal(shouldWarnSocketBlocking(true, true), false);
  });
});

describe('getSandboxStatus surfaces/suppresses the socket caveat', () => {
  it('adds the socket caveat on the ulimits fallback (socket blocking unenforceable)', () => {
    withPlatform('darwin', () => {
      const s = getSandboxStatus();
      assert.equal(s.method, 'ulimits');
      assert.ok(s.warnings.includes(SOCKET_BLOCK_UNAVAILABLE_WARNING));
    });
  });

  it('suppresses the socket caveat when policy.network.allowAllUnixSockets is set', () => {
    withPlatform('darwin', () => {
      const s = getSandboxStatus(undefined, { mode: 'container', network: { allowAllUnixSockets: true } });
      assert.ok(!s.warnings.includes(SOCKET_BLOCK_UNAVAILABLE_WARNING));
    });
  });

  it('suppresses the socket caveat when cfg.allowUnixSockets is set', () => {
    withPlatform('darwin', () => {
      const s = getSandboxStatus({ allowUnixSockets: true });
      assert.ok(!s.warnings.includes(SOCKET_BLOCK_UNAVAILABLE_WARNING));
    });
  });

  it('does not add the socket caveat when the full namespace sandbox is in use', () => {
    if (resolveSandboxMethod() !== 'unshare') return;
    const s = getSandboxStatus();
    assert.equal(s.method, 'unshare');
    assert.ok(!s.warnings.includes(SOCKET_BLOCK_UNAVAILABLE_WARNING));
  });
});

// ---------------------------------------------------------------------------
// sandbox indicator knob
// ---------------------------------------------------------------------------
describe('isSandboxIndicatorEnabled / sandboxBashDisplayName (indicator knob)', () => {
  it('env gate is false by default, true for truthy values', () => {
    assert.equal(isSandboxIndicatorEnabled({}), false);
    assert.equal(isSandboxIndicatorEnabled({ [BASH_SANDBOX_SHOW_INDICATOR_ENV]: '0' }), false);
    assert.equal(isSandboxIndicatorEnabled({ [BASH_SANDBOX_SHOW_INDICATOR_ENV]: 'true' }), true);
    assert.equal(isSandboxIndicatorEnabled({ [BASH_SANDBOX_SHOW_INDICATOR_ENV]: ' ON ' }), true);
  });

  it('name is SandboxedBash only when sandboxed AND the knob is on', () => {
    const on = { [BASH_SANDBOX_SHOW_INDICATOR_ENV]: '1' };
    assert.equal(sandboxBashDisplayName(true, on), 'SandboxedBash');
    assert.equal(sandboxBashDisplayName(false, on), 'Bash'); // not sandboxed → plain Bash
    assert.equal(sandboxBashDisplayName(true, {}), 'Bash'); // knob off → plain Bash
    assert.equal(sandboxBashDisplayName(false, {}), 'Bash');
  });
});

// ---------------------------------------------------------------------------
// E2E: the indicator flows end-to-end through the real bash tool.
// `sh -c '...'` is a sandbox trigger, so the command runs sandboxed; with the
// knob on its output is prefixed with the `[SandboxedBash]` indicator.
// ---------------------------------------------------------------------------
describe('bash tool — sandbox indicator (end-to-end)', () => {
  it('prefixes [SandboxedBash] when the knob is on and the command is sandboxed', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const saved = process.env[BASH_SANDBOX_SHOW_INDICATOR_ENV];
    process.env[BASH_SANDBOX_SHOW_INDICATOR_ENV] = '1';
    try {
      const result = await bashTool.execute(
        { command: "sh -c 'echo indicator_probe'" },
        { cwd: '/tmp', abortSignal: new AbortController().signal },
      );
      assert.ok(!result.isError, result.output);
      assert.ok(result.output.startsWith('[SandboxedBash]\n'), result.output);
      assert.ok(result.output.includes('indicator_probe'));
    } finally {
      if (saved === undefined) delete process.env[BASH_SANDBOX_SHOW_INDICATOR_ENV];
      else process.env[BASH_SANDBOX_SHOW_INDICATOR_ENV] = saved;
    }
  });

  it('does NOT prefix the indicator when the knob is off (default)', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const result = await bashTool.execute(
      { command: "sh -c 'echo no_indicator_probe'" },
      { cwd: '/tmp', abortSignal: new AbortController().signal },
    );
    assert.ok(!result.isError, result.output);
    assert.ok(!result.output.includes('[SandboxedBash]'));
    assert.ok(result.output.includes('no_indicator_probe'));
  });
});
