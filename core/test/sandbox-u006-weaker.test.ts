import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isSandboxPlatformEnabled,
  describeSandboxSecurityTradeoffs,
  getSandboxStatus,
  hostSandboxConfigFromPolicy,
  buildLoopbackSetup,
  buildSocketDenial,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { SandboxPolicy, ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// UNIT U006 — SandboxPolicy security-relaxation parity knobs + per-platform
// gating + explicit tradeoff warnings:
//   enabledPlatforms            — gate the sandbox (container+host) per platform
//                                 (Claude sandbox-adapter enabledPlatforms).
//   allowUnsandboxedCommands    — when false, the dangerouslyDisableSandbox
//                                 escape hatch is COMPLETELY ignored.
//   failIfUnavailable /         — hard-gate: refuse to run UNsandboxed when the
//   allowUnsandboxedFallback      host sandbox cannot start.
//   network.allowLocalBinding   — bring loopback UP so local binding works
//                                 without opening outbound network.
//   enableWeakerNestedSandbox   — drop --map-root-user for nested userns.
//   enableWeakerNetworkIsolation— leave default-denied sockets reachable.
//   describeSandboxSecurityTradeoffs — blunt per-option "Reduces security" text.
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

// ---------------------------------------------------------------------------
// isSandboxPlatformEnabled — undefined=all, []=none, else membership
// ---------------------------------------------------------------------------
describe('isSandboxPlatformEnabled (sandbox.enabledPlatforms)', () => {
  it('undefined → enabled on every platform (default)', () => {
    assert.equal(isSandboxPlatformEnabled(undefined, 'linux'), true);
    assert.equal(isSandboxPlatformEnabled(undefined, 'darwin'), true);
    assert.equal(isSandboxPlatformEnabled(undefined, 'win32'), true);
  });

  it('empty array → enabled on NO platform', () => {
    assert.equal(isSandboxPlatformEnabled([], 'linux'), false);
    assert.equal(isSandboxPlatformEnabled([], 'darwin'), false);
  });

  it('non-empty array → membership test', () => {
    assert.equal(isSandboxPlatformEnabled(['linux'], 'linux'), true);
    assert.equal(isSandboxPlatformEnabled(['linux'], 'darwin'), false);
    assert.equal(isSandboxPlatformEnabled(['darwin', 'linux'], 'darwin'), true);
  });

  it('defaults the platform arg to process.platform', () => {
    withPlatform('linux', () => {
      assert.equal(isSandboxPlatformEnabled(['linux']), true);
      assert.equal(isSandboxPlatformEnabled(['darwin']), false);
    });
  });
});

// ---------------------------------------------------------------------------
// describeSandboxSecurityTradeoffs — blunt wording per enabled weakening knob
// ---------------------------------------------------------------------------
describe('describeSandboxSecurityTradeoffs', () => {
  it('returns [] for undefined / mode:off / no weakening knobs', () => {
    assert.deepEqual(describeSandboxSecurityTradeoffs(undefined), []);
    assert.deepEqual(describeSandboxSecurityTradeoffs({ mode: 'off', allowNetwork: true, enableWeakerNestedSandbox: true }), []);
    assert.deepEqual(describeSandboxSecurityTradeoffs({ mode: 'container' }), []);
  });

  it('flags allowNetwork', () => {
    const out = describeSandboxSecurityTradeoffs({ mode: 'container', allowNetwork: true });
    assert.equal(out.length, 1);
    assert.equal(out[0].option, 'allowNetwork');
    assert.match(out[0].warning, /Reduces security/);
  });

  it('flags enableWeakerNestedSandbox and enableWeakerNetworkIsolation', () => {
    const out = describeSandboxSecurityTradeoffs({
      mode: 'container',
      enableWeakerNestedSandbox: true,
      enableWeakerNetworkIsolation: true,
    });
    const options = out.map((o) => o.option);
    assert.ok(options.includes('enableWeakerNestedSandbox'));
    assert.ok(options.includes('enableWeakerNetworkIsolation'));
    assert.ok(out.every((o) => /Reduces security/.test(o.warning)));
  });

  it('flags network.allowAllUnixSockets and allowUnsandboxedFallback', () => {
    const out = describeSandboxSecurityTradeoffs({
      mode: 'container',
      network: { allowAllUnixSockets: true },
      allowUnsandboxedFallback: true,
    });
    const options = out.map((o) => o.option);
    assert.ok(options.includes('network.allowAllUnixSockets'));
    assert.ok(options.includes('allowUnsandboxedFallback'));
  });
});

// ---------------------------------------------------------------------------
// getSandboxStatus folds the tradeoff warnings in (the /sandbox surface)
// ---------------------------------------------------------------------------
describe('getSandboxStatus — folds in security-tradeoff warnings', () => {
  it('adds a warning naming each enabled weakening knob', () => {
    const status = getSandboxStatus(undefined, {
      mode: 'container',
      enableWeakerNetworkIsolation: true,
    });
    assert.ok(status.warnings.some((w) => w.includes('enableWeakerNetworkIsolation')));
    assert.ok(status.warnings.some((w) => /Reduces security/.test(w)));
  });

  it('adds nothing extra when no weakening knob is set', () => {
    const withPolicy = getSandboxStatus(undefined, { mode: 'container' });
    const withoutPolicy = getSandboxStatus(undefined);
    assert.deepEqual(withPolicy.warnings, withoutPolicy.warnings);
  });
});

// ---------------------------------------------------------------------------
// hostSandboxConfigFromPolicy maps the new fields onto the host SandboxConfig
// ---------------------------------------------------------------------------
describe('hostSandboxConfigFromPolicy — new knob mapping', () => {
  it('maps network.allowLocalBinding, enableWeakerNestedSandbox, enableWeakerNetworkIsolation', () => {
    const policy: SandboxPolicy = {
      mode: 'off',
      network: { allowLocalBinding: true },
      enableWeakerNestedSandbox: true,
      enableWeakerNetworkIsolation: true,
    };
    const cfg = hostSandboxConfigFromPolicy(policy);
    assert.equal(cfg.allowLocalBinding, true);
    assert.equal(cfg.enableWeakerNestedSandbox, true);
    assert.equal(cfg.enableWeakerNetworkIsolation, true);
  });

  it('omits the new fields when unset (no behavior change)', () => {
    const cfg = hostSandboxConfigFromPolicy({ mode: 'off' });
    assert.equal('allowLocalBinding' in cfg, false);
    assert.equal('enableWeakerNestedSandbox' in cfg, false);
    assert.equal('enableWeakerNetworkIsolation' in cfg, false);
  });
});

// ---------------------------------------------------------------------------
// buildLoopbackSetup — bring lo up only when allowLocalBinding && !allowNetwork
// ---------------------------------------------------------------------------
describe('buildLoopbackSetup (network.allowLocalBinding)', () => {
  it('brings loopback UP inside the netns when local binding is allowed', () => {
    assert.equal(buildLoopbackSetup(baseCfg({ allowLocalBinding: true })), 'ip link set lo up 2>/dev/null; ');
  });

  it('is a no-op when allowLocalBinding is unset', () => {
    assert.equal(buildLoopbackSetup(baseCfg()), '');
  });

  it('is a no-op when outbound network is on (loopback already up)', () => {
    assert.equal(buildLoopbackSetup(baseCfg({ allowLocalBinding: true, allowNetwork: true })), '');
  });
});

// ---------------------------------------------------------------------------
// buildSocketDenial — enableWeakerNetworkIsolation leaves sockets reachable
// ---------------------------------------------------------------------------
describe('buildSocketDenial (enableWeakerNetworkIsolation)', () => {
  it('denies default sockets by default', () => {
    const out = buildSocketDenial(baseCfg());
    assert.match(out, /mount --bind \/dev\/null/);
  });

  it('leaves ALL sockets reachable when enableWeakerNetworkIsolation is set', () => {
    assert.equal(buildSocketDenial(baseCfg({ enableWeakerNetworkIsolation: true })), '');
  });
});

// ---------------------------------------------------------------------------
// wrapWithUnshare — enableWeakerNestedSandbox drops --map-root-user; loopback up
// ---------------------------------------------------------------------------
describe('wrapWithUnshare — nested-sandbox + local-binding flags', () => {
  it('includes --map-root-user by default', () => {
    const wrapped = wrapWithUnshare('echo hi', '/tmp', baseCfg());
    assert.match(wrapped, /unshare --map-root-user /);
  });

  it('omits --map-root-user when enableWeakerNestedSandbox is set', () => {
    const wrapped = wrapWithUnshare('echo hi', '/tmp', baseCfg({ enableWeakerNestedSandbox: true }));
    assert.doesNotMatch(wrapped, /--map-root-user/);
    assert.match(wrapped, /^unshare --mount --pid --fork /);
  });

  it('brings loopback up in the command when allowLocalBinding is set', () => {
    const wrapped = wrapWithUnshare('echo hi', '/tmp', baseCfg({ allowLocalBinding: true }));
    assert.match(wrapped, /ip link set lo up/);
  });
});

// ---------------------------------------------------------------------------
// INTEGRATION — bashTool.execute enforcement wiring. Capture the exact command
// handed to the shell via the _bashHooks.execCommand seam (same technique as
// the M11 sandboxExcludedCommands test).
// ---------------------------------------------------------------------------
describe('bashTool.execute — enabledPlatforms / allowUnsandboxedCommands enforcement', () => {
  const TRIGGER = 'curl https://example.com';

  function baseCtx(overrides: Partial<ToolContext> = {}): ToolContext {
    return {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      ...overrides,
    } as ToolContext;
  }

  async function captureExecuted(ctx: ToolContext, input: Record<string, unknown> = {}): Promise<string> {
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    try {
      const result = await bashTool.execute({ command: TRIGGER, ...input }, ctx);
      assert.equal(result.isError, false, `execute should succeed: ${JSON.stringify(result)}`);
      return captured;
    } finally {
      mock.restoreAll();
    }
  }

  afterEach(() => mock.restoreAll());

  it('sanity: the trigger is sandboxed by default (no policy)', async () => {
    const executed = await captureExecuted(baseCtx());
    assert.match(executed, /unshare|ulimit/, `expected a sandbox wrapper, got: ${executed}`);
  });

  it('enabledPlatforms excluding this platform runs the command UNWRAPPED', async () => {
    // ['__none__'] can never match process.platform → sandbox disabled → raw run.
    const executed = await captureExecuted(baseCtx({ sandbox: { mode: 'off', enabledPlatforms: ['__none__'] } }));
    assert.equal(executed, TRIGGER, `platform-gated-off sandbox must run raw, got: ${executed}`);
  });

  it('allowUnsandboxedCommands:false IGNORES dangerouslyDisableSandbox (stays sandboxed)', async () => {
    // Discriminator: without the policy lock, dangerouslyDisableSandbox:true runs raw.
    const executed = await captureExecuted(
      baseCtx({ sandbox: { mode: 'off', allowUnsandboxedCommands: false } }),
      { dangerouslyDisableSandbox: true },
    );
    assert.notEqual(executed, TRIGGER, 'the escape hatch must be ignored when the policy locks it');
    assert.match(executed, /unshare|ulimit/, `expected a sandbox wrapper, got: ${executed}`);
  });

  it('dangerouslyDisableSandbox is honored when the policy does NOT lock it (regression lock)', async () => {
    const executed = await captureExecuted(
      baseCtx({ sandbox: { mode: 'off' } }),
      { dangerouslyDisableSandbox: true },
    );
    assert.equal(executed, TRIGGER, 'the escape hatch must run raw when unlocked');
  });
});

describe('bashTool.execute — failIfUnavailable / allowUnsandboxedFallback hard gate', () => {
  const TRIGGER = 'curl https://example.com';

  afterEach(() => mock.restoreAll());

  function ctx(sandbox: SandboxPolicy): ToolContext {
    return { cwd: process.cwd(), abortSignal: new AbortController().signal, sandbox } as ToolContext;
  }

  it('refuses to run UNsandboxed when the host sandbox cannot start and fallback is disallowed', async () => {
    // Force the host sandbox UNavailable by pretending to be a non-Linux platform
    // (checkSandboxDependencies errors → validateSandboxEnabled invalid).
    const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
    Object.defineProperty(process, 'platform', { value: 'darwin', configurable: true });
    const spy = mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    try {
      const result = await bashTool.execute(
        { command: TRIGGER },
        ctx({ mode: 'off', allowUnsandboxedFallback: false }),
      );
      assert.equal(result.isError, true, 'must refuse when the sandbox is unavailable');
      assert.match(result.output, /SANDBOX REQUIRED/);
      assert.equal(spy.mock.callCount(), 0, 'the command must not reach the shell');
    } finally {
      Object.defineProperty(process, 'platform', orig);
      mock.restoreAll();
    }
  });

  it('allows the (degraded) run when fallback is permitted (default)', async () => {
    const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
    Object.defineProperty(process, 'platform', { value: 'darwin', configurable: true });
    let captured = '';
    mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
      captured = cmd;
      return { stdout: '', stderr: '', exitCode: 0 };
    });
    try {
      const result = await bashTool.execute({ command: TRIGGER }, ctx({ mode: 'off' }));
      assert.equal(result.isError, false);
      assert.match(captured, /ulimit/, 'degrades to ulimits-only on a non-Linux host');
    } finally {
      Object.defineProperty(process, 'platform', orig);
      mock.restoreAll();
    }
  });
});
