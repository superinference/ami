import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildReadDenials,
  buildProxyEnv,
  buildSandboxTmpDir,
  checkSandboxDependencies,
  hostSandboxConfigFromPolicy,
  wrapWithUnshare,
  wrapWithSandbox,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';
import type { SandboxPolicy } from '../src/types';

// ---------------------------------------------------------------------------
// UNIT U045 (fs/net carve-outs) — additional host-sandbox parity knobs:
//   allowReadPaths  — allowRead re-opens a path inside a denyRead region
//                     (Claude SandboxFilesystemConfig.allowRead precedence).
//   httpProxyPort / socksProxyPort — proxy-routed network isolation env
//                     (Claude SandboxNetworkConfig.http/socksProxyPort).
//   sandboxTmpDir   — pin TMPDIR/TMP/TEMP into a contained scratch dir
//                     (Claude shell-provider sandboxTmpDir).
//   checkSandboxDependencies glob warning — fs path restriction with glob
//                     metacharacters silently no-ops under bind mounts
//                     (Claude "glob pattern unsupported" restriction warning).
//   hostSandboxConfigFromPolicy — the single SandboxPolicy → host SandboxConfig
//                     translation so the host backend honors the same carve-outs
//                     the container backend already does.
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
// allowReadPaths — allowRead wins over denyRead for matching paths
// ---------------------------------------------------------------------------
describe('buildReadDenials — allowRead precedence over denyRead', () => {
  it('denies every denyReadPaths entry when no allowReadPaths are set', () => {
    const out = buildReadDenials(baseCfg({ denyReadPaths: ['/secret/a', '/secret/b'] }));
    assert.ok(out.includes("mount --bind /dev/null '/secret/a'"));
    assert.ok(out.includes("mount --bind /dev/null '/secret/b'"));
  });

  it('re-allows an exact denied path listed in allowReadPaths', () => {
    const out = buildReadDenials(
      baseCfg({ denyReadPaths: ['/secret/a', '/secret/b'], allowReadPaths: ['/secret/a'] }),
    );
    assert.ok(!out.includes("'/secret/a'"), 'exact allow-read path is NOT denied');
    assert.ok(out.includes("mount --bind /dev/null '/secret/b'"), 'other path still denied');
  });

  it('re-allows a denied path NESTED under an allowReadPaths region', () => {
    const out = buildReadDenials(
      baseCfg({ denyReadPaths: ['/data/project/keep.txt', '/data/other.txt'], allowReadPaths: ['/data/project'] }),
    );
    assert.ok(!out.includes('keep.txt'), 'nested path inside allowed region re-opened');
    assert.ok(out.includes("mount --bind /dev/null '/data/other.txt'"), 'sibling outside region still denied');
  });

  it('does NOT re-allow a sibling with a shared name prefix (segment-aware)', () => {
    const out = buildReadDenials(
      baseCfg({ denyReadPaths: ['/data-x/secret'], allowReadPaths: ['/data'] }),
    );
    assert.ok(out.includes("mount --bind /dev/null '/data-x/secret'"), '/data-x is not under /data');
  });

  it('returns empty string when every denied path is re-allowed', () => {
    assert.equal(
      buildReadDenials(baseCfg({ denyReadPaths: ['/s/a'], allowReadPaths: ['/s'] })),
      '',
    );
  });

  it('ignores blank allowReadPaths entries', () => {
    const out = buildReadDenials(baseCfg({ denyReadPaths: ['/s/a'], allowReadPaths: ['   ', ''] }));
    assert.ok(out.includes("mount --bind /dev/null '/s/a'"));
  });
});

// ---------------------------------------------------------------------------
// buildProxyEnv — http/socks proxy routing
// ---------------------------------------------------------------------------
describe('buildProxyEnv — proxy-routed network isolation', () => {
  it('is empty when no proxy ports are set', () => {
    assert.equal(buildProxyEnv(baseCfg()), '');
  });

  it('exports http/https proxy vars for a valid httpProxyPort', () => {
    const out = buildProxyEnv(baseCfg({ httpProxyPort: 8080 }));
    assert.ok(out.includes("export http_proxy='http://127.0.0.1:8080'"));
    assert.ok(out.includes("https_proxy='http://127.0.0.1:8080'"));
    assert.ok(out.includes("HTTP_PROXY='http://127.0.0.1:8080'"));
    assert.ok(out.includes("HTTPS_PROXY='http://127.0.0.1:8080'"));
  });

  it('exports all_proxy for a valid socksProxyPort', () => {
    const out = buildProxyEnv(baseCfg({ socksProxyPort: 1080 }));
    assert.ok(out.includes("export all_proxy='socks5://127.0.0.1:1080'"));
    assert.ok(out.includes("ALL_PROXY='socks5://127.0.0.1:1080'"));
  });

  it('emits both when both ports are set', () => {
    const out = buildProxyEnv(baseCfg({ httpProxyPort: 3128, socksProxyPort: 1080 }));
    assert.ok(out.includes('http://127.0.0.1:3128'));
    assert.ok(out.includes('socks5://127.0.0.1:1080'));
  });

  it('rejects out-of-range, zero, and non-integer ports (no-op)', () => {
    assert.equal(buildProxyEnv(baseCfg({ httpProxyPort: 0 })), '');
    assert.equal(buildProxyEnv(baseCfg({ httpProxyPort: 70000 })), '');
    assert.equal(buildProxyEnv(baseCfg({ httpProxyPort: 8080.5 })), '');
    assert.equal(buildProxyEnv(baseCfg({ socksProxyPort: -1 })), '');
    // @ts-expect-error — defensive: a non-number must not emit anything.
    assert.equal(buildProxyEnv(baseCfg({ httpProxyPort: '8080' })), '');
  });
});

// ---------------------------------------------------------------------------
// buildSandboxTmpDir — contained scratch dir
// ---------------------------------------------------------------------------
describe('buildSandboxTmpDir — sandbox-scoped TMPDIR', () => {
  it('is empty when unset', () => {
    assert.equal(buildSandboxTmpDir(baseCfg()), '');
  });

  it('creates and pins TMPDIR/TMP/TEMP', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: '/var/sbx-tmp' }));
    assert.ok(out.includes("mkdir -p '/var/sbx-tmp'"));
    assert.ok(out.includes("export TMPDIR='/var/sbx-tmp' TMP='/var/sbx-tmp' TEMP='/var/sbx-tmp'"));
  });

  it('escapes single quotes in the path', () => {
    const out = buildSandboxTmpDir(baseCfg({ sandboxTmpDir: "/t/a'b" }));
    assert.ok(out.includes("'/t/a'\\''b'"));
  });
});

// ---------------------------------------------------------------------------
// checkSandboxDependencies — glob-pattern restriction warning
// ---------------------------------------------------------------------------
describe('checkSandboxDependencies — glob-pattern warning', () => {
  it('warns when a filesystem restriction path contains glob metacharacters (linux)', () => {
    withPlatform('linux', () => {
      const { warnings } = checkSandboxDependencies({ denyReadPaths: ['/home/*/.ssh'] });
      const glob = warnings.find(w => w.includes('glob metacharacters'));
      assert.ok(glob, 'glob warning present');
      assert.ok(glob!.includes('/home/*/.ssh'));
    });
  });

  it('collects glob paths from every fs list, deduped', () => {
    withPlatform('linux', () => {
      const { warnings } = checkSandboxDependencies({
        readOnlyPaths: ['/a/*'],
        writablePaths: ['/b/?'],
        denyWritePaths: ['/c/[x]'],
        allowReadPaths: ['/d/{x,y}'],
        denyReadPaths: ['/a/*'], // dup of readOnlyPaths entry
      });
      const glob = warnings.find(w => w.includes('glob metacharacters'))!;
      assert.ok(glob.includes('/a/*') && glob.includes('/b/?') && glob.includes('/c/[x]') && glob.includes('/d/{x,y}'));
      // deduped: /a/* appears once
      assert.equal(glob.split('/a/*').length - 1, 1);
    });
  });

  it('does not warn for literal absolute paths', () => {
    withPlatform('linux', () => {
      const { warnings } = checkSandboxDependencies({ denyReadPaths: ['/home/u/.ssh/id_rsa'] });
      assert.ok(!warnings.some(w => w.includes('glob metacharacters')));
    });
  });

  it('does not emit the glob warning on non-linux (bind mounts not used)', () => {
    withPlatform('darwin', () => {
      const { warnings } = checkSandboxDependencies({ denyReadPaths: ['/home/*/.ssh'] });
      assert.ok(!warnings.some(w => w.includes('glob metacharacters')));
    });
  });
});

// ---------------------------------------------------------------------------
// wrapWithUnshare / wrapWithSandbox — end-to-end threading
// ---------------------------------------------------------------------------
describe('wrapWithUnshare — new knobs are emitted in the wrapped command', () => {
  it('emits tmpdir + proxy env and honors allowRead precedence', () => {
    const out = wrapWithUnshare(
      'echo hi',
      '/home/u/proj',
      baseCfg({
        sandboxTmpDir: '/sbx/tmp',
        httpProxyPort: 8080,
        denyReadPaths: ['/sec/keep', '/sec/drop'],
        allowReadPaths: ['/sec/keep'],
      }),
    );
    assert.ok(out.includes("export TMPDIR='/sbx/tmp'"), 'TMPDIR exported');
    assert.ok(out.includes("http_proxy='http://127.0.0.1:8080'"), 'proxy exported');
    assert.ok(out.includes("mount --bind /dev/null '/sec/drop'"), 'non-allowed denied path bound');
    assert.ok(!out.includes("'/sec/keep'"), 're-allowed path not bound');
    assert.ok(out.includes('cd /home/u/proj && echo hi'), 'command still runs after env setup');
  });
});

describe('wrapWithSandbox — resource-limits fallback also sets tmpdir + proxy', () => {
  it('emits tmpdir + proxy env on a non-linux host', () => {
    withPlatform('darwin', () => {
      const out = wrapWithSandbox('echo hi', '/home/u/proj', {
        sandboxTmpDir: '/sbx/tmp',
        socksProxyPort: 1080,
      });
      assert.ok(out.includes("export TMPDIR='/sbx/tmp'"));
      assert.ok(out.includes("all_proxy='socks5://127.0.0.1:1080'"));
      assert.ok(out.includes('echo hi'));
    });
  });
});

// ---------------------------------------------------------------------------
// hostSandboxConfigFromPolicy — SandboxPolicy → host SandboxConfig mapping
// ---------------------------------------------------------------------------
describe('hostSandboxConfigFromPolicy', () => {
  it('returns {} for an undefined policy', () => {
    assert.deepEqual(hostSandboxConfigFromPolicy(undefined), {});
  });

  it('returns {} when no carve-outs are configured', () => {
    assert.deepEqual(hostSandboxConfigFromPolicy({ mode: 'off' }), {});
  });

  it('maps every host-enforceable network + filesystem field', () => {
    const policy: SandboxPolicy = {
      mode: 'container',
      network: {
        allowedHosts: ['a.com'],
        deniedHosts: ['b.com'],
        allowUnixSockets: ['/run/x.sock'],
        allowAllUnixSockets: false,
        httpProxyPort: 8080,
        socksProxyPort: 1080,
      },
      filesystem: {
        denyRead: ['/sec'],
        denyWrite: ['/ro'],
        allowRead: ['/sec/ok'],
      },
    };
    assert.deepEqual(hostSandboxConfigFromPolicy(policy), {
      allowedDomains: ['a.com'],
      deniedDomains: ['b.com'],
      allowedUnixSockets: ['/run/x.sock'],
      allowUnixSockets: false,
      httpProxyPort: 8080,
      socksProxyPort: 1080,
      denyReadPaths: ['/sec'],
      denyWritePaths: ['/ro'],
      allowReadPaths: ['/sec/ok'],
    });
  });

  it('omits unset fields (partial policy)', () => {
    assert.deepEqual(
      hostSandboxConfigFromPolicy({ mode: 'container', network: { deniedHosts: ['x.com'] } }),
      { deniedDomains: ['x.com'] },
    );
  });
});
