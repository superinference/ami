import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildSocketDenial,
  buildDeniedDomainRules,
  buildReadDenials,
  buildWriteDenials,
  buildNetworkAllowlist,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

/**
 * Parity tests for bash-sandbox additive hardening (unshare backend):
 *  - host denylist (`deniedDomains`) with deny-wins-over-allow ordering
 *  - per-path Unix-socket re-allow (`allowedUnixSockets`)
 *  - filesystem read-deny (`denyReadPaths`)
 *  - filesystem write-deny (`denyWritePaths`)
 *
 * These mirror Claude's SandboxNetworkConfig/SandboxFilesystemConfig knobs while
 * remaining an ADDITIVE superset: every new field defaults to a no-op so the
 * pre-existing wrapping behavior is byte-for-byte unchanged when unset.
 *
 * Each builder is validated as a pure string transform (no live runtime needed).
 */

/** Minimal valid config for wrapWithUnshare / builder tests. */
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

// ---------------------------------------------------------------------------
// Host denylist (deniedDomains) — parity with SandboxNetworkConfig.deniedHosts
// ---------------------------------------------------------------------------
describe('buildDeniedDomainRules (host denylist parity)', () => {
  it('returns empty string for undefined/empty list', () => {
    assert.equal(buildDeniedDomainRules(undefined), '');
    assert.equal(buildDeniedDomainRules([]), '');
  });

  it('emits a DROP rule (via resolved IPs) for each denied domain', () => {
    const rules = buildDeniedDomainRules(['evil.example.com', 'tracker.test']);
    assert.ok(rules.includes("getent ahosts 'evil.example.com'"), 'resolves first domain');
    assert.ok(rules.includes("getent ahosts 'tracker.test'"), 'resolves second domain');
    assert.ok(rules.includes('iptables -A OUTPUT -d $ip -j DROP'), 'drops resolved IPs');
    // Must NOT accidentally ACCEPT a denied host.
    assert.ok(!/-d \$ip -j ACCEPT/.test(rules), 'denied domains are never accepted');
  });

  it('strips trailing FQDN dots before resolving', () => {
    const rules = buildDeniedDomainRules(['evil.example.com...']);
    assert.ok(rules.includes("getent ahosts 'evil.example.com'"), 'trailing dots removed');
    assert.ok(!rules.includes("evil.example.com...'"), 'raw dotted form not used');
  });

  it('escapes single quotes in a denied domain', () => {
    const rules = buildDeniedDomainRules(["bad'host"]);
    assert.ok(rules.includes("bad'\\''host"), 'single quote escaped for the single-quoted shell literal');
  });

  it('DENY WINS: deny DROP rules precede the allowlist ACCEPT rules in the wrapper', () => {
    const wrapped = wrapWithUnshare(
      'curl https://x',
      '/tmp',
      baseCfg({ allowedDomains: ['api.example.com'], deniedDomains: ['api.example.com'] }),
    );
    const denyPos = wrapped.indexOf('-d $ip -j DROP');
    const firstAcceptPos = wrapped.indexOf('iptables -A OUTPUT -o lo -j ACCEPT');
    assert.ok(denyPos >= 0, 'deny rule present in wrapper');
    assert.ok(firstAcceptPos >= 0, 'allowlist accept rules present in wrapper');
    assert.ok(denyPos < firstAcceptPos, 'deny rule appended before any ACCEPT so deny wins over allow');
  });

  it('is a no-op when deniedDomains is unset (byte-identical to allowlist-only)', () => {
    const withUnset = wrapWithUnshare('curl https://x', '/tmp', baseCfg({ allowedDomains: ['api.example.com'] }));
    const manual =
      // same wrapper composed with an explicit empty denylist must match
      wrapWithUnshare('curl https://x', '/tmp', baseCfg({ allowedDomains: ['api.example.com'], deniedDomains: [] }));
    assert.equal(withUnset, manual, 'empty denylist changes nothing');
    assert.ok(withUnset.includes(buildNetworkAllowlist(['api.example.com'])), 'allowlist still intact');
  });
});

// ---------------------------------------------------------------------------
// Per-path Unix-socket re-allow — parity with SandboxNetworkConfig.allowUnixSockets
// ---------------------------------------------------------------------------
describe('buildSocketDenial (per-path socket allow parity)', () => {
  it('denies all default privileged sockets by default', () => {
    const out = buildSocketDenial(baseCfg());
    assert.ok(out.includes("/var/run/docker.sock"), 'docker.sock denied by default');
    assert.ok(out.includes("/run/containerd/containerd.sock"), 'containerd.sock denied by default');
  });

  it('re-allows ONLY the listed socket path, keeping the rest denied', () => {
    const out = buildSocketDenial(baseCfg({ allowedUnixSockets: ['/var/run/docker.sock'] }));
    assert.ok(
      !out.includes("mount --bind /dev/null '/var/run/docker.sock'"),
      'explicitly allowed socket is NOT bound to /dev/null',
    );
    assert.ok(
      out.includes("mount --bind /dev/null '/run/containerd/containerd.sock'"),
      'other default sockets stay denied',
    );
  });

  it('ignores empty/whitespace-only allow entries', () => {
    const out = buildSocketDenial(baseCfg({ allowedUnixSockets: ['', '   '] }));
    assert.ok(out.includes("/var/run/docker.sock"), 'blank entries do not re-allow anything');
  });

  it('the all-or-nothing boolean still fully disables denial (unchanged)', () => {
    assert.equal(buildSocketDenial(baseCfg({ allowUnixSockets: true, allowedUnixSockets: [] })), '');
  });
});

// ---------------------------------------------------------------------------
// Filesystem read-deny — parity with SandboxFilesystemConfig.denyRead
// ---------------------------------------------------------------------------
describe('buildReadDenials (filesystem read-deny parity)', () => {
  it('returns empty string when unset/empty', () => {
    assert.equal(buildReadDenials(baseCfg()), '');
    assert.equal(buildReadDenials(baseCfg({ denyReadPaths: [] })), '');
  });

  it('binds /dev/null over each denied-read path', () => {
    const out = buildReadDenials(baseCfg({ denyReadPaths: ['/home/u/.ssh', '/etc/shadow'] }));
    assert.ok(out.includes("mount --bind /dev/null '/home/u/.ssh'"), 'first path hidden');
    assert.ok(out.includes("mount --bind /dev/null '/etc/shadow'"), 'second path hidden');
    assert.ok(out.includes('2>/dev/null'), 'best-effort (errors swallowed)');
  });

  it('escapes single quotes in a denied-read path', () => {
    const out = buildReadDenials(baseCfg({ denyReadPaths: ["/a'b"] }));
    assert.ok(out.includes("'/a'\\''b'"), 'single quote escaped');
  });

  it('is wired into the unshare wrapper', () => {
    const wrapped = wrapWithUnshare('cat secret', '/tmp', baseCfg({ denyReadPaths: ['/secret'] }));
    assert.ok(wrapped.includes("mount --bind /dev/null '/secret'"), 'read-deny present in wrapper');
  });
});

// ---------------------------------------------------------------------------
// Filesystem write-deny — parity with SandboxFilesystemConfig.denyWrite
// ---------------------------------------------------------------------------
describe('buildWriteDenials (filesystem write-deny parity)', () => {
  it('returns empty string when unset/empty', () => {
    assert.equal(buildWriteDenials(baseCfg()), '');
    assert.equal(buildWriteDenials(baseCfg({ denyWritePaths: [] })), '');
  });

  it('bind-remounts each denied-write path read-only', () => {
    // Path is shell-escaped for embedding in the single-quoted `bash -c` body
    // (same technique as readOnlyPaths), so match loosely on the escaped form.
    const out = buildWriteDenials(baseCfg({ denyWritePaths: ['/opt/data'] }));
    assert.ok(/mount --bind[^;]*\/opt\/data/.test(out), 'bind the path onto itself');
    assert.ok(/mount -o remount,ro,bind[^;]*\/opt\/data/.test(out), 'remount it read-only');
  });

  it('is wired into the unshare wrapper', () => {
    const cfg = baseCfg({ denyWritePaths: ['/opt/data'] });
    const wrapped = wrapWithUnshare('touch /opt/data/x', '/tmp', cfg);
    assert.ok(wrapped.includes(buildWriteDenials(cfg)), 'write-deny segment present verbatim in wrapper');
    assert.ok(/mount -o remount,ro,bind[^;]*\/opt\/data/.test(wrapped), 'remount rule for the path in wrapper');
  });
});

// ---------------------------------------------------------------------------
// Additive guarantee: with every new field unset, the wrapper is unchanged.
// ---------------------------------------------------------------------------
describe('additive superset: new fields default to no-ops', () => {
  it('a plain config produces no deny/read-deny/write-deny/denied-domain artifacts', () => {
    const wrapped = wrapWithUnshare('echo hi', '/tmp', baseCfg());
    assert.ok(!wrapped.includes('-d $ip -j DROP'), 'no host-deny rules');
    // Only the default privileged-socket denial should appear, nothing else new.
    assert.ok(wrapped.includes("/var/run/docker.sock"), 'default socket denial unchanged');
  });
});
