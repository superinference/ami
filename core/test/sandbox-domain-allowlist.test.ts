import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildNetworkAllowlist,
  wrapWithUnshare,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';

/** Minimal valid config for wrapWithUnshare tests. */
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

describe('buildNetworkAllowlist', () => {
  it('returns empty string for empty domain array', () => {
    assert.equal(buildNetworkAllowlist([]), '');
  });

  it('includes loopback allow rule', () => {
    const result = buildNetworkAllowlist(['example.com']);
    assert.ok(result.includes('iptables -A OUTPUT -o lo -j ACCEPT'),
      'must allow loopback traffic');
  });

  it('includes DNS allow rules (UDP and TCP port 53)', () => {
    const result = buildNetworkAllowlist(['example.com']);
    assert.ok(result.includes('iptables -A OUTPUT -p udp --dport 53 -j ACCEPT'),
      'must allow DNS over UDP');
    assert.ok(result.includes('iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT'),
      'must allow DNS over TCP');
  });

  it('generates iptables rules for each domain', () => {
    const result = buildNetworkAllowlist(['api.example.com', 'cdn.example.org']);
    assert.ok(result.includes("getent ahosts 'api.example.com'"),
      'must resolve api.example.com');
    assert.ok(result.includes("getent ahosts 'cdn.example.org'"),
      'must resolve cdn.example.org');
    // Each domain gets an iptables -A OUTPUT -d $ip -j ACCEPT
    const acceptMatches = result.match(/iptables -A OUTPUT -d \$ip -j ACCEPT/g);
    assert.ok(acceptMatches, 'must have per-IP accept rules');
    assert.equal(acceptMatches!.length, 2, 'one accept rule per domain');
  });

  it('ends with default DROP rule', () => {
    const result = buildNetworkAllowlist(['example.com']);
    assert.ok(result.includes('iptables -A OUTPUT -j DROP'),
      'must have default deny at the end');
    // DROP must be the LAST iptables rule
    const dropPos = result.lastIndexOf('iptables -A OUTPUT -j DROP');
    const lastAcceptPos = result.lastIndexOf('iptables -A OUTPUT -d');
    assert.ok(dropPos > lastAcceptPos, 'DROP must come after all ACCEPT rules');
  });

  it('escapes single quotes in domain names', () => {
    const result = buildNetworkAllowlist(["evil'domain.com"]);
    assert.ok(result.includes("'\\''"), 'single quotes must be escaped');
  });

  it('maintains correct rule order: loopback -> DNS -> domains -> DROP', () => {
    const result = buildNetworkAllowlist(['example.com']);
    const loPos = result.indexOf('-o lo -j ACCEPT');
    const dnsUdpPos = result.indexOf('--dport 53 -j ACCEPT');
    const domainPos = result.indexOf("getent ahosts 'example.com'");
    const dropPos = result.indexOf('-j DROP');

    assert.ok(loPos >= 0 && dnsUdpPos >= 0 && domainPos >= 0 && dropPos >= 0,
      'all rule types must be present');
    assert.ok(loPos < dnsUdpPos, 'loopback must come before DNS');
    assert.ok(dnsUdpPos < domainPos, 'DNS must come before domain rules');
    assert.ok(domainPos < dropPos, 'domain rules must come before DROP');
  });
});

describe('wrapWithUnshare with allowedDomains', () => {
  it('includes network allowlist rules when allowNetwork is false and domains are set', () => {
    const cfg = baseCfg({ allowedDomains: ['api.openai.com'] });
    const wrapped = wrapWithUnshare('curl https://api.openai.com', '/tmp', cfg);

    assert.ok(wrapped.includes('--net'), 'must use --net flag for network namespace');
    assert.ok(wrapped.includes('iptables -A OUTPUT -o lo -j ACCEPT'),
      'must include loopback allow');
    assert.ok(wrapped.includes("getent ahosts 'api.openai.com'"),
      'must resolve the allowed domain');
    assert.ok(wrapped.includes('iptables -A OUTPUT -j DROP'),
      'must include default DROP');
    assert.ok(wrapped.includes('curl https://api.openai.com'),
      'must include the original command');
  });

  it('does not include network rules when allowNetwork is true', () => {
    const cfg = baseCfg({
      allowNetwork: true,
      allowedDomains: ['api.openai.com'],
    });
    const wrapped = wrapWithUnshare('curl https://api.openai.com', '/tmp', cfg);
    assert.ok(!wrapped.includes('iptables'), 'no iptables when network is fully allowed');
    assert.ok(!wrapped.includes('--net'), 'no --net when network is allowed');
  });

  it('does not include network rules when allowedDomains is empty', () => {
    const cfg = baseCfg({ allowedDomains: [] });
    const wrapped = wrapWithUnshare('curl https://example.com', '/tmp', cfg);
    assert.ok(!wrapped.includes('iptables'), 'no iptables for empty allowedDomains');
    assert.ok(wrapped.includes('--net'), '--net still present for blanket block');
  });

  it('does not include network rules when allowedDomains is undefined', () => {
    const cfg = baseCfg();
    const wrapped = wrapWithUnshare('curl https://example.com', '/tmp', cfg);
    assert.ok(!wrapped.includes('iptables'), 'no iptables when allowedDomains undefined');
  });

  it('network rules appear after mounts and credential scrub but before cd', () => {
    const cfg = baseCfg({
      readOnlyPaths: ['/usr'],
      hiddenEnvVars: ['TOKEN'],
      allowedDomains: ['api.example.com'],
    });
    const wrapped = wrapWithUnshare('echo test', '/home/user', cfg);

    const roMountPos = wrapped.indexOf('mount -o remount,ro');
    const unsetPos = wrapped.indexOf('unset TOKEN');
    const iptablesPos = wrapped.indexOf('iptables');
    const cdPos = wrapped.indexOf('cd /home/user');

    assert.ok(roMountPos >= 0 && unsetPos >= 0 && iptablesPos >= 0 && cdPos >= 0,
      'all sections must be present');
    assert.ok(roMountPos < unsetPos, 'mounts before credential scrub');
    assert.ok(unsetPos < iptablesPos, 'credential scrub before network rules');
    assert.ok(iptablesPos < cdPos, 'network rules before cd');
  });

  it('supports multiple allowed domains', () => {
    const cfg = baseCfg({
      allowedDomains: ['api.example.com', 'cdn.example.com', 'auth.example.com'],
    });
    const wrapped = wrapWithUnshare('wget https://api.example.com/data', '/tmp', cfg);

    assert.ok(wrapped.includes("getent ahosts 'api.example.com'"));
    assert.ok(wrapped.includes("getent ahosts 'cdn.example.com'"));
    assert.ok(wrapped.includes("getent ahosts 'auth.example.com'"));
  });
});
