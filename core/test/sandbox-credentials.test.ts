import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildCredentialScrub,
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

describe('buildCredentialScrub', () => {
  it('generates unset for hiddenEnvVars', () => {
    const cfg = baseCfg({ hiddenEnvVars: ['AWS_SECRET_ACCESS_KEY', 'OPENAI_API_KEY'] });
    const result = buildCredentialScrub(cfg);
    assert.equal(result, 'unset AWS_SECRET_ACCESS_KEY OPENAI_API_KEY; ');
  });

  it('generates mount --bind commands for hiddenFiles', () => {
    const cfg = baseCfg({ hiddenFiles: ['/home/user/.aws/credentials', '/home/user/.ssh/id_rsa'] });
    const result = buildCredentialScrub(cfg);
    assert.ok(result.includes("mount --bind /dev/null '/home/user/.aws/credentials' 2>/dev/null; "));
    assert.ok(result.includes("mount --bind /dev/null '/home/user/.ssh/id_rsa' 2>/dev/null; "));
  });

  it('generates both unset and mount commands when both are set', () => {
    const cfg = baseCfg({
      hiddenEnvVars: ['SECRET_TOKEN'],
      hiddenFiles: ['/etc/shadow'],
    });
    const result = buildCredentialScrub(cfg);
    assert.ok(result.includes('unset SECRET_TOKEN; '));
    assert.ok(result.includes("mount --bind /dev/null '/etc/shadow' 2>/dev/null; "));
    // unset comes before mount
    const unsetPos = result.indexOf('unset');
    const mountPos = result.indexOf('mount --bind');
    assert.ok(unsetPos < mountPos, 'unset must appear before mount --bind');
  });

  it('returns empty string for empty arrays', () => {
    const cfg = baseCfg({ hiddenEnvVars: [], hiddenFiles: [] });
    assert.equal(buildCredentialScrub(cfg), '');
  });

  it('returns empty string when fields are undefined', () => {
    const cfg = baseCfg();
    assert.equal(buildCredentialScrub(cfg), '');
  });

  it('escapes single quotes in file paths', () => {
    const cfg = baseCfg({ hiddenFiles: ["/home/user/it's-a-secret"] });
    const result = buildCredentialScrub(cfg);
    assert.ok(result.includes("'\\''"), 'single quotes in paths must be escaped');
    assert.ok(result.includes('mount --bind /dev/null'));
  });
});

describe('wrapWithUnshare includes credential scrub', () => {
  it('inserts unset command into the unshare wrapper', () => {
    const cfg = baseCfg({ hiddenEnvVars: ['API_KEY', 'DB_PASSWORD'] });
    const wrapped = wrapWithUnshare('curl https://example.com', '/tmp', cfg);
    assert.ok(wrapped.includes('unset API_KEY DB_PASSWORD; '),
      'unshare wrapper must include unset for hiddenEnvVars');
    // The command itself must still be present
    assert.ok(wrapped.includes('curl https://example.com'));
    assert.ok(wrapped.startsWith('unshare'));
  });

  it('inserts mount --bind for hiddenFiles into unshare wrapper', () => {
    const cfg = baseCfg({ hiddenFiles: ['/root/.aws/credentials'] });
    const wrapped = wrapWithUnshare('python3 exploit.py', '/tmp', cfg);
    assert.ok(wrapped.includes("mount --bind /dev/null '/root/.aws/credentials'"),
      'unshare wrapper must include mount --bind for hiddenFiles');
    assert.ok(wrapped.includes('python3 exploit.py'));
  });

  it('credential scrub appears after read-only mounts but before cd/command', () => {
    const cfg = baseCfg({
      readOnlyPaths: ['/usr'],
      hiddenEnvVars: ['TOKEN'],
      hiddenFiles: ['/etc/secret'],
    });
    const wrapped = wrapWithUnshare('echo test', '/home/user', cfg);

    // Order: ulimits ... readOnlyMounts ... credentialScrub ... cd ... command
    const roMountPos = wrapped.indexOf('mount -o remount,ro');
    const unsetPos = wrapped.indexOf('unset TOKEN');
    const cdPos = wrapped.indexOf('cd /home/user');
    const cmdPos = wrapped.indexOf('echo test');

    assert.ok(roMountPos >= 0, 'read-only mount must be present');
    assert.ok(unsetPos >= 0, 'unset must be present');
    assert.ok(cdPos >= 0, 'cd must be present');
    assert.ok(cmdPos >= 0, 'command must be present');
    assert.ok(roMountPos < unsetPos, 'read-only mounts must come before credential scrub');
    assert.ok(unsetPos < cdPos, 'credential scrub must come before cd');
    assert.ok(cdPos < cmdPos, 'cd must come before command');
  });

  it('omits credential scrub when no hiddenEnvVars or hiddenFiles', () => {
    const cfg = baseCfg();
    const wrapped = wrapWithUnshare('echo safe', '/tmp', cfg);
    assert.ok(!wrapped.includes('unset '), 'no unset when hiddenEnvVars is not set');
    assert.ok(!/mount --bind \/dev\/null/.test(wrapped),
      'no mount --bind /dev/null when hiddenFiles is not set');
  });
});
