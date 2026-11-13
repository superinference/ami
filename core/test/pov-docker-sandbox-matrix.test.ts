/**
 * POV (round 4): pure arg-building + security-flag blocking for the Docker
 * sandbox backend — NO running daemon required, NO container ever started.
 *
 * Two behavioral surfaces are exercised here:
 *   1. `wrapWithDocker` (tools/docker-sandbox.ts) deterministically builds the
 *      host `docker run …` argv for a command: image (default/override),
 *      project bind-mount, network on/off, resource limits, workdir clamping,
 *      and POSIX single-quote escaping. Expected argv is computed independently
 *      by this test (not by reading source text) and compared for equality.
 *   2. `detectContainerEscape` (tools/bash-security.ts) is the security policy
 *      that BLOCKS isolation-breaking `docker/podman run` flags (--privileged,
 *      --pid=host, --net=host, host-root volume mounts, --cap-add=ALL/SYS_ADMIN,
 *      dangerous --security-opt) while ALLOWING normal runs and read-only
 *      subcommands. These are fresh, distinct cases.
 *
 * Also: `_resetDockerAvailableCache` idempotence via the mocked shell seam.
 * pullImage / live container-run are intentionally NOT touched (need a runtime).
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  isDockerAvailable,
  _resetDockerAvailableCache,
  DEFAULT_DOCKER_IMAGE,
} from '../src/tools/docker-sandbox';
import { detectContainerEscape } from '../src/tools/bash-security';
import { shellHooks } from '../src/utils/shell';

let tmpProject: string;

beforeEach(() => {
  _resetDockerAvailableCache();
  tmpProject = fs.mkdtempSync(path.join(os.tmpdir(), 'si-dockmx-'));
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
  fs.rmSync(tmpProject, { recursive: true, force: true });
});

function cfg(over: Record<string, unknown> = {}) {
  return { image: DEFAULT_DOCKER_IMAGE, projectDir: tmpProject, allowNetwork: false, ...over };
}

describe('DEFAULT_DOCKER_IMAGE', () => {
  it('is the published openshell-ami GHCR image tag', () => {
    assert.equal(DEFAULT_DOCKER_IMAGE, 'ghcr.io/superinference/openshell-ami:latest');
  });
});

describe('wrapWithDocker — exact argv assembly', () => {
  it('produces the full expected docker-run argv for a fully-specified config', () => {
    const p = tmpProject;
    const out = wrapWithDocker('echo hi', p, cfg({
      image: DEFAULT_DOCKER_IMAGE,
      allowNetwork: true,
      maxMemoryMB: 512,
      maxProcesses: 128,
    }));
    const expected = [
      'docker', 'run', '--rm', '-i',
      '--user', '0:0',
      '--entrypoint', 'bash',
      '--network', 'bridge',
      '--tmpfs', '/tmp', '--tmpfs', '/root', '--tmpfs', '/home',
      '-v', `'${p}:${p}'`,
      '-w', `'${p}'`,
      '--memory', '512m',
      '--pids-limit', '128',
      `'${DEFAULT_DOCKER_IMAGE}'`, '-c', `'echo hi'`,
    ].join(' ');
    assert.equal(out, expected);
  });

  it('produces the full expected argv with network OFF and no resource limits', () => {
    const p = tmpProject;
    const out = wrapWithDocker('ls -la', p, cfg({ image: 'alpine:3.20', allowNetwork: false }));
    const expected = [
      'docker', 'run', '--rm', '-i',
      '--user', '0:0',
      '--entrypoint', 'bash',
      '--network', 'none',
      '--tmpfs', '/tmp', '--tmpfs', '/root', '--tmpfs', '/home',
      '-v', `'${p}:${p}'`,
      '-w', `'${p}'`,
      `'alpine:3.20'`, '-c', `'ls -la'`,
    ].join(' ');
    assert.equal(out, expected);
  });
});

describe('wrapWithDocker — image: default vs override', () => {
  it('embeds the DEFAULT_DOCKER_IMAGE verbatim when passed as the image', () => {
    const out = wrapWithDocker('true', tmpProject, cfg({ image: DEFAULT_DOCKER_IMAGE }));
    assert.ok(out.endsWith(`'${DEFAULT_DOCKER_IMAGE}' -c 'true'`), out);
  });

  it('embeds an arbitrary override image verbatim (wrap never forces a default)', () => {
    const out = wrapWithDocker('true', tmpProject, cfg({ image: 'registry.example.com/foo/bar:v9' }));
    assert.ok(out.includes(`'registry.example.com/foo/bar:v9' -c 'true'`), out);
    assert.ok(!out.includes(DEFAULT_DOCKER_IMAGE), 'override image must not inject the default');
  });
});

describe('wrapWithDocker — project bind mount', () => {
  it('mounts only the project directory read-write (identity host:container path)', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(out.includes(`-v '${tmpProject}:${tmpProject}'`), out);
    // nothing else is mounted
    const mounts = out.match(/-v '[^']*'/g) ?? [];
    assert.equal(mounts.length, 1, 'exactly one -v mount');
  });

  it('resolves a relative projectDir to an absolute path before mounting', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ projectDir: '.' }));
    const resolved = path.resolve('.');
    assert.ok(out.includes(`-v '${resolved}:${resolved}'`), out);
  });
});

describe('wrapWithDocker — network toggle', () => {
  it('uses bridge when allowNetwork is true', () => {
    assert.ok(wrapWithDocker('x', tmpProject, cfg({ allowNetwork: true })).includes('--network bridge'));
  });
  it('uses none when allowNetwork is false', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ allowNetwork: false }));
    assert.ok(out.includes('--network none'));
    assert.ok(!out.includes('--network bridge'));
  });
});

describe('wrapWithDocker — resource limits (only when positive)', () => {
  it('adds --memory only (pids omitted) when only maxMemoryMB is positive', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ maxMemoryMB: 256 }));
    assert.ok(out.includes('--memory 256m'));
    assert.ok(!out.includes('--pids-limit'));
  });

  it('adds --pids-limit only (memory omitted) when only maxProcesses is positive', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ maxProcesses: 64 }));
    assert.ok(out.includes('--pids-limit 64'));
    assert.ok(!out.includes('--memory'));
  });

  it('omits both when limits are zero', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ maxMemoryMB: 0, maxProcesses: 0 }));
    assert.ok(!out.includes('--memory'));
    assert.ok(!out.includes('--pids-limit'));
  });

  it('omits both when limits are negative (treated as not-set)', () => {
    const out = wrapWithDocker('x', tmpProject, cfg({ maxMemoryMB: -5, maxProcesses: -1 }));
    assert.ok(!out.includes('--memory'));
    assert.ok(!out.includes('--pids-limit'));
  });

  it('omits both when limits are left undefined', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(!out.includes('--memory'));
    assert.ok(!out.includes('--pids-limit'));
  });
});

describe('wrapWithDocker — workdir clamping', () => {
  it('keeps an in-project subdirectory as the workdir', () => {
    const sub = path.join(tmpProject, 'a', 'b');
    const out = wrapWithDocker('x', sub, cfg());
    assert.ok(out.includes(`-w '${sub}'`), out);
  });

  it('keeps the project root itself as the workdir', () => {
    const out = wrapWithDocker('x', tmpProject, cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`), out);
  });

  it('clamps a cwd entirely outside the project to the project root', () => {
    const out = wrapWithDocker('x', '/var/tmp', cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`), out);
  });

  it('clamps a sibling whose path merely prefixes the project name (no sep boundary)', () => {
    // e.g. project=/tmp/si-dockmx-XXXX, cwd=/tmp/si-dockmx-XXXX-sibling —
    // startsWith(project + sep) is false, so it must clamp to the project root.
    const sibling = tmpProject + '-sibling';
    const out = wrapWithDocker('x', sibling, cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`), out);
  });

  it('falls back to the project root when cwd is empty', () => {
    const out = wrapWithDocker('x', '', cfg());
    assert.ok(out.includes(`-w '${tmpProject}'`), out);
  });
});

describe('wrapWithDocker — single-quote escaping', () => {
  it('escapes an embedded single quote in the command', () => {
    const out = wrapWithDocker("echo 'hi'", tmpProject, cfg());
    assert.ok(out.includes(`-c 'echo '\\''hi'\\'''`), out);
  });

  it('leaves a quote-free command as a plain single-quoted arg', () => {
    const out = wrapWithDocker('echo plain', tmpProject, cfg());
    assert.ok(out.endsWith(`-c 'echo plain'`), out);
  });
});

describe('_resetDockerAvailableCache — idempotence + re-probe', () => {
  it('is safe to call repeatedly and forces a fresh probe each cycle', () => {
    let calls = 0;
    mock.method(shellHooks, 'execSync', () => { calls++; return Buffer.from('1.2.3'); });

    assert.equal(isDockerAvailable(), true);
    assert.equal(isDockerAvailable(), true); // cached, no new probe
    assert.equal(calls, 1);

    // reset twice in a row — idempotent, returns undefined
    assert.equal(_resetDockerAvailableCache(), undefined);
    assert.equal(_resetDockerAvailableCache(), undefined);

    assert.equal(isDockerAvailable(), true);
    assert.equal(calls, 2, 'reset must force exactly one new probe');
  });

  it('a reset lets a now-failing daemon flip the cached result', () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('ok'));
    assert.equal(isDockerAvailable(), true);

    _resetDockerAvailableCache();
    mock.restoreAll();
    mock.method(shellHooks, 'execSync', () => { throw new Error('daemon gone'); });
    assert.equal(isDockerAvailable(), false);
  });
});

describe('detectContainerEscape — ALLOWS safe invocations', () => {
  const allowed = [
    'docker ps -a',
    'docker images',
    'docker logs mycontainer',
    'docker run --rm -it ubuntu:24.04 bash',
    'docker run -v /tmp/data:/data alpine echo hi',
    'docker run --volume /home/user/src:/app node:26 npm test',
    'docker run --network bridge alpine true',
    'docker run --cap-add=NET_ADMIN alpine true',
    'docker run --security-opt no-new-privileges:true alpine true',
    'podman run --rm alpine echo ok',
    'echo hello world',
    'ls -la /',
  ];
  for (const command of allowed) {
    it(`allows: ${command}`, () => {
      assert.deepEqual(detectContainerEscape(command), { blocked: false });
    });
  }
});

describe('detectContainerEscape — BLOCKS isolation-breaking flags', () => {
  const blocked: Array<[string, RegExp]> = [
    ['docker run --privileged ubuntu bash', /--privileged/],
    ['podman run --privileged alpine sh', /--privileged/],
    ['docker container run --privileged alpine true', /--privileged/],
    ['docker run --pid=host alpine ps aux', /PID namespace/],
    ['docker run --pid host alpine true', /PID namespace/],
    ['docker run --net=host alpine true', /host network/],
    ['docker run --network=host alpine true', /host network/],
    ['docker run --net host alpine true', /host network/],
    ['docker run -v /:/host ubuntu bash', /entire host filesystem/],
    ['docker run --volume /:/mnt alpine sh', /entire host filesystem/],
    ['docker run --volume=/:/ alpine sh', /entire host filesystem/],
    ['docker run --cap-add=ALL alpine true', /dangerous Linux capabilities/],
    ['docker run --cap-add SYS_ADMIN alpine true', /dangerous Linux capabilities/],
    ['docker run --cap-add=all alpine true', /dangerous Linux capabilities/], // case-insensitive
    ['docker run --security-opt=no-new-privileges:false alpine sh', /privilege escalation/],
    ['docker run --security-opt no-new-privileges:false alpine sh', /privilege escalation/],
    ['docker run --security-opt apparmor=unconfined alpine sh', /AppArmor/],
    ['docker run --security-opt=apparmor:unconfined alpine sh', /AppArmor/],
  ];
  for (const [command, reasonRe] of blocked) {
    it(`blocks: ${command}`, () => {
      const res = detectContainerEscape(command);
      assert.equal(res.blocked, true, command);
      assert.ok(res.reason && reasonRe.test(res.reason), `reason "${res.reason}" should match ${reasonRe}`);
    });
  }
});

describe('detectContainerEscape — scope guards', () => {
  it('ignores dangerous flags when there is no docker/podman token at all', () => {
    assert.deepEqual(detectContainerEscape('kubectl apply --privileged -f x.yaml'), { blocked: false });
  });

  it('ignores dangerous flags on non-run docker subcommands', () => {
    // --privileged on `docker ps` is not a `run` — isolation not at stake here
    assert.deepEqual(detectContainerEscape('docker ps --privileged'), { blocked: false });
  });

  it('allows a bare `docker run` with no flags', () => {
    assert.deepEqual(detectContainerEscape('docker run alpine true'), { blocked: false });
  });
});
