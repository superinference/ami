import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as net from 'net';
import {
  findAvailablePort,
  probePort,
  buildRedirectUri,
  readConfiguredPort,
  loopbackPortRange,
  DEFAULT_OAUTH_CALLBACK_PORT,
  OAUTH_CALLBACK_HOST,
  OAUTH_CALLBACK_PORT_ENV,
  LOOPBACK_PORT_RANGE,
  WINDOWS_LOOPBACK_PORT_RANGE,
} from '../src/mcp/oauth-port';

// ---------------------------------------------------------------------------
// H42/H43: dynamic available-port discovery for the OAuth redirect listener —
// honor a pinned env port, probe a preferred port, retry across a sequential
// run, random-probe within the (Windows-aware) loopback range, then fall back
// to an ephemeral port.
// ---------------------------------------------------------------------------

describe('findAvailablePort (H42/H43)', () => {
  it('returns the preferred port when it is free', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => { seen.push(port); return port; };
    const port = await findAvailablePort({ preferred: 40000, host: OAUTH_CALLBACK_HOST, env: {}, probe });
    assert.equal(port, 40000);
    assert.deepEqual(seen, [40000]);
  });

  it('retries sequential candidates when earlier ports are in use', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      if (port < 40002) throw new Error('EADDRINUSE');
      return port;
    };
    const port = await findAvailablePort({ preferred: 40000, env: {}, probe });
    assert.equal(port, 40002);
    assert.deepEqual(seen, [40000, 40001, 40002]);
  });

  it('falls back to an OS-assigned ephemeral port after exhausting the range', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      if (port !== 0) throw new Error('EADDRINUSE');
      return 55555; // the OS-assigned port
    };
    // randomAttempts:0 isolates the sequential -> ephemeral path.
    const port = await findAvailablePort({ preferred: 40000, attempts: 3, randomAttempts: 0, env: {}, probe });
    assert.equal(port, 55555);
    assert.deepEqual(seen, [40000, 40001, 40002, 0]);
  });

  it('does not probe beyond port 65535', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      if (port !== 0) throw new Error('EADDRINUSE');
      return 1234;
    };
    await findAvailablePort({ preferred: 65534, attempts: 10, randomAttempts: 0, env: {}, probe });
    // Only 65534 and 65535 are valid candidates, then the ephemeral :0 fallback.
    assert.deepEqual(seen, [65534, 65535, 0]);
  });

  // --- new: operator-pinned MCP_OAUTH_CALLBACK_PORT env override --------------

  it('returns a pinned MCP_OAUTH_CALLBACK_PORT without probing', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => { seen.push(port); return port; };
    const port = await findAvailablePort({
      env: { [OAUTH_CALLBACK_PORT_ENV]: '45999' },
      preferred: 40000,
      probe,
    });
    assert.equal(port, 45999);
    assert.deepEqual(seen, [], 'a pinned port must be returned without any bind probe');
  });

  it('ignores an invalid pinned port and falls through to probing', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => { seen.push(port); return port; };
    const port = await findAvailablePort({
      env: { [OAUTH_CALLBACK_PORT_ENV]: 'nope' },
      preferred: 40000,
      probe,
    });
    assert.equal(port, 40000);
    assert.deepEqual(seen, [40000]);
  });

  it('reads MCP_OAUTH_CALLBACK_PORT from process.env by default', async () => {
    const prev = process.env[OAUTH_CALLBACK_PORT_ENV];
    try {
      process.env[OAUTH_CALLBACK_PORT_ENV] = '46001';
      const seen: number[] = [];
      const probe = async (port: number) => { seen.push(port); return port; };
      // No `env` option -> findAvailablePort must consult process.env.
      const port = await findAvailablePort({ preferred: 40000, probe });
      assert.equal(port, 46001);
      assert.deepEqual(seen, []);
    } finally {
      if (prev === undefined) delete process.env[OAUTH_CALLBACK_PORT_ENV];
      else process.env[OAUTH_CALLBACK_PORT_ENV] = prev;
    }
  });

  // --- new: random probe within the loopback range ---------------------------

  it('random-probes within the loopback range after the sequential run fails', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      // Reject the sequential candidates; accept the first in-range random one.
      if (port >= LOOPBACK_PORT_RANGE.min && port <= LOOPBACK_PORT_RANGE.max) return port;
      throw new Error('EADDRINUSE');
    };
    // Deterministic RNG -> candidate = min + floor(0.5 * range).
    const random = () => 0.5;
    const port = await findAvailablePort({
      preferred: 100, attempts: 2, env: {}, platform: 'linux', random, probe,
    });
    const { min, max } = LOOPBACK_PORT_RANGE;
    const expected = min + Math.floor(0.5 * (max - min + 1));
    assert.equal(port, expected);
    assert.deepEqual(seen, [100, 101, expected]);
  });

  it('special-cases the Windows loopback range for random probing', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      if (port >= WINDOWS_LOOPBACK_PORT_RANGE.min && port <= WINDOWS_LOOPBACK_PORT_RANGE.max) return port;
      throw new Error('EADDRINUSE');
    };
    const random = () => 0; // candidate = min
    const port = await findAvailablePort({
      preferred: 100, attempts: 1, env: {}, platform: 'win32', random, probe,
    });
    assert.equal(port, WINDOWS_LOOPBACK_PORT_RANGE.min);
    assert.deepEqual(seen, [100, WINDOWS_LOOPBACK_PORT_RANGE.min]);
  });

  it('falls back to ephemeral when sequential and random probes all fail', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => {
      seen.push(port);
      if (port !== 0) throw new Error('EADDRINUSE');
      return 61000;
    };
    const random = () => 0.25;
    const port = await findAvailablePort({
      preferred: 100, attempts: 1, randomAttempts: 2, env: {}, platform: 'linux', random, probe,
    });
    const { min, max } = LOOPBACK_PORT_RANGE;
    const rnd = min + Math.floor(0.25 * (max - min + 1));
    assert.equal(port, 61000);
    assert.deepEqual(seen, [100, rnd, rnd, 0]);
  });

  it('uses real probePort and default options when none are injected', async () => {
    // Exercises the real net bind plus the default preferred/attempts/probe/
    // platform/random/randomAttempts fall-throughs. preferred:0 asks the OS for
    // a free ephemeral port directly.
    const port = await findAvailablePort({ preferred: 0, env: {} });
    assert.ok(port > 0 && port <= 65535);
  });

  // --- probePort + helpers ---------------------------------------------------

  it('probePort binds a real free port and reports it', async () => {
    // Grab a real free ephemeral port, then assert probePort can bind it.
    const got = await probePort(0, OAUTH_CALLBACK_HOST);
    assert.ok(got > 0 && got <= 65535);
  });

  it('probePort rejects when the port is already held', async () => {
    const srv = net.createServer();
    await new Promise<void>((res) => srv.listen(0, OAUTH_CALLBACK_HOST, () => res()));
    const port = (srv.address() as net.AddressInfo).port;
    try {
      await assert.rejects(() => probePort(port, OAUTH_CALLBACK_HOST));
    } finally {
      srv.close();
    }
  });

  it('readConfiguredPort honors a positive int, rejects junk/zero/negatives', () => {
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '45999' }), 45999);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: 'nope' }), undefined);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '0' }), undefined);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '-5' }), undefined);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '' }), undefined);
    assert.equal(readConfiguredPort({}), undefined);
    // Default param reads process.env (no arg).
    const def = readConfiguredPort();
    assert.ok(def === undefined || (typeof def === 'number' && def > 0));
  });

  it('loopbackPortRange selects the Windows band only on win32', () => {
    assert.deepEqual(loopbackPortRange('win32'), WINDOWS_LOOPBACK_PORT_RANGE);
    assert.deepEqual(loopbackPortRange('linux'), LOOPBACK_PORT_RANGE);
    assert.deepEqual(loopbackPortRange('darwin'), LOOPBACK_PORT_RANGE);
    // Default param uses process.platform.
    const r = loopbackPortRange();
    assert.ok(r === LOOPBACK_PORT_RANGE || r === WINDOWS_LOOPBACK_PORT_RANGE);
  });

  it('buildRedirectUri formats a loopback callback URL', () => {
    assert.equal(buildRedirectUri(33418), 'http://127.0.0.1:33418/oauth/callback');
    assert.equal(DEFAULT_OAUTH_CALLBACK_PORT, 33418);
  });
});
