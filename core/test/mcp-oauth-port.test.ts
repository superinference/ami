import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as net from 'net';
import {
  findAvailablePort,
  probePort,
  buildRedirectUri,
  DEFAULT_OAUTH_CALLBACK_PORT,
  OAUTH_CALLBACK_HOST,
} from '../src/mcp/oauth-port';

// ---------------------------------------------------------------------------
// H42/H43: dynamic available-port discovery for the OAuth redirect listener —
// probe a preferred port, retry across a range, fall back to an ephemeral port.
// ---------------------------------------------------------------------------

describe('findAvailablePort (H42/H43)', () => {
  it('returns the preferred port when it is free', async () => {
    const seen: number[] = [];
    const probe = async (port: number) => { seen.push(port); return port; };
    const port = await findAvailablePort({ preferred: 40000, probe });
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
    const port = await findAvailablePort({ preferred: 40000, probe });
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
    const port = await findAvailablePort({ preferred: 40000, attempts: 3, probe });
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
    await findAvailablePort({ preferred: 65534, attempts: 10, probe });
    // Only 65534 and 65535 are valid candidates, then the ephemeral :0 fallback.
    assert.deepEqual(seen, [65534, 65535, 0]);
  });

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

  it('buildRedirectUri formats a loopback callback URL', () => {
    assert.equal(buildRedirectUri(33418), 'http://127.0.0.1:33418/oauth/callback');
    assert.equal(DEFAULT_OAUTH_CALLBACK_PORT, 33418);
  });
});
