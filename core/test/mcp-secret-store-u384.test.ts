/**
 * U384 — MCP secret-store WRITE side: transactional keychain cleanup.
 *
 * Parity goal (xaaIdpCommand.ts setup/clear, L44-135 / L257-263): a persistent
 * MCP credential mutation validates ALL inputs before any write and touches the
 * OS keychain only AFTER the config/settings write has committed, so a crash /
 * failed write can never leave a live config pointing at a secret that is already
 * gone. Covers the delete-capable keychain store, the `${secret:keychain:…}`
 * account collector + cleaner, and the transactional ordering orchestrator.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  KeychainSecretStore,
  defaultWritableMcpSecretStore,
  collectKeychainAccounts,
  clearMcpConfigKeychainSecrets,
  runMcpSecretWriteTransaction,
  type SecretSpawnFn,
  type SecretSpawnResult,
  type WritableMcpSecretStore,
  type KeychainCleanupResult,
} from '../src/mcp/secret-store';

function scriptedSpawn(
  result: (command: string, args: string[]) => SecretSpawnResult,
): { spawn: SecretSpawnFn; calls: Array<{ command: string; args: string[] }> } {
  const calls: Array<{ command: string; args: string[] }> = [];
  const spawn: SecretSpawnFn = async (command, args) => {
    calls.push({ command, args });
    return result(command, args);
  };
  return { spawn, calls };
}

const exit = (code: number | null): SecretSpawnResult => ({ stdout: '', code, killed: false });

// A fake writable store recording which accounts a delete was requested for.
function fakeStore(deleted: (account: string) => boolean): WritableMcpSecretStore & { deletes: string[] } {
  const deletes: string[] = [];
  return {
    deletes,
    async getKeychainSecret() { return undefined; },
    async deleteKeychainSecret(account: string) {
      deletes.push(account);
      return deleted(account);
    },
  };
}

describe('U384 KeychainSecretStore.deleteKeychainSecret', () => {
  it('darwin: runs `security delete-generic-password -s <service> -a <account>` and maps exit 0 → true', async () => {
    const { spawn, calls } = scriptedSpawn(() => exit(0));
    const store = new KeychainSecretStore({ platform: 'darwin', service: 'ami-mcp', spawn });
    assert.equal(await store.deleteKeychainSecret('tok'), true);
    assert.deepEqual(calls, [{ command: 'security', args: ['delete-generic-password', '-s', 'ami-mcp', '-a', 'tok'] }]);
    // The secret VALUE never appears on argv — only the account name does.
    assert.ok(!calls[0].args.some((a) => a.toLowerCase().includes('secret')));
  });

  it('linux: runs `secret-tool clear service <service> account <account>`', async () => {
    const { spawn, calls } = scriptedSpawn(() => exit(0));
    const store = new KeychainSecretStore({ platform: 'linux', service: 'svc', spawn });
    assert.equal(await store.deleteKeychainSecret('acct'), true);
    assert.deepEqual(calls, [{ command: 'secret-tool', args: ['clear', 'service', 'svc', 'account', 'acct'] }]);
  });

  it('non-zero exit (not found / unavailable) → false, never thrown', async () => {
    const { spawn } = scriptedSpawn(() => exit(44));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    assert.equal(await store.deleteKeychainSecret('missing'), false);
  });

  it('exit null (tool not installed) → false', async () => {
    const { spawn } = scriptedSpawn(() => exit(null));
    const store = new KeychainSecretStore({ platform: 'linux', spawn });
    assert.equal(await store.deleteKeychainSecret('x'), false);
  });

  it('unsupported platform → false WITHOUT spawning', async () => {
    const { spawn, calls } = scriptedSpawn(() => exit(0));
    const store = new KeychainSecretStore({ platform: 'win32', spawn });
    assert.equal(await store.deleteKeychainSecret('x'), false);
    assert.equal(calls.length, 0);
  });

  it('defaultWritableMcpSecretStore returns a delete-capable store', () => {
    const store = defaultWritableMcpSecretStore({ platform: 'win32' });
    assert.equal(typeof store.deleteKeychainSecret, 'function');
  });
});

describe('U384 collectKeychainAccounts', () => {
  it('gathers distinct keychain accounts from headers, env, url, and args (order-preserving, de-duped)', () => {
    const config = {
      transport: 'http',
      url: 'https://api/${secret:keychain:urlKey}',
      headers: { Authorization: 'Bearer ${secret:keychain:tok}', 'X-Dup': 'Bearer ${secret:keychain:tok}' },
      env: { TOKEN: '${secret:keychain:envKey}' },
      args: ['--flag', '${secret:keychain:argKey}'],
    };
    assert.deepEqual(collectKeychainAccounts(config), ['urlKey', 'tok', 'envKey', 'argKey']);
  });

  it('ignores ${secret:env:…} refs and plain strings', () => {
    const config = { headers: { A: 'Bearer ${secret:env:PLAIN}', B: 'literal-token' } };
    assert.deepEqual(collectKeychainAccounts(config), []);
  });

  it('returns [] for a non-object / null config', () => {
    assert.deepEqual(collectKeychainAccounts(undefined), []);
    assert.deepEqual(collectKeychainAccounts(null), []);
    assert.deepEqual(collectKeychainAccounts(42), []);
    assert.deepEqual(collectKeychainAccounts('${secret:keychain:top}'), ['top']);
  });
});

describe('U384 clearMcpConfigKeychainSecrets', () => {
  it('deletes each referenced account and splits cleared vs failed', async () => {
    const store = fakeStore((a) => a === 'tok'); // only `tok` has a slot
    const config = { headers: { A: 'Bearer ${secret:keychain:tok}' }, env: { E: '${secret:keychain:gone}' } };
    const res = await clearMcpConfigKeychainSecrets(config, store);
    assert.deepEqual(res, { cleared: ['tok'], failed: ['gone'] });
    assert.deepEqual(store.deletes, ['tok', 'gone']);
  });

  it('is a no-op (store untouched) when there are no keychain refs', async () => {
    const store = fakeStore(() => true);
    const res = await clearMcpConfigKeychainSecrets({ headers: { A: 'Bearer ${secret:env:X}' } }, store);
    assert.deepEqual(res, { cleared: [], failed: [] });
    assert.equal(store.deletes.length, 0);
  });
});

describe('U384 runMcpSecretWriteTransaction — crash-safe ordering', () => {
  it('validation error aborts BEFORE any write or keychain touch', async () => {
    let wrote = false;
    let cleared = false;
    const res = await runMcpSecretWriteTransaction({
      validate: () => 'bad input',
      writeSettings: () => { wrote = true; },
      clearSecrets: async () => { cleared = true; return { cleared: [], failed: [] }; },
    });
    assert.deepEqual(res, { ok: false, error: 'bad input', settingsWritten: false, cleared: [], failed: [] });
    assert.equal(wrote, false);
    assert.equal(cleared, false);
  });

  it('a settings-write throw leaves the keychain UNTOUCHED', async () => {
    let cleared = false;
    const res = await runMcpSecretWriteTransaction({
      writeSettings: () => { throw new Error('disk full'); },
      clearSecrets: async () => { cleared = true; return { cleared: ['x'], failed: [] }; },
    });
    assert.equal(res.ok, false);
    assert.equal(res.settingsWritten, false);
    assert.equal(res.error, 'disk full');
    assert.deepEqual(res.cleared, []);
    assert.equal(cleared, false);
  });

  it('a non-Error throw is stringified', async () => {
    const res = await runMcpSecretWriteTransaction({
      writeSettings: () => { throw 'boom'; },
      clearSecrets: async () => ({ cleared: [], failed: [] }),
    });
    assert.equal(res.ok, false);
    assert.equal(res.error, 'boom');
  });

  it('success runs writeSettings THEN clearSecrets, in that order, and reports the cleanup', async () => {
    const order: string[] = [];
    const res = await runMcpSecretWriteTransaction({
      validate: () => null,
      writeSettings: () => { order.push('write'); },
      clearSecrets: async () => { order.push('clear'); return { cleared: ['tok'], failed: ['gone'] }; },
    });
    assert.deepEqual(order, ['write', 'clear']);
    assert.deepEqual(res, { ok: true, settingsWritten: true, cleared: ['tok'], failed: ['gone'] });
  });

  it('validate is optional (undefined return proceeds to write)', async () => {
    let wrote = false;
    const res = await runMcpSecretWriteTransaction({
      writeSettings: () => { wrote = true; },
      clearSecrets: async () => ({ cleared: [], failed: [] }),
    });
    assert.equal(wrote, true);
    assert.equal(res.ok, true);
  });

  it('validate returning undefined explicitly also proceeds', async () => {
    const res = await runMcpSecretWriteTransaction({
      validate: () => undefined,
      writeSettings: () => {},
      clearSecrets: async () => ({ cleared: [], failed: [] }),
    });
    assert.equal(res.ok, true);
  });
});

describe('U384 end-to-end: remove a server config, clean its keychain', () => {
  it('clears exactly the removed config’s keychain slots after the (fake) settings write', async () => {
    // Model the /mcp remove flow: a config map that we mutate in writeSettings,
    // and a keychain store whose deletions we observe only after the write.
    const servers: Record<string, unknown> = {
      live: { headers: { A: 'Bearer ${secret:keychain:keepTok}' } },
      gone: { transport: 'http', url: 'https://x', headers: { Authorization: 'Bearer ${secret:keychain:deadTok}' } },
    };
    const removed = servers['gone'];
    const store = fakeStore(() => true);
    let diskState: Record<string, unknown> | null = null;

    const result: KeychainCleanupResult & { ok: boolean } = await (async () => {
      const tx = await runMcpSecretWriteTransaction({
        writeSettings: () => { delete servers['gone']; diskState = { ...servers }; },
        clearSecrets: () => clearMcpConfigKeychainSecrets(removed, store),
      });
      return { ok: tx.ok, cleared: tx.cleared, failed: tx.failed };
    })();

    assert.equal(result.ok, true);
    assert.deepEqual(result.cleared, ['deadTok']);
    // settings write happened first (disk no longer has `gone`)…
    assert.ok(diskState && !('gone' in diskState) && 'live' in diskState);
    // …and only the removed server's slot was deleted — the live server's is kept.
    assert.deepEqual(store.deletes, ['deadTok']);
  });
});
