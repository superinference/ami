/**
 * U360 — MCP secret-store seam.
 *
 * Covers the `${secret:env:...}` / `${secret:keychain:...}` reference form that
 * lets MCP config secrets (bearer tokens in `headers`) live OUTSIDE the plaintext
 * `.ami/mcp.json` — in the environment or the OS keychain — plus its end-to-end
 * wiring through resolveMcpServerHeaders + McpManager.connectServer (trust-gated
 * keychain resolution). Parity goal: a confidential secret is never inlined in
 * the config file (xaaIdpCommand.ts keeps the IdP client secret in the keychain,
 * out of argv/plaintext).
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import {
  parseSecretRefs,
  containsKeychainSecretRef,
  headersHaveKeychainSecretRef,
  describeSecretRef,
  resolveEnvSecretRefs,
  resolveKeychainSecretRefs,
  KeychainSecretStore,
  KeychainReadTimeoutError,
  defaultMcpSecretStore,
  DEFAULT_KEYCHAIN_TIMEOUT_MS,
  SECRET_REF_RE,
  type SecretSpawnFn,
  type SecretSpawnResult,
  type SecretRef,
} from '../src/mcp/secret-store';
import { McpManager, resolveMcpServerHeaders } from '../src/mcp/manager';

// A scripted keychain spawn: maps `account` → result, records invocations.
function scriptedSpawn(
  byArgsAccount: (account: string) => SecretSpawnResult,
): { spawn: SecretSpawnFn; calls: Array<{ command: string; args: string[]; timeoutMs: number }> } {
  const calls: Array<{ command: string; args: string[]; timeoutMs: number }> = [];
  const spawn: SecretSpawnFn = async (command, args, timeoutMs) => {
    calls.push({ command, args, timeoutMs });
    // account is the last arg on macOS (`-a <acct> -w` → acct at index len-2) or
    // after `account` on linux; grab it generically as the value following an
    // `account`/`-a` flag.
    const idx = args.findIndex((a) => a === '-a' || a === 'account');
    const account = idx >= 0 ? args[idx + 1] : '';
    return byArgsAccount(account);
  };
  return { spawn, calls };
}

const ok = (stdout: string): SecretSpawnResult => ({ stdout, code: 0, killed: false });

describe('U360 secret-store — parsing + detection', () => {
  it('parseSecretRefs extracts env and keychain refs with raw token + key', () => {
    const refs = parseSecretRefs('Bearer ${secret:keychain:tok} and ${secret:env:API}');
    assert.deepEqual(refs, [
      { backend: 'keychain', key: 'tok', raw: '${secret:keychain:tok}' },
      { backend: 'env', key: 'API', raw: '${secret:env:API}' },
    ]);
  });

  it('parseSecretRefs is stateless across calls (shared global regex not reused)', () => {
    const s = '${secret:env:A}';
    assert.equal(parseSecretRefs(s).length, 1);
    assert.equal(parseSecretRefs(s).length, 1); // would be 0 if lastIndex leaked
  });

  it('parseSecretRefs returns [] when there are no refs', () => {
    assert.deepEqual(parseSecretRefs('plain value ${NOT_A_SECRET}'), []);
  });

  it('SECRET_REF_RE rejects an unknown backend and a key containing a colon', () => {
    assert.deepEqual(parseSecretRefs('${secret:vault:x}'), []);
    assert.deepEqual(parseSecretRefs('${secret:env:a:b}'), []);
    assert.ok(SECRET_REF_RE.source.includes('keychain'));
  });

  it('containsKeychainSecretRef / headersHaveKeychainSecretRef flag only keychain refs', () => {
    assert.equal(containsKeychainSecretRef('${secret:keychain:x}'), true);
    assert.equal(containsKeychainSecretRef('${secret:env:x}'), false);
    assert.equal(containsKeychainSecretRef(undefined), false);
    assert.equal(headersHaveKeychainSecretRef({ A: '${secret:env:x}', B: '${secret:keychain:y}' }), true);
    assert.equal(headersHaveKeychainSecretRef({ A: 'plain' }), false);
    assert.equal(headersHaveKeychainSecretRef(undefined), false);
  });

  it('describeSecretRef never leaks the value — only backend:key', () => {
    const ref: SecretRef = { backend: 'keychain', key: 'openai-token', raw: '${secret:keychain:openai-token}' };
    assert.equal(describeSecretRef(ref), 'keychain:openai-token');
  });
});

describe('U360 secret-store — env ref resolution (sync, trust-free)', () => {
  it('resolves ${secret:env:NAME} from a provided env map', () => {
    assert.equal(resolveEnvSecretRefs('Bearer ${secret:env:TOK}', { TOK: 'abc' }), 'Bearer abc');
  });

  it('leaves the literal token when the env var is missing (visible, not blanked)', () => {
    assert.equal(resolveEnvSecretRefs('${secret:env:NOPE}', {}), '${secret:env:NOPE}');
  });

  it('leaves keychain refs untouched (resolved on the async path)', () => {
    assert.equal(resolveEnvSecretRefs('${secret:keychain:k}', {}), '${secret:keychain:k}');
  });

  it('defaults to process.env when no env map is given', () => {
    process.env.__U360_ENV = 'from-proc';
    assert.equal(resolveEnvSecretRefs('${secret:env:__U360_ENV}'), 'from-proc');
    delete process.env.__U360_ENV;
  });
});

describe('U360 KeychainSecretStore — bounded, platform-aware reads', () => {
  it('darwin: spawns `security find-generic-password` and returns the value (one trailing newline stripped)', async () => {
    const { spawn, calls } = scriptedSpawn((acct) => ok(acct === 'tok' ? 's3cr3t\n' : ''));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn, service: 'ami-mcp' });
    assert.equal(await store.getKeychainSecret('tok'), 's3cr3t');
    assert.deepEqual(calls[0].command, 'security');
    assert.deepEqual(calls[0].args, ['find-generic-password', '-s', 'ami-mcp', '-a', 'tok', '-w']);
    assert.equal(calls[0].timeoutMs, DEFAULT_KEYCHAIN_TIMEOUT_MS);
  });

  it('preserves leading/trailing spaces in the secret (no full trim)', async () => {
    const { spawn } = scriptedSpawn(() => ok('  pad me  \n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    assert.equal(await store.getKeychainSecret('tok'), '  pad me  ');
  });

  it('linux: spawns `secret-tool lookup` with service+account', async () => {
    const { spawn, calls } = scriptedSpawn(() => ok('linux-secret\n'));
    const store = new KeychainSecretStore({ platform: 'linux', spawn, service: 'svc' });
    assert.equal(await store.getKeychainSecret('acct'), 'linux-secret');
    assert.deepEqual(calls[0].command, 'secret-tool');
    assert.deepEqual(calls[0].args, ['lookup', 'service', 'svc', 'account', 'acct']);
  });

  it('non-zero exit (not found) → undefined', async () => {
    const { spawn } = scriptedSpawn(() => ({ stdout: '', code: 44, killed: false }));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    assert.equal(await store.getKeychainSecret('missing'), undefined);
  });

  it('empty value on success → undefined (treated as absent)', async () => {
    const { spawn } = scriptedSpawn(() => ok('\n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    assert.equal(await store.getKeychainSecret('blank'), undefined);
  });

  it('timeout (killed) → throws KeychainReadTimeoutError (not mistaken for absent)', async () => {
    const { spawn } = scriptedSpawn(() => ({ stdout: '', code: null, killed: true }));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    await assert.rejects(() => store.getKeychainSecret('slow'), (e: unknown) => {
      assert.ok(e instanceof KeychainReadTimeoutError);
      assert.match((e as Error).message, /slow/);
      return true;
    });
  });

  it('unsupported platform → undefined and NEVER spawns', async () => {
    const { spawn, calls } = scriptedSpawn(() => ok('x'));
    const store = new KeychainSecretStore({ platform: 'win32', spawn });
    assert.equal(await store.getKeychainSecret('tok'), undefined);
    assert.equal(calls.length, 0);
  });

  it('honors a custom timeout', async () => {
    const { spawn, calls } = scriptedSpawn(() => ok('v\n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn, timeoutMs: 250 });
    await store.getKeychainSecret('t');
    assert.equal(calls[0].timeoutMs, 250);
  });

  it('defaultMcpSecretStore returns an McpSecretStore', () => {
    const store = defaultMcpSecretStore({ platform: 'win32' });
    assert.equal(typeof store.getKeychainSecret, 'function');
  });
});

describe('U360 resolveKeychainSecretRefs — substitute + report unresolved', () => {
  it('substitutes a resolved keychain ref', async () => {
    const { spawn } = scriptedSpawn(() => ok('TOKEN\n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const out = await resolveKeychainSecretRefs('Bearer ${secret:keychain:k}', store);
    assert.equal(out.resolved, 'Bearer TOKEN');
    assert.deepEqual(out.unresolved, []);
  });

  it('reads a repeated account only once (de-dup)', async () => {
    const { spawn, calls } = scriptedSpawn(() => ok('V\n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const out = await resolveKeychainSecretRefs('${secret:keychain:k}-${secret:keychain:k}', store);
    assert.equal(out.resolved, 'V-V');
    assert.equal(calls.length, 1);
  });

  it('leaves the token + reports unresolved when the secret is absent', async () => {
    const { spawn } = scriptedSpawn(() => ({ stdout: '', code: 44, killed: false }));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const out = await resolveKeychainSecretRefs('X ${secret:keychain:gone}', store);
    assert.equal(out.resolved, 'X ${secret:keychain:gone}');
    assert.equal(out.unresolved.length, 1);
    assert.equal(out.unresolved[0].key, 'gone');
  });

  it('a timeout is caught → reported unresolved, never throws', async () => {
    const { spawn } = scriptedSpawn(() => ({ stdout: '', code: null, killed: true }));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const out = await resolveKeychainSecretRefs('${secret:keychain:slow}', store);
    assert.equal(out.unresolved.length, 1);
    assert.equal(out.resolved, '${secret:keychain:slow}');
  });

  it('no keychain ref → returns the value unchanged without touching the store', async () => {
    let touched = false;
    const store = { getKeychainSecret: async () => { touched = true; return 'x'; } };
    const out = await resolveKeychainSecretRefs('${secret:env:A} plain', store);
    assert.equal(out.resolved, '${secret:env:A} plain');
    assert.equal(touched, false);
  });
});

// ---------------------------------------------------------------------------
// End-to-end wiring through the manager's header resolver + connectServer.
// ---------------------------------------------------------------------------

describe('U360 resolveMcpServerHeaders — secret refs in static headers', () => {
  it('resolves ${secret:env:NAME} in a header (via resolveEnvVars)', async () => {
    process.env.__U360_HDR = 'env-token';
    const out = await resolveMcpServerHeaders('s', { command: '', headers: { Authorization: 'Bearer ${secret:env:__U360_HDR}' } });
    assert.deepEqual(out, { Authorization: 'Bearer env-token' });
    delete process.env.__U360_HDR;
  });

  it('resolves ${secret:keychain:ACCT} via the injected secret store', async () => {
    const { spawn } = scriptedSpawn(() => ok('kc-token\n'));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', headers: { Authorization: 'Bearer ${secret:keychain:tok}' } },
      undefined,
      undefined,
      store,
    );
    assert.deepEqual(out, { Authorization: 'Bearer kc-token' });
  });

  it('leaves an unresolved keychain ref in place and reports via onError', async () => {
    const { spawn } = scriptedSpawn(() => ({ stdout: '', code: 44, killed: false }));
    const store = new KeychainSecretStore({ platform: 'darwin', spawn });
    const errors: string[] = [];
    const out = await resolveMcpServerHeaders(
      's',
      { command: '', headers: { Authorization: 'Bearer ${secret:keychain:gone}' } },
      undefined,
      { onError: (e) => errors.push(String(e)) },
      store,
    );
    assert.deepEqual(out, { Authorization: 'Bearer ${secret:keychain:gone}' });
    assert.equal(errors.length, 1);
    assert.match(errors[0], /keychain:gone/);
  });
});

// Minimal fake client to observe the headers connectServer applies.
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  applied: Record<string, string> | null = null;
  setHeaders(h: Record<string, string>): void { this.applied = h; }
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return []; }
  disconnect(): void {}
}

describe('U360 connectServer — trust-gated keychain header resolution (E2E)', () => {
  let mgr: McpManager;
  let fake: FakeClient;
  beforeEach(() => {
    mgr = new McpManager();
    fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    const { spawn } = scriptedSpawn((acct) => ok(acct === 'tok' ? 'resolved-kc\n' : ''));
    mgr._secretStore = new KeychainSecretStore({ platform: 'darwin', spawn });
  });

  it('a trusted (non-project) server with a keychain header ref resolves it at connect', async () => {
    mgr.addServer('s', {
      command: '',
      transport: 'http',
      url: 'https://api.example.com',
      headers: { Authorization: 'Bearer ${secret:keychain:tok}' },
    });
    await mgr.connectServer('s');
    assert.ok(fake.applied, 'setHeaders must be called when a keychain header ref is present');
    assert.equal(fake.applied?.Authorization, 'Bearer resolved-kc');
  });

  it('does NOT resolve the keychain ref for an UNAPPROVED project server (trust boundary) and records a warning', async () => {
    // Register as a project-scoped, not-yet-approved server (scope is normally set
    // via loadFromConfig/loadManagedServers; seeded directly here).
    (mgr as unknown as { _serverScopes: Map<string, string> })._serverScopes.set('p', 'project');
    mgr.addServer('p', {
      command: '',
      transport: 'http',
      url: 'https://evil.example.com',
      headers: { Authorization: 'Bearer ${secret:keychain:tok}' },
    });
    await mgr.connectServer('p');
    // setHeaders must NOT have applied the resolved secret — the untrusted
    // project server must not siphon the keychain into a header it then POSTs.
    assert.equal(fake.applied, null);
    const warnings = mgr.getConfigWarnings('p');
    assert.ok(warnings.some((w) => /keychain secret refs not resolved/.test(w)), `expected keychain trust warning, got ${JSON.stringify(warnings)}`);
  });

  it('a server with NEITHER a helper NOR a keychain ref never calls setHeaders', async () => {
    mgr.addServer('plain', {
      command: '',
      transport: 'http',
      url: 'https://api.example.com',
      headers: { 'X-Static': '1' },
    });
    await mgr.connectServer('plain');
    assert.equal(fake.applied, null);
  });
});
