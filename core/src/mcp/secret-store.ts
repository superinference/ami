/**
 * MCP secret-store seam — resolve MCP config secrets (bearer tokens in
 * `headers`, static credentials) from an env-var-only reference or the OS
 * keychain instead of inlining the plaintext value into `.ami/mcp.json`.
 *
 * Parity goal (xaaIdpCommand.ts: a confidential client secret is ingested only
 * via an env var — never CLI argv — and persisted in the OS keychain, so it
 * never lands in shell history, `ps`/argv, or a plaintext settings file). AMI
 * previously had no secure-at-rest path for MCP credentials: every header /
 * bearer token had to be written literally into a plaintext config, with only
 * `${VAR}` string expansion. This module adds a `${secret:...}` reference form
 * so the secret lives OUTSIDE the config file.
 *
 * Two backends, with deliberately different trust + sync profiles:
 *   - `${secret:env:NAME}`      — reads `process.env[NAME]`. Synchronous and as
 *     safe as the existing `${NAME}` expansion (just an env lookup), so it
 *     resolves everywhere regardless of workspace trust. It exists so an author
 *     can express "this is a SECRET, pulled from the environment" distinctly
 *     from an ordinary `${VAR}` substitution, keeping it out of the file.
 *   - `${secret:keychain:ACCOUNT}` — reads the value from the OS keychain
 *     (`security` on macOS, `secret-tool` on Linux). Asynchronous and spawns a
 *     subprocess, so it is resolved only at connect time AND only for a
 *     workspace-trusted server (see McpManager.connectServer): an untrusted
 *     project `.mcp.json` must not be able to siphon the user's keychain into a
 *     header it then POSTs to its own server URL.
 *
 * Self-contained core building block: no dependency on any upper layer, all I/O
 * (the keychain subprocess) goes through an injected {@link SecretSpawnFn} so it
 * is unit-testable without a real keychain.
 */
import { execFile } from 'child_process';

/** Which backend a secret reference resolves against. */
export const SECRET_REF_BACKENDS = ['env', 'keychain'] as const;
export type SecretRefBackend = (typeof SECRET_REF_BACKENDS)[number];

/** A parsed `${secret:<backend>:<key>}` reference found in a config string. */
export interface SecretRef {
  backend: SecretRefBackend;
  /** The env-var name (env backend) or keychain account (keychain backend). */
  key: string;
  /** The exact source token, e.g. `${secret:keychain:openai-token}`. */
  raw: string;
}

/**
 * Matches a `${secret:env:NAME}` / `${secret:keychain:ACCOUNT}` token. The key
 * segment forbids `}` (token terminator) and `:` (segment separator) so the
 * three-part grammar is unambiguous. Global + sticky-free so a single string may
 * carry several refs. Exported for direct token-shape assertions in tests.
 */
export const SECRET_REF_RE = /\$\{secret:(env|keychain):([^:}]+)\}/g;

/** Non-sensitive label for a secret ref — the backend + key, NEVER the value.
 *  Used in MCP debug logs / diagnostics so "what was resolved" is observable
 *  without leaking the secret (parity with `show` printing `(stored in
 *  keychain)` rather than the secret). */
export function describeSecretRef(ref: SecretRef): string {
  return `${ref.backend}:${ref.key}`;
}

/** Parse every `${secret:...}` reference in a string (empty array when none). */
export function parseSecretRefs(value: string): SecretRef[] {
  const refs: SecretRef[] = [];
  // A fresh regex per call — SECRET_REF_RE is global/stateful, so sharing
  // `lastIndex` across calls would skip refs on alternating invocations.
  const re = new RegExp(SECRET_REF_RE.source, 'g');
  let m: RegExpExecArray | null;
  while ((m = re.exec(value)) !== null) {
    refs.push({ backend: m[1] as SecretRefBackend, key: m[2], raw: m[0] });
  }
  return refs;
}

/** True when the string contains at least one `${secret:keychain:...}` ref. */
export function containsKeychainSecretRef(value: string | undefined): boolean {
  if (!value) return false;
  return parseSecretRefs(value).some((r) => r.backend === 'keychain');
}

/** True when any header value carries a `${secret:keychain:...}` ref — the
 *  signal McpManager.connectServer uses to decide whether an async
 *  (trust-gated) keychain resolution pass is needed before the handshake. */
export function headersHaveKeychainSecretRef(headers?: Record<string, string>): boolean {
  if (!headers) return false;
  return Object.values(headers).some(containsKeychainSecretRef);
}

/**
 * Resolve `${secret:env:NAME}` references synchronously from `env` (default
 * `process.env`). A missing env var leaves the ORIGINAL token in place (parity
 * with {@link expandEnvVarsTracked} leaving an unresolved `${VAR}` visible
 * rather than silently blanking it). `${secret:keychain:...}` refs are left
 * untouched — they are resolved on the async {@link resolveKeychainSecretRefs}
 * path.
 */
export function resolveEnvSecretRefs(value: string, env: NodeJS.ProcessEnv = process.env): string {
  return value.replace(new RegExp(SECRET_REF_RE.source, 'g'), (match, backend: string, key: string) => {
    if (backend !== 'env') return match; // keychain refs resolved elsewhere
    const v = env[key];
    return v !== undefined ? v : match;
  });
}

/** Outcome of a single keychain subprocess read. `killed` discriminates a
 *  timeout (the process was killed by the timeout) from an ordinary non-zero
 *  exit (e.g. "entry not found"). */
export interface SecretSpawnResult {
  stdout: string;
  /** Exit code, or `null` when the process was killed / failed to spawn. */
  code: number | null;
  /** True when the process was killed (timeout). */
  killed: boolean;
}

/** Injectable keychain subprocess runner (so tests never touch a real
 *  keychain). Resolves with the raw result; it never rejects — spawn failures
 *  (e.g. the tool is not installed) surface as `{ code: null, killed: false }`. */
export type SecretSpawnFn = (command: string, args: string[], timeoutMs: number) => Promise<SecretSpawnResult>;

/** Thrown by {@link KeychainSecretStore.getKeychainSecret} when the read timed
 *  out (killed) — distinct from "absent", so a transient timeout is NOT
 *  mistaken for a missing secret (it leaves the config token in place and is
 *  surfaced as a warning rather than silently dropping the credential). */
export class KeychainReadTimeoutError extends Error {
  constructor(public readonly account: string) {
    super(`keychain read for "${account}" timed out`);
    this.name = 'KeychainReadTimeoutError';
  }
}

/** Default keychain timeout (ms) — mirrors the reference's keychain read cap. */
export const DEFAULT_KEYCHAIN_TIMEOUT_MS = 10_000;

const defaultSecretSpawn: SecretSpawnFn = (command, args, timeoutMs) =>
  new Promise<SecretSpawnResult>((resolve) => {
    execFile(command, args, { timeout: timeoutMs, maxBuffer: 1024 * 1024 }, (err, stdout) => {
      if (!err) return resolve({ stdout: String(stdout), code: 0, killed: false });
      const e = err as NodeJS.ErrnoException & { killed?: boolean };
      // `killed` = timeout; a numeric `code` = the tool's own exit status
      // (e.g. 44 "not found" on macOS `security`); anything else (ENOENT: tool
      // not installed) collapses to code:null so it reads as "unavailable".
      resolve({
        stdout: String(stdout ?? ''),
        code: typeof e.code === 'number' ? e.code : null,
        killed: e.killed === true,
      });
    });
  });

/** Read-only OS-keychain secret store. Resolves a `${secret:keychain:ACCOUNT}`
 *  account to its stored value. */
export interface McpSecretStore {
  /** Resolve a keychain account to its secret, or `undefined` when absent /
   *  unavailable. Throws {@link KeychainReadTimeoutError} on timeout. */
  getKeychainSecret(account: string): Promise<string | undefined>;
}

export interface KeychainStoreOptions {
  /** Platform selector (default `process.platform`). */
  platform?: NodeJS.Platform;
  /** Injectable subprocess runner (default spawns the real keychain tool). */
  spawn?: SecretSpawnFn;
  /** Keychain service/collection the accounts live under (default `ami-mcp`). */
  service?: string;
  /** Per-read timeout in ms (default {@link DEFAULT_KEYCHAIN_TIMEOUT_MS}). */
  timeoutMs?: number;
}

/**
 * {@link McpSecretStore} backed by the platform OS keychain:
 *   - macOS: `security find-generic-password -s <service> -a <account> -w`
 *     (exit 44 = not found).
 *   - Linux: `secret-tool lookup service <service> account <account>`
 *     (non-zero = not found).
 * Any other platform has no supported backend → always `undefined` (never
 * spawns), so a `${secret:keychain:...}` ref on an unsupported OS degrades to an
 * unresolved token + warning rather than an error.
 */
export class KeychainSecretStore implements McpSecretStore {
  private readonly platform: NodeJS.Platform;
  private readonly spawn: SecretSpawnFn;
  private readonly service: string;
  private readonly timeoutMs: number;

  constructor(opts: KeychainStoreOptions = {}) {
    this.platform = opts.platform ?? process.platform;
    this.spawn = opts.spawn ?? defaultSecretSpawn;
    this.service = opts.service ?? 'ami-mcp';
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_KEYCHAIN_TIMEOUT_MS;
  }

  async getKeychainSecret(account: string): Promise<string | undefined> {
    const invocation = this.commandFor(account);
    if (!invocation) return undefined; // unsupported platform — no backend
    const { stdout, code, killed } = await this.spawn(invocation.command, invocation.args, this.timeoutMs);
    if (killed) throw new KeychainReadTimeoutError(account);
    if (code !== 0) return undefined; // not found / unavailable
    // Keychain tools append a trailing newline; strip exactly one so a stored
    // value is returned verbatim (do NOT full-trim — a secret may legitimately
    // start/end with spaces).
    const value = stdout.replace(/\n$/, '');
    return value === '' ? undefined : value;
  }

  async deleteKeychainSecret(account: string): Promise<boolean> {
    const invocation = this.deleteCommandFor(account);
    if (!invocation) return false; // unsupported platform — no backend
    const { code } = await this.spawn(invocation.command, invocation.args, this.timeoutMs);
    // exit 0 = a slot was deleted; anything else (not found / tool unavailable) =
    // nothing to clean up, reported as `false` rather than thrown.
    return code === 0;
  }

  private commandFor(account: string): { command: string; args: string[] } | null {
    if (this.platform === 'darwin') {
      return { command: 'security', args: ['find-generic-password', '-s', this.service, '-a', account, '-w'] };
    }
    if (this.platform === 'linux') {
      return { command: 'secret-tool', args: ['lookup', 'service', this.service, 'account', account] };
    }
    return null;
  }

  /** The DELETE invocation for a keychain account. Carries only the account name
   *  on argv (never a secret value), so it is argv-safe on every platform — unlike
   *  a keychain *write*, whose value the macOS `security` CLI can only take on
   *  argv (and so is deliberately not offered here). */
  private deleteCommandFor(account: string): { command: string; args: string[] } | null {
    if (this.platform === 'darwin') {
      return { command: 'security', args: ['delete-generic-password', '-s', this.service, '-a', account] };
    }
    if (this.platform === 'linux') {
      return { command: 'secret-tool', args: ['clear', 'service', this.service, 'account', account] };
    }
    return null;
  }
}

/** The default store used when a caller does not inject one. */
export function defaultMcpSecretStore(opts?: KeychainStoreOptions): McpSecretStore {
  return new KeychainSecretStore(opts);
}

// ---------------------------------------------------------------------------
// Write side — transactional keychain cleanup.
//
// Parity goal (xaaIdpCommand.ts setup/clear, L44-135 / L257-263): persistent MCP
// credential mutations follow a two-phase ordering so a crash / write failure can
// never leave a half-configured state:
//   1. validate EVERY input BEFORE any write (a mid-write exit must not leave the
//      config pointing at a resource whose secret was already written/removed);
//   2. mutate the OS keychain only AFTER the settings/config write has succeeded
//      (so a failed config write never orphans a live config whose secret is gone).
// AMI's read-only secret-store (above) had no write path, so this discipline had
// no home. This section adds the delete+ordering primitives; its first consumer is
// `/mcp remove`, which clears a removed server's keychain secrets only after the
// `.ami/mcp.json` write succeeds.
// ---------------------------------------------------------------------------

/**
 * A {@link McpSecretStore} that can also DELETE a keychain slot — the write-side
 * capability the transactional cleanup path needs. Only deletion is offered (not
 * a value write) because a delete carries just the account name on argv, which is
 * safe everywhere, whereas writing a secret value through the macOS `security` CLI
 * would leak it into `ps`/argv.
 */
export interface WritableMcpSecretStore extends McpSecretStore {
  /**
   * Delete the keychain slot for `account`. Resolves `true` when a slot was
   * actually deleted, `false` when none existed / the platform has no backend /
   * the tool is unavailable. Does not throw for an ordinary "not found" — a
   * cleanup miss must never abort the already-committed config write it follows.
   */
  deleteKeychainSecret(account: string): Promise<boolean>;
}

/** Writable default store (keychain-backed) for callers that mutate secrets. */
export function defaultWritableMcpSecretStore(opts?: KeychainStoreOptions): WritableMcpSecretStore {
  return new KeychainSecretStore(opts);
}

/**
 * Collect the DISTINCT keychain accounts referenced by `${secret:keychain:ACCOUNT}`
 * tokens anywhere in a server config value — recursively scanning every string
 * leaf (so `url`, `command`, `args[]`, `headers{}`, `env{}`, and any nested field
 * are all covered). Order-preserving and de-duplicated. `${secret:env:...}` refs
 * are ignored (nothing to clean up — they live in the environment, not a store).
 * Used to learn which keychain slots a server owned so they can be cleaned up when
 * it is removed.
 */
export function collectKeychainAccounts(config: unknown): string[] {
  const accounts: string[] = [];
  const seen = new Set<string>();
  const visit = (node: unknown): void => {
    if (typeof node === 'string') {
      for (const ref of parseSecretRefs(node)) {
        if (ref.backend === 'keychain' && !seen.has(ref.key)) {
          seen.add(ref.key);
          accounts.push(ref.key);
        }
      }
      return;
    }
    if (Array.isArray(node)) {
      for (const n of node) visit(n);
      return;
    }
    if (node !== null && typeof node === 'object') {
      for (const v of Object.values(node)) visit(v);
    }
  };
  visit(config);
  return accounts;
}

/** Outcome of clearing a config's keychain secrets: which accounts had a slot
 *  deleted (`cleared`) vs. which had nothing to delete / failed (`failed`). */
export interface KeychainCleanupResult {
  cleared: string[];
  failed: string[];
}

/**
 * Delete every keychain slot the given server `config` referenced via
 * `${secret:keychain:ACCOUNT}`. A no-op (the store is never touched) when the
 * config carries no keychain ref. Never throws. MUST be invoked only AFTER the
 * config write that removed/rewrote the server has succeeded (parity: the xaa IdP
 * `clear`/`setup` clear keychain slots only after `updateSettingsForSource`
 * returns without error) — see {@link runMcpSecretWriteTransaction}, which
 * enforces that ordering.
 */
export async function clearMcpConfigKeychainSecrets(
  config: unknown,
  store: WritableMcpSecretStore,
): Promise<KeychainCleanupResult> {
  const cleared: string[] = [];
  const failed: string[] = [];
  for (const account of collectKeychainAccounts(config)) {
    if (await store.deleteKeychainSecret(account)) cleared.push(account);
    else failed.push(account);
  }
  return { cleared, failed };
}

/**
 * A persistent MCP-credential mutation expressed as its three ordered phases.
 * {@link runMcpSecretWriteTransaction} runs them in exactly this order and stops
 * at the first failure, so the keychain is only ever touched after the config
 * write it depends on has committed.
 */
export interface McpSecretWriteTransaction {
  /** Validate ALL inputs. Return a human-readable error to abort BEFORE anything
   *  is written, or `null`/`undefined` to proceed. Optional (a caller that has
   *  already validated may omit it). */
  validate?: () => string | null | undefined;
  /** The settings/config write. Throws on failure — a throw aborts the
   *  transaction and the keychain is left UNTOUCHED. */
  writeSettings: () => void;
  /** Clear/rotate keychain secrets. Invoked ONLY after {@link writeSettings}
   *  returns without throwing. */
  clearSecrets: () => Promise<KeychainCleanupResult>;
}

/** Discriminated result of {@link runMcpSecretWriteTransaction}. */
export interface McpSecretWriteResult {
  ok: boolean;
  /** Set when a phase failed (validation error or the settings-write message). */
  error?: string;
  /** True only when {@link McpSecretWriteTransaction.writeSettings} committed. */
  settingsWritten: boolean;
  cleared: string[];
  failed: string[];
}

/**
 * Run a {@link McpSecretWriteTransaction} with crash-safe ordering:
 *   1. {@link McpSecretWriteTransaction.validate validate} — on error, return
 *      WITHOUT writing settings or touching the keychain;
 *   2. {@link McpSecretWriteTransaction.writeSettings writeSettings} — on throw,
 *      return with `settingsWritten:false` and the keychain UNTOUCHED (so a failed
 *      config write never orphans a live config whose secret was already deleted);
 *   3. {@link McpSecretWriteTransaction.clearSecrets clearSecrets} — only reached
 *      once settings committed.
 * This is the single enforcement point for the validate-before-write /
 * clear-after-write-success discipline.
 */
export async function runMcpSecretWriteTransaction(
  tx: McpSecretWriteTransaction,
): Promise<McpSecretWriteResult> {
  const validationError = tx.validate?.() ?? null;
  if (validationError !== null && validationError !== undefined) {
    return { ok: false, error: validationError, settingsWritten: false, cleared: [], failed: [] };
  }
  try {
    tx.writeSettings();
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
      settingsWritten: false,
      cleared: [],
      failed: [],
    };
  }
  const { cleared, failed } = await tx.clearSecrets();
  return { ok: true, settingsWritten: true, cleared, failed };
}

/** Result of resolving the keychain refs in a string. `unresolved` carries the
 *  refs that could not be resolved (absent, unavailable, or timed out) with
 *  their original token left in `resolved` so the failure is visible. */
export interface KeychainResolution {
  resolved: string;
  unresolved: SecretRef[];
}

/**
 * Resolve every `${secret:keychain:ACCOUNT}` reference in `value` via `store`.
 * A ref that resolves is substituted; one that is absent / unavailable / times
 * out is left as its literal token and reported in `unresolved` (never throws —
 * a timeout from {@link KeychainSecretStore.getKeychainSecret} is caught and
 * treated as unresolved, so one flaky read never blocks the connection). Env
 * refs are ignored here (handled synchronously by {@link resolveEnvSecretRefs}).
 */
export async function resolveKeychainSecretRefs(value: string, store: McpSecretStore): Promise<KeychainResolution> {
  const keychainRefs = parseSecretRefs(value).filter((r) => r.backend === 'keychain');
  if (keychainRefs.length === 0) return { resolved: value, unresolved: [] };
  let resolved = value;
  const unresolved: SecretRef[] = [];
  // De-dup by account so the same keychain entry referenced twice is read once.
  const seen = new Map<string, string | undefined>();
  for (const ref of keychainRefs) {
    if (!seen.has(ref.key)) {
      try {
        seen.set(ref.key, await store.getKeychainSecret(ref.key));
      } catch {
        seen.set(ref.key, undefined); // timeout / read error → unresolved
      }
    }
    const resolvedValue = seen.get(ref.key);
    if (resolvedValue === undefined) {
      unresolved.push(ref);
      continue;
    }
    resolved = resolved.split(ref.raw).join(resolvedValue);
  }
  return { resolved, unresolved };
}
