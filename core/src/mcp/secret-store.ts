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
export type SecretRefBackend = 'env' | 'keychain';

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

/** Default keychain timeout (ms) — mirrors Claude Code's keychain read cap. */
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

  private commandFor(account: string): { command: string; args: string[] } | null {
    if (this.platform === 'darwin') {
      return { command: 'security', args: ['find-generic-password', '-s', this.service, '-a', account, '-w'] };
    }
    if (this.platform === 'linux') {
      return { command: 'secret-tool', args: ['lookup', 'service', this.service, 'account', account] };
    }
    return null;
  }
}

/** The default store used when a caller does not inject one. */
export function defaultMcpSecretStore(opts?: KeychainStoreOptions): McpSecretStore {
  return new KeychainSecretStore(opts);
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
