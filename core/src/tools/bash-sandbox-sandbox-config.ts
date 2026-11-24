// Split from bash-sandbox.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { execSync } from 'child_process';

export interface SandboxConfig {
  allowNetwork: boolean;
  readOnlyPaths: string[];
  writablePaths: string[];
  maxMemoryMB: number;
  maxProcesses: number;
  timeout: number;
  hiddenEnvVars?: string[];
  hiddenFiles?: string[];
  allowedDomains?: string[];
  // PARITY (host denylist): domains blocked for outbound traffic. Honored only
  // within the allowlist path (alongside `allowedDomains`), where their resolved
  // IPs are DROPped BEFORE the allow ACCEPT rules so a denied host wins over the
  // allowlist even if it also matches an allowed entry. Mirrors the reference's
  // `SandboxNetworkConfig.deniedHosts` ("deny wins over allow"). Additive:
  // undefined/empty leaves the allowlist behavior unchanged.
  deniedDomains?: string[];
  // When false/omitted (the secure default), the unshare sandbox bind-mounts
  // /dev/null over well-known privileged daemon sockets (Docker/containerd)
  // inside its mount namespace so a sandboxed command cannot reach them — a
  // classic sandbox-escape vector (e.g. `curl --unix-socket /var/run/docker.sock`
  // can launch a privileged container). Set true to opt out of that denial.
  allowUnixSockets?: boolean;
  // PARITY (per-path socket allow): specific Unix-socket paths to RE-ALLOW (i.e.
  // NOT bind /dev/null over) even while the blanket socket denial is otherwise
  // active. Mirrors the reference's `SandboxNetworkConfig.allowUnixSockets: string[]`:
  // granular control instead of the all-or-nothing `allowUnixSockets` boolean.
  // A path listed here is removed from the default-denied set. Additive:
  // undefined/empty keeps every default socket denied.
  allowedUnixSockets?: string[];
  // PARITY (filesystem read-deny): absolute paths that must be UNreadable inside
  // the sandbox. Each is bind-mounted with /dev/null so a read returns empty,
  // using the same technique as credential file-hiding. Mirrors the reference's
  // `SandboxFilesystemConfig.denyRead`. Additive: undefined/empty is a no-op.
  denyReadPaths?: string[];
  // PARITY (filesystem write-deny): absolute paths that must be read-only inside
  // the sandbox even if they would otherwise be writable. Each is bind-remounted
  // read-only, the same technique as `readOnlyPaths`. Mirrors the reference's
  // `SandboxFilesystemConfig.denyWrite`. Additive: undefined/empty is a no-op.
  denyWritePaths?: string[];
  // PARITY (config-rewrite / privilege-escalation escape): when true, the
  // sandbox bind-remounts the agent's own settings/config files AND the
  // auto-loaded skills/commands/agents directories (under the command's cwd)
  // read-only, so a sandboxed command cannot rewrite the trusted config to
  // weaken the sandbox, nor plant an auto-discovered skill/command/agent that
  // would later run with full capabilities. Mirrors the reference's
  // sandbox-adapter unconditional `denyWrite` of settings.json files and
  // `.claude/skills`/commands/agents. Additive: undefined/false is a no-op.
  denyConfigWrites?: boolean;
  // PARITY (bare-repo .git-file poisoning defense): when true, the sandbox
  // bind-remounts read-only the files git's `is_git_directory()` keys on
  // (HEAD, objects, refs, hooks, config) under cwd, so a sandboxed command
  // cannot alter a planted/pre-existing bare-repo marker (e.g. a `config` with
  // `core.fsmonitor`) that a later UNsandboxed `git` run would execute. Mirrors
  // the reference's sandbox-adapter bare-repo guard. Additive: undefined/false is
  // a no-op.
  denyBareGitPoisoning?: boolean;
  // PARITY (fail-closed hard gate): when true, an unavailable sandbox is a
  // FATAL startup error rather than a silent degrade-to-unsandboxed. Consumed
  // by callers via {@link validateSandboxEnabled}'s `fatal` flag. Intended for
  // managed deployments that require sandboxing. Mirrors the reference's
  // `SandboxSettings.failIfUnavailable`. Additive: undefined/false preserves the
  // warn-and-run-unsandboxed default.
  failIfUnavailable?: boolean;
  // PARITY (filesystem read RE-allow with precedence): absolute paths that must
  // stay READABLE inside the sandbox even when they fall inside a `denyReadPaths`
  // region. Any `denyReadPaths` entry equal to, or nested under, one of these is
  // NOT /dev/null-bound — "allowRead wins over denyRead for matching paths".
  // Mirrors the reference's `SandboxFilesystemConfig.allowRead`. Additive: undefined/
  // empty leaves every `denyReadPaths` entry denied.
  allowReadPaths?: string[];
  // PARITY (filesystem write RE-allow with precedence): absolute paths that must
  // stay WRITABLE inside the sandbox even when they fall inside a `denyWritePaths`
  // region. Any `denyWritePaths` entry equal to, or nested under, one of these is
  // NOT read-only-remounted — the write mirror of `allowReadPaths`,
  // "allowWrite wins over denyWrite for matching paths" (the reference's nested
  // `SandboxFilesystemConfig.allowWrite` / `denyWithinAllow` carve-out model, and
  // the host-backend home of `permissions.additionalDirectories`/`--add-dir`
  // grants). Additive: undefined/empty leaves every `denyWritePaths` entry denied.
  allowWritePaths?: string[];
  // PARITY (proxy-routed network isolation): when set, the sandbox exports the
  // standard HTTP/HTTPS proxy env vars pointing at a loopback proxy on this port
  // so outbound traffic is routed (and can be MITM-filtered) through it instead
  // of the raw iptables IP allowlist. Mirrors the reference's
  // `SandboxNetworkConfig.httpProxyPort`. Additive: undefined/invalid is a no-op.
  httpProxyPort?: number;
  // PARITY (proxy-routed network isolation, SOCKS): loopback SOCKS proxy port
  // exported as `all_proxy`/`ALL_PROXY`. Mirrors the reference's
  // `SandboxNetworkConfig.socksProxyPort`. Additive: undefined/invalid is a no-op.
  socksProxyPort?: number;
  // PARITY (sandbox-scoped temp dir): directory to pin as the sandbox's TMPDIR/
  // TMP/TEMP so a sandboxed command's scratch/heredoc temp files land in a
  // writable, isolated location (created if missing) rather than leaking to a
  // user-env TMPDIR that could point outside the sandbox. Mirrors the reference's
  // bash/powershell provider `sandboxTmpDir` TMPDIR override. Additive:
  // undefined is a no-op.
  sandboxTmpDir?: string;
  // PARITY (allowLocalBinding): when the sandbox runs in its OWN network
  // namespace (outbound network off) the loopback interface starts DOWN, so a
  // sandboxed server/test that binds 127.0.0.1 fails. When true, bring loopback
  // UP so purely-LOCAL binding works WITHOUT opening outbound network. Mirrors
  // the reference's `SandboxNetworkConfig.allowLocalBinding`. No-op when outbound
  // network is already on (loopback is up) or the knob is unset.
  allowLocalBinding?: boolean;
  // PARITY (enableWeakerNestedSandbox): drop the `--map-root-user` unshare flag,
  // which fails inside a NESTED user namespace (outer container / CI) where the
  // UID map is already consumed — letting a (weaker) sandbox still start there.
  // Security-weakening escape hatch. Additive: undefined/false keeps the flag.
  enableWeakerNestedSandbox?: boolean;
  // PARITY (enableWeakerNetworkIsolation): do NOT bind /dev/null over the
  // default-denied Unix domain sockets, so a local trust/CA service (e.g. a MITM
  // proxy's validator) stays reachable. Security-weakening escape hatch.
  // Additive: undefined/false keeps the default socket denial.
  enableWeakerNetworkIsolation?: boolean;
  // PARITY (encoded-command seam — the reference's PowerShell `-EncodedCommand`): when
  // true, the user command is handed to the inner shell base64-encoded and decoded
  // at exec time, so NO additional shell-quoting layer (a nested `sh -c`, the outer
  // `bash -c '...'` body being re-wrapped, a sandbox runtime re-applying
  // shellquote) can corrupt quoting-sensitive metacharacters (`!$?`, backslashes)
  // the way `!$?`→`\!$?` happens inside re-quoted double quotes. A base64 payload
  // is [A-Za-z0-9+/=] only, so it survives ANY quoting layer. Driven by the
  // {@link isSandboxEncodeCommandEnabled} env gate (`AMI_SANDBOX_ENCODE_COMMAND`).
  // Additive: undefined/false wraps the raw single-quote-escaped command exactly
  // as before (no behavior change). Honored on the Linux `unshare` backend only,
  // where coreutils `base64` is guaranteed; the ulimits fallback keeps raw wrapping.
  encodeCommand?: boolean;
}

/**
 * PARITY (encoded-command seam — the reference's PowerShell `-EncodedCommand`): the env
 * gate `AMI_SANDBOX_ENCODE_COMMAND` that opts the host `unshare` sandbox into
 * passing the user command base64-encoded (decoded at exec time) so a nested
 * shell-quoting layer cannot corrupt quoting-sensitive metacharacters. Default OFF
 * → the raw single-quote-escaped command is wrapped exactly as before. Reads an
 * injectable env for testability, mirroring the other sandbox env gates. Truthy =
 * `1`/`true`/`yes`/`on`.
 */
export const SANDBOX_ENCODE_COMMAND_ENV = 'AMI_SANDBOX_ENCODE_COMMAND';

export function isSandboxEncodeCommandEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[SANDBOX_ENCODE_COMMAND_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * PARITY (sandbox-indicator knob — the reference's `CLAUDE_CODE_BASH_SANDBOX_SHOW_INDICATOR`
 * → `SandboxedBash` userFacingName): the env gate `AMI_BASH_SANDBOX_SHOW_INDICATOR`
 * that, when truthy AND a command is actually sandboxed, surfaces a visible
 * `[sandboxed]` indicator to the user so they can tell a sandboxed run from an
 * unsandboxed one. Default OFF → no indicator (output unchanged). Reads an
 * injectable env for testability. Truthy = `1`/`true`/`yes`/`on`.
 */
export const BASH_SANDBOX_SHOW_INDICATOR_ENV = 'AMI_BASH_SANDBOX_SHOW_INDICATOR';

export function isSandboxIndicatorEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = (env[BASH_SANDBOX_SHOW_INDICATOR_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * The user-facing indicator for a bash call, parity with the reference's `SandboxedBash`
 * vs `Bash` userFacingName switch: returns `'SandboxedBash'` only when the command
 * IS sandboxed AND the indicator knob is enabled; otherwise `'Bash'`. Pure; env
 * injectable. Consumed by the bash tool to annotate a sandboxed command's output.
 */
export function sandboxBashDisplayName(sandboxed: boolean, env: NodeJS.ProcessEnv = process.env): string {
  return sandboxed && isSandboxIndicatorEnabled(env) ? 'SandboxedBash' : 'Bash';
}

/**
 * PARITY (encoded-command seam): build the exec expression for the user command
 * inside the sandbox. When `encode` is set, the RAW command is base64-encoded and
 * decoded + run by an inner `bash` at exec time — immune to any outer re-quoting
 * layer. Otherwise the pre-escaped `escapedCmd` (single-quote-escaped for the
 * outer `bash -c '...'`) is returned unchanged. The base64 alphabet is
 * [A-Za-z0-9+/=], so the emitted `printf %s '<b64>'` is injection-free. Pure +
 * exported for direct unit testing of both arms.
 */
export function buildSandboxRunExpr(rawCommand: string, escapedCmd: string, encode: boolean): string {
  if (!encode) return escapedCmd;
  const b64 = Buffer.from(rawCommand, 'utf8').toString('base64');
  return `printf %s '${b64}' | base64 -d | bash`;
}

export function getDefaultMemoryMB(): number {
  const env = process.env.AMI_TOOL_MEMORY_LIMIT_MB;
  if (env) {
    const n = parseInt(env, 10);
    if (n >= 64 && n <= 65536) return n;
  }
  return 512;
}

/** The WSL (Windows Subsystem for Linux) generation, which decides whether the
 *  host namespace sandbox can run at all. */
export const WSL_VERSIONS = ['none', 'wsl1', 'wsl2'] as const;

export type WslVersion = (typeof WSL_VERSIONS)[number];

/**
 * PARITY (the reference's WSL1-vs-WSL2 sandbox gating — `isSupportedPlatform()`):
 * classify a kernel release/version string into the WSL generation. Pure +
 * injectable so the branch can be tested on any host:
 *   - no `microsoft`/`wsl` marker → `'none'` (native Linux or other),
 *   - marker PLUS a `wsl2` / `microsoft-standard` tag → `'wsl2'`,
 *   - marker without that tag → `'wsl1'`.
 * This answers a DIFFERENT question than the MCP layer's boolean `detectWsl()`
 * (is-this-WSL-at-all, for `/mnt/c` config discovery): the sandbox needs the
 * GENERATION, because WSL2 runs a real Linux kernel (user namespaces + `unshare`
 * work → the sandbox runs) while WSL1 is a syscall-translation shim with NO
 * namespace support, so the host sandbox cannot run there and the reference reports a
 * targeted "requires WSL2" error rather than a confusing generic unshare failure.
 */
export function detectWslVersion(kernel: string): WslVersion {
  const k = kernel.toLowerCase();
  if (!k.includes('microsoft') && !k.includes('wsl')) return 'none';
  if (k.includes('wsl2') || k.includes('microsoft-standard')) return 'wsl2';
  return 'wsl1';
}

/** Result of the platform-support check for the host namespace sandbox. */
export interface SandboxPlatformSupport {
  /** True when the host namespace sandbox CAN run on this platform/WSL generation. */
  supported: boolean;
  /** When unsupported, the targeted reason (e.g. the WSL2 requirement). */
  reason?: string;
}

/**
 * Best-effort probe for whether an external command is resolvable on the current
 * PATH (via the POSIX `command -v` builtin, which works even with an empty PATH
 * because the shell itself is spawned by absolute path). Used by
 * {@link checkSandboxDependencies} to name the SPECIFIC missing tool (unshare /
 * iptables / getent) rather than conflating all failures into one generic
 * message. NOT memoized: PATH can change between calls (and across tests), and
 * the cost is only paid on diagnostics (`/sandbox`, validate), never per command.
 */
export function isCommandAvailable(name: string): boolean {
  // Guard: only ever probe the fixed internal literals below. Rejecting anything
  // else keeps the shell invocation injection-free even if a caller slips.
  if (!/^[a-zA-Z0-9_-]+$/.test(name)) return false;
  try {
    execSync(`command -v ${name}`, { stdio: 'pipe', timeout: 5000 });
    return true;
  } catch {
    return false;
  }
}

/**
 * Structured, two-tier result of a sandbox dependency pre-flight (parity with
 * the reference's `SandboxManager.checkDependencies()` → `{ errors, warnings }`):
 *   - `errors`   — the namespace sandbox (real filesystem/network isolation)
 *                  cannot run at all; each string names the specific missing
 *                  dependency/capability and how to install/enable it.
 *   - `warnings` — the sandbox CAN run but a REQUESTED security control is
 *                  silently degraded (e.g. `allowedDomains` set but `iptables`
 *                  unavailable, so the outbound allowlist is a no-op). Non-fatal,
 *                  surfaced to the user so a security footgun isn't hidden.
 */
export interface SandboxDependencyCheck {
  errors: string[];
  warnings: string[];
}

/**
 * Pre-flight the host bash sandbox's dependencies and report, split into
 * `errors` (namespace isolation cannot run — names the specific missing tool:
 * unshare not installed vs. present-but-userns-disabled, or non-Linux platform)
 * and `warnings` (a requested control silently cannot be enforced — an
 * `allowedDomains`/`deniedDomains` policy with `iptables`/`getent` missing).
 *
 * Mirrors the reference's `checkDependencies()` two-tier contract and closes the
 * "security control silently unenforced" footgun: `buildNetworkAllowlist`
 * swallows iptables failures with `2>/dev/null`, so without this warning a user
 * who set `allowedDomains` on a host lacking iptables would believe outbound
 * traffic is restricted when it is wide open. Additive + pure: no cfg → only the
 * platform/unshare errors; the degradation warnings require the relevant cfg.
 */
/** Shell glob metacharacters a literal bind mount cannot expand. */
export const GLOB_METACHARS = /[*?[\]{}]/;

/**
 * Collect every configured filesystem-restriction path that contains a glob
 * metacharacter. Such entries silently no-op under the bind-mount sandbox (which
 * matches literal paths only), so {@link checkSandboxDependencies} surfaces them
 * as a degradation warning. Deduped and order-stable.
 */
export function collectGlobPatternPaths(cfg: Partial<SandboxConfig>): string[] {
  const lists: (string[] | undefined)[] = [
    cfg.readOnlyPaths,
    cfg.writablePaths,
    cfg.denyReadPaths,
    cfg.denyWritePaths,
    cfg.allowReadPaths,
    cfg.allowWritePaths,
  ];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const list of lists) {
    for (const p of list ?? []) {
      if (GLOB_METACHARS.test(p) && !seen.has(p)) {
        seen.add(p);
        out.push(p);
      }
    }
  }
  return out;
}

export const SANDBOX_TRIGGER_PATTERNS = [
  /\bcurl\b/,
  /\bwget\b/,
  /\bpython\s/,
  /\bpython3?\s/,
  /\bnode\s+-e\b/,
  /\bperl\s+-e\b/,
  /\bruby\s+-e\b/,
  /\bphp\s+-r\b/,
  /\beval\b/,
  /\bsh\s+-c\b/,
  /\bbash\s+-c\b/,
];

export const SANDBOX_EXEMPT_PATTERNS = [
  /\bgit\b/,
  /\bnpm\b/,
  /\bnpx\b/,
  /\byarn\b/,
  /\bpnpm\b/,
  /\bmake\b/,
  /\bcargo\b/,
  /\bgo\s+(build|test|run|mod)\b/,
  /\bpytest\b/,
  /\bpython\S*\s+-m\s+pytest\b/,
  /\bpython\S*\s+-m\s+unittest\b/,
  /\bruntests\.py\b/,
  /\bmanage\.py\s+test\b/,
  /\bpip\s+install\b/,
  /\buv\s+pip\b/,
];

/**
 * Options controlling which commands bypass the sandbox.
 *
 * By default the hardcoded `SANDBOX_EXEMPT_PATTERNS` (package managers, build
 * tools, test runners) are exempt. Operators/policy can extend or replace that
 * set via `excludedCommands`:
 *   - `excludedCommands` is a list of command rules that should NOT be
 *     sandboxed. Each entry is parsed into a structured rule
 *     (see {@link parseExcludedRule}) anchored to the base command of each
 *     subcommand rather than matched as a loose substring:
 *       - a bare token / `foo:*` → PREFIX rule: matches the command `foo` or any
 *         `foo <args...>` (AMI's additive-prefix convenience; also matches the
 *         exact command);
 *       - a pattern with an unescaped `*` → WILDCARD rule (e.g.
 *         `bazel build //...:*`), matched with the canonical {@link wildcardMatch}.
 *   - When `overrideDefaults` is false/omitted, `excludedCommands` is ADDITIVE
 *     to the hardcoded defaults (no behavior change by default — an empty/absent
 *     list keeps exactly the current exemptions).
 *   - When `overrideDefaults` is true, `excludedCommands` REPLACES the hardcoded
 *     defaults entirely (an operator opting into a stricter or fully custom
 *     exemption policy).
 *
 * Exemption matching mirrors the reference's `containsExcludedCommand`: each
 * top-level subcommand is normalized to a fixed-point candidate set by stripping
 * leading env-var assignments (except binary-hijack vars, see
 * {@link BINARY_HIJACK_VARS}) and safe wrappers (`timeout`, `nice`, …) so
 * `timeout 300 FOO=bar bazel run` still matches a `bazel:*` exemption, while a
 * hijack prefix like `LD_PRELOAD=evil.so bazel` does NOT.
 *
 * NOTE: `excludedCommands` is a user-facing convenience, not a security
 * boundary — the sandbox permission system is the actual control.
 */
export interface SandboxExemptionOptions {
  excludedCommands?: string[];
  overrideDefaults?: boolean;
  // PARITY (default-deny coverage): when true, {@link shouldUseSandbox} sandboxes
  // EVERY command that is not explicitly exempt instead of only the hardcoded
  // `SANDBOX_TRIGGER_PATTERNS`. Mirrors the reference's `shouldUseSandbox()`, which
  // returns true for all commands once sandboxing is enabled and false only for
  // excluded commands — closing the hole where a non-trigger command (`./evil`,
  // `nc`, `ssh`, `scp`, `dd`, a path-invoked script, `> /etc/…`) ran unsandboxed.
  // Driven by the `AMI_SANDBOX_DEFAULT_DENY` gate ({@link isSandboxDefaultDeny}).
  // Additive: undefined/false preserves the trigger-allowlist behavior exactly.
  defaultSandbox?: boolean;
}

/**
 * Resolve the effective list of hardcoded sandbox-exempt regex patterns for a
 * policy. Returns the hardcoded defaults unless `overrideDefaults` is set, in
 * which case the defaults are dropped and only the structured user
 * `excludedCommands` rules (see {@link buildExcludedRules}) apply.
 */
export function buildSandboxExemptPatterns(options?: SandboxExemptionOptions): RegExp[] {
  if (options?.overrideDefaults) return [];
  return [...SANDBOX_EXEMPT_PATTERNS];
}

/**
 * Env-var prefixes that make a DIFFERENT binary run (injection or resolution
 * hijack). These are NOT stripped before exemption matching, so an operator's
 * `bazel:*` exemption cannot be smuggled onto a hijacked binary via
 * `LD_PRELOAD=evil.so bazel ...` or `PATH=/evil bazel ...`. Heuristic only
 * (excludedCommands is a convenience, not a security boundary), mirroring
 * the reference's `BINARY_HIJACK_VARS`.
 */
export const BINARY_HIJACK_VARS = /^(LD_|DYLD_|PATH$)/;

// A leading `NAME=value ` assignment. Values are restricted to quoted strings or
// a run of non-whitespace/non-quote chars (shell metacharacters that could
// change what runs are intentionally NOT consumed). Horizontal whitespace only
// (`[ \t]`): a newline is a command separator, not an assignment terminator.
export const LEADING_ENV_ASSIGN_RE =
  /^([A-Za-z_][A-Za-z0-9_]*)=(?:'[^'\n\r]*'|"[^"\n\r]*"|[^ \t\n\r'"]*)[ \t]+/;
