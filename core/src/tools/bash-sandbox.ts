import { execSync } from 'child_process';
import { wildcardMatch } from '../permissions';

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
  // allowlist even if it also matches an allowed entry. Mirrors Claude's
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
  // active. Mirrors Claude's `SandboxNetworkConfig.allowUnixSockets: string[]`:
  // granular control instead of the all-or-nothing `allowUnixSockets` boolean.
  // A path listed here is removed from the default-denied set. Additive:
  // undefined/empty keeps every default socket denied.
  allowedUnixSockets?: string[];
  // PARITY (filesystem read-deny): absolute paths that must be UNreadable inside
  // the sandbox. Each is bind-mounted with /dev/null so a read returns empty,
  // using the same technique as credential file-hiding. Mirrors Claude's
  // `SandboxFilesystemConfig.denyRead`. Additive: undefined/empty is a no-op.
  denyReadPaths?: string[];
  // PARITY (filesystem write-deny): absolute paths that must be read-only inside
  // the sandbox even if they would otherwise be writable. Each is bind-remounted
  // read-only, the same technique as `readOnlyPaths`. Mirrors Claude's
  // `SandboxFilesystemConfig.denyWrite`. Additive: undefined/empty is a no-op.
  denyWritePaths?: string[];
  // PARITY (config-rewrite / privilege-escalation escape): when true, the
  // sandbox bind-remounts the agent's own settings/config files AND the
  // auto-loaded skills/commands/agents directories (under the command's cwd)
  // read-only, so a sandboxed command cannot rewrite the trusted config to
  // weaken the sandbox, nor plant an auto-discovered skill/command/agent that
  // would later run with full capabilities. Mirrors Claude Code's
  // sandbox-adapter unconditional `denyWrite` of settings.json files and
  // `.claude/skills`/commands/agents. Additive: undefined/false is a no-op.
  denyConfigWrites?: boolean;
  // PARITY (bare-repo .git-file poisoning defense): when true, the sandbox
  // bind-remounts read-only the files git's `is_git_directory()` keys on
  // (HEAD, objects, refs, hooks, config) under cwd, so a sandboxed command
  // cannot alter a planted/pre-existing bare-repo marker (e.g. a `config` with
  // `core.fsmonitor`) that a later UNsandboxed `git` run would execute. Mirrors
  // Claude Code's sandbox-adapter bare-repo guard. Additive: undefined/false is
  // a no-op.
  denyBareGitPoisoning?: boolean;
  // PARITY (fail-closed hard gate): when true, an unavailable sandbox is a
  // FATAL startup error rather than a silent degrade-to-unsandboxed. Consumed
  // by callers via {@link validateSandboxEnabled}'s `fatal` flag. Intended for
  // managed deployments that require sandboxing. Mirrors Claude Code's
  // `SandboxSettings.failIfUnavailable`. Additive: undefined/false preserves the
  // warn-and-run-unsandboxed default.
  failIfUnavailable?: boolean;
}

export function getDefaultMemoryMB(): number {
  const env = process.env.AMI_TOOL_MEMORY_LIMIT_MB;
  if (env) {
    const n = parseInt(env, 10);
    if (n >= 64 && n <= 65536) return n;
  }
  return 512;
}

const DEFAULT_SANDBOX_CONFIG: SandboxConfig = {
  allowNetwork: false,
  readOnlyPaths: ['/usr', '/bin', '/lib', '/lib64', '/etc'],
  writablePaths: ['/tmp'],
  maxMemoryMB: getDefaultMemoryMB(),
  maxProcesses: 8192,
  timeout: 30000,
};

let _unshareAvailable: boolean | null = null;

function isUnshareAvailable(): boolean {
  if (_unshareAvailable !== null) return _unshareAvailable;
  try {
    execSync('unshare --map-root-user --mount --pid --fork -- true', { stdio: 'pipe', timeout: 5000 });
    _unshareAvailable = true;
  } catch {
    _unshareAvailable = false;
  }
  return _unshareAvailable;
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
function isCommandAvailable(name: string): boolean {
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
 * Claude Code's `SandboxManager.checkDependencies()` → `{ errors, warnings }`):
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
 * Mirrors Claude Code's `checkDependencies()` two-tier contract and closes the
 * "security control silently unenforced" footgun: `buildNetworkAllowlist`
 * swallows iptables failures with `2>/dev/null`, so without this warning a user
 * who set `allowedDomains` on a host lacking iptables would believe outbound
 * traffic is restricted when it is wide open. Additive + pure: no cfg → only the
 * platform/unshare errors; the degradation warnings require the relevant cfg.
 */
export function checkSandboxDependencies(cfg?: Partial<SandboxConfig>): SandboxDependencyCheck {
  const errors: string[] = [];
  const warnings: string[] = [];

  // --- namespace isolation availability (the real filesystem/network sandbox) ---
  if (process.platform !== 'linux') {
    errors.push(
      `Namespace sandbox requires Linux; current platform is ${process.platform}. ` +
        'Only resource limits (ulimit) are applied — filesystem and network isolation are NOT enforced.',
    );
  } else if (!isUnshareAvailable()) {
    // Distinguish "not installed" from "installed but user namespaces blocked" so
    // the remediation hint names the actual missing capability.
    if (!isCommandAvailable('unshare')) {
      errors.push(
        'Namespace sandbox dependency `unshare` is not installed. Install util-linux ' +
          '(Debian/Ubuntu: `apt-get install util-linux`; Fedora/RHEL: `dnf install util-linux`; ' +
          'Alpine: `apk add util-linux`).',
      );
    } else {
      errors.push(
        'Namespace sandbox dependency `unshare` is present but could not create a user namespace ' +
          '(unprivileged user namespaces are likely disabled). Enable them with ' +
          '`sysctl -w kernel.unprivileged_userns_clone=1`, or set sandbox.enabled to false.',
      );
    }
  }

  // --- capability-degradation warnings for the REQUESTED config. These apply
  //     even when namespaces work, because the allow/deny list is realized by
  //     iptables + getent INSIDE the namespace. ---
  const wantsAllowlist =
    !!cfg && cfg.allowNetwork === false && !!cfg.allowedDomains && cfg.allowedDomains.length > 0;
  const wantsDenylist = !!cfg?.deniedDomains && cfg.deniedDomains.length > 0;
  if (wantsAllowlist && !isCommandAvailable('iptables')) {
    warnings.push(
      'allowedDomains is configured but `iptables` is not available; the outbound network ' +
        'allowlist will NOT be enforced — all outbound traffic is permitted. Install iptables ' +
        '(Debian/Ubuntu: `apt-get install iptables`; Fedora/RHEL: `dnf install iptables`).',
    );
  }
  if ((wantsAllowlist || wantsDenylist) && !isCommandAvailable('getent')) {
    warnings.push(
      'A network allow/deny domain list is configured but `getent` (glibc name resolution) is ' +
        'not available; configured domains cannot be resolved to IPs and the rules will not match. ' +
        'Install glibc tools (Debian/Ubuntu: `apt-get install libc-bin`).',
    );
  }

  return { errors, warnings };
}

const SANDBOX_TRIGGER_PATTERNS = [
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

const SANDBOX_EXEMPT_PATTERNS = [
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
 * Exemption matching mirrors Claude Code's `containsExcludedCommand`: each
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
 * Claude Code's `BINARY_HIJACK_VARS`.
 */
const BINARY_HIJACK_VARS = /^(LD_|DYLD_|PATH$)/;

// A leading `NAME=value ` assignment. Values are restricted to quoted strings or
// a run of non-whitespace/non-quote chars (shell metacharacters that could
// change what runs are intentionally NOT consumed). Horizontal whitespace only
// (`[ \t]`): a newline is a command separator, not an assignment terminator.
const LEADING_ENV_ASSIGN_RE =
  /^([A-Za-z_][A-Za-z0-9_]*)=(?:'[^'\n\r]*'|"[^"\n\r]*"|[^ \t\n\r'"]*)[ \t]+/;

/**
 * Strip ONE leading env-var assignment from a command unless the variable is a
 * binary-hijack var (which stops the stripping). Returns the stripped command,
 * or null when nothing could be stripped.
 */
function stripOneLeadingEnvVar(cmd: string): string | null {
  const m = cmd.match(LEADING_ENV_ASSIGN_RE);
  if (!m) return null;
  if (BINARY_HIJACK_VARS.test(m[1]!)) return null;
  return cmd.slice(m[0].length);
}

/**
 * Safe command wrappers whose leading occurrence can be transparently stripped
 * before exemption matching so `timeout 300 bazel run` still matches a `bazel:*`
 * exemption. Mirrors Claude Code's `stripSafeWrappers` safe-list — deliberately
 * EXCLUDING `sudo`/`env`/`bash -c`/`eval`, which run arbitrary code and must
 * never be transparently stripped (doing so would let them inherit an exemption
 * meant for the inner command).
 */
const SAFE_WRAPPER_PATTERNS: RegExp[] = [
  // timeout [FLAGS] DURATION ...  (flags: -k 5, -s TERM, --preserve-status, …)
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, flag/duration tokens are disjoint
  /^timeout[ \t]+(?:(?:--[a-z-]+(?:=[A-Za-z0-9_.+-]+)?|-[A-Za-z][ \t]*[A-Za-z0-9_.+-]*)[ \t]+)*\d+(?:\.\d+)?[smhd]?[ \t]+/,
  /^time[ \t]+/,
  // nice forms: `nice cmd`, `nice -10 cmd`, `nice -n 10 cmd`. Alternatives start
  // with `-` (not whitespace) so there is no overlap with the leading `[ \t]+`.
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, bounded; alternatives start with `-`
  /^nice[ \t]+(?:-n[ \t]+-?\d+[ \t]+|-\d+[ \t]+)?/,
  /^nohup[ \t]+/,
  // stdbuf forms: `stdbuf -o0 cmd`, `stdbuf -o0 -eL cmd`. Each repeated group must
  // begin with `-[ioe]`, so backtracking is bounded by the flag count (no ReDoS).
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, each iteration starts with `-[ioe]`
  /^stdbuf[ \t]+(?:-[ioe][A-Za-z0-9]+[ \t]+)+/,
];

/**
 * Strip ONE leading safe wrapper from a command. Returns the stripped command,
 * or null when no wrapper matched.
 */
function stripOneSafeWrapper(cmd: string): string | null {
  for (const p of SAFE_WRAPPER_PATTERNS) {
    const stripped = cmd.replace(p, '');
    if (stripped !== cmd) return stripped;
  }
  return null;
}

/**
 * Build the fixed-point candidate set for a subcommand by iteratively stripping
 * leading env-var assignments and safe wrappers until no new form is produced.
 * This handles interleaved patterns like `timeout 300 FOO=bar bazel run`
 * (mirrors Claude Code's fixed-point candidate generation).
 */
function buildExemptionCandidates(subcommand: string): string[] {
  const start = subcommand.trim();
  const candidates = [start];
  const seen = new Set(candidates);
  for (let i = 0; i < candidates.length; i++) {
    const cur = candidates[i]!;
    for (const next of [stripOneLeadingEnvVar(cur), stripOneSafeWrapper(cur)]) {
      if (next === null) continue;
      const t = next.trim();
      if (!seen.has(t)) {
        seen.add(t);
        candidates.push(t);
      }
    }
  }
  return candidates;
}

/**
 * A parsed user `excludedCommands` rule. `prefix` matches the base command and
 * its args; `wildcard` matches via the canonical glob matcher.
 */
type ExcludedRule =
  | { type: 'prefix'; prefix: string }
  | { type: 'wildcard'; pattern: string };

/**
 * True when `pattern` contains an UNescaped `*` (a real wildcard), ignoring the
 * legacy `:*` prefix suffix and `\*` escapes. Mirrors Claude Code's
 * `hasWildcards`.
 */
function hasUnescapedWildcard(pattern: string): boolean {
  if (pattern.endsWith(':*')) return false;
  for (let i = 0; i < pattern.length; i++) {
    if (pattern[i] !== '*') continue;
    let backslashes = 0;
    for (let j = i - 1; j >= 0 && pattern[j] === '\\'; j--) backslashes++;
    if (backslashes % 2 === 0) return true;
  }
  return false;
}

/**
 * Parse one `excludedCommands` entry into a structured rule (mirrors Claude
 * Code's `bashPermissionRule`). A `foo:*` legacy suffix and a bare token both
 * become PREFIX rules (AMI's additive-prefix convenience — a prefix rule also
 * matches the exact command); any other unescaped `*` becomes a WILDCARD rule.
 */
function parseExcludedRule(pattern: string): ExcludedRule {
  const trimmed = pattern.trim();
  if (trimmed.endsWith(':*')) {
    return { type: 'prefix', prefix: trimmed.slice(0, -2).trim() };
  }
  if (hasUnescapedWildcard(trimmed)) {
    return { type: 'wildcard', pattern: trimmed };
  }
  return { type: 'prefix', prefix: trimmed };
}

/**
 * Compile the user `excludedCommands` into structured rules, dropping
 * empty/whitespace-only entries.
 */
export function buildExcludedRules(options?: SandboxExemptionOptions): ExcludedRule[] {
  return (options?.excludedCommands ?? [])
    .map(c => c.trim())
    .filter(c => c.length > 0)
    .map(parseExcludedRule);
}

/** Match a single candidate command against one structured rule. */
function matchesExcludedRule(rule: ExcludedRule, cand: string): boolean {
  if (rule.type === 'prefix') {
    if (rule.prefix.length === 0) return false;
    return cand === rule.prefix || cand.startsWith(rule.prefix + ' ');
  }
  return wildcardMatch(rule.pattern, cand);
}

/**
 * True when a subcommand (after fixed-point env-var / wrapper normalization)
 * matches ANY user `excludedCommands` rule — i.e. the operator asked to exempt
 * it from the sandbox.
 */
function commandIsExcluded(subcommand: string, rules: ExcludedRule[]): boolean {
  if (rules.length === 0) return false;
  const candidates = buildExemptionCandidates(subcommand);
  for (const rule of rules) {
    for (const cand of candidates) {
      if (matchesExcludedRule(rule, cand)) return true;
    }
  }
  return false;
}

/**
 * Split a command line into its top-level subcommands on shell control
 * operators (`&&`, `||`, `;`, `|`, `&`, newline) WITHOUT splitting inside
 * single/double quotes. Each subcommand is evaluated independently by
 * `shouldUseSandbox` so a compound command cannot escape the sandbox merely
 * because ONE of its subcommands is exempt (e.g. the `curl` in
 * `git pull && curl https://evil` must still be sandboxed even though `git` is
 * exempt). Mirrors the per-subcommand matching the permission layer already does
 * for command chaining.
 */
function splitTopLevelSubcommands(command: string): string[] {
  const parts: string[] = [];
  let cur = '';
  let quote: string | null = null;
  for (let i = 0; i < command.length; i++) {
    const ch = command[i];
    if (quote) {
      cur += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      cur += ch;
      continue;
    }
    const two = command.slice(i, i + 2);
    if (two === '&&' || two === '||') {
      parts.push(cur);
      cur = '';
      i++;
      continue;
    }
    if (ch === ';' || ch === '|' || ch === '&' || ch === '\n') {
      parts.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  parts.push(cur);
  return parts.map(p => p.trim()).filter(p => p.length > 0);
}

/**
 * Strip URL arguments (`scheme://...`) from a subcommand before exemption
 * matching. An exempt keyword appearing inside a URL/arg handed to a network
 * trigger (e.g. `curl https://git.example.com/x` or
 * `curl https://raw.githubusercontent.com/npm/pkg`) must NOT flip the exemption
 * decision — only the command itself may. Attacker-influenced URLs/args can
 * otherwise defeat the sandbox trigger.
 */
function stripUrlArgs(s: string): string {
  return s.replace(/\b[a-z][a-z0-9+.-]*:\/\/\S+/gi, '');
}

export function shouldUseSandbox(command: string, options?: SandboxExemptionOptions): boolean {
  const defaultPatterns = buildSandboxExemptPatterns(options);
  const userRules = buildExcludedRules(options);
  // Evaluate each top-level subcommand independently. A compound command is
  // sandboxed if ANY of its subcommands is a trigger that is not itself exempt,
  // so an exempt subcommand (git/npm/test-runner) can never suppress the
  // sandbox for a sibling trigger subcommand (the compound-escape vector).
  for (const sub of splitTopLevelSubcommands(command)) {
    // Hardcoded defaults match on the subcommand with URL args removed, so an
    // exempt keyword inside a trigger's URL cannot exempt it. Quotes are KEPT
    // here so test runners hidden in a quoted argument (`bash -c "pytest ..."`,
    // `python tests/runtests.py`) remain exempt — AMI's deliberate superset.
    const forExempt = stripUrlArgs(sub);
    if (defaultPatterns.some(p => p.test(forExempt))) continue;
    // User-configured excludedCommands are matched as structured prefix/wildcard
    // rules anchored to the base command of the (env/wrapper-normalized)
    // subcommand — NOT a loose substring — so an exempt keyword appearing as an
    // argument or inside another command can never flip the exemption.
    if (commandIsExcluded(sub, userRules)) continue;
    // Triggers match on the quote-stripped subcommand so trigger text inside
    // quotes (`echo "curl is a tool"`) does not force the sandbox.
    const stripped = sub.replace(/"[^"]*"|'[^']*'/g, '');
    if (SANDBOX_TRIGGER_PATTERNS.some(p => p.test(stripped))) return true;
  }
  return false;
}

export function wrapWithSandbox(
  command: string,
  cwd: string,
  config: Partial<SandboxConfig> = {},
): string {
  const cfg = { ...DEFAULT_SANDBOX_CONFIG, ...config };

  if (process.platform !== 'linux') {
    return wrapWithResourceLimits(command, cfg);
  }

  if (isUnshareAvailable()) {
    return wrapWithUnshare(command, cwd, cfg);
  }

  return wrapWithResourceLimits(command, cfg);
}

export function wrapWithUnshare(command: string, cwd: string, cfg: SandboxConfig): string {
  const flags: string[] = ['--map-root-user', '--mount', '--pid', '--fork'];

  const hasAllowedDomains =
    cfg.allowedDomains && cfg.allowedDomains.length > 0;

  if (!cfg.allowNetwork) {
    // Always create a network namespace. When allowedDomains is set we keep
    // --net so we get our own namespace but add iptables rules inside it
    // instead of blanket-blocking all traffic.
    flags.push('--net');
  }

  const ulimits = buildUlimits(cfg);
  const escapedCmd = command.replace(/'/g, "'\\''");

  const escapedCwd = cwd.replace(/'/g, "'\\''");
  const mounts = buildReadOnlyMounts(cfg);
  const writeDenials = buildWriteDenials(cfg);
  const configGuard = buildConfigWriteGuard(cwd, cfg);
  const gitGuard = buildBareGitRepoGuard(cwd, cfg);
  const readDenials = buildReadDenials(cfg);
  const credentialScrub = buildCredentialScrub(cfg);
  const socketDenial = buildSocketDenial(cfg);
  // Deny rules are emitted (and thus iptables-appended) BEFORE the allowlist's
  // accepts/default-drop, so a denied host is DROPped first → deny wins over allow.
  const networkRules =
    !cfg.allowNetwork && hasAllowedDomains
      ? buildDeniedDomainRules(cfg.deniedDomains) + buildNetworkAllowlist(cfg.allowedDomains!)
      : '';
  return `unshare ${flags.join(' ')} -- bash -c '${ulimits}${mounts}${writeDenials}${configGuard}${gitGuard}${readDenials}${credentialScrub}${socketDenial}${networkRules}cd ${escapedCwd} && ${escapedCmd}'`;
}

/** Join a sandbox cwd and a relative path, normalizing a trailing slash so the
 * segment never contains a doubled separator. */
function joinUnderCwd(cwd: string, rel: string): string {
  return cwd.replace(/\/+$/, '') + '/' + rel;
}

/**
 * Bind-remount each path onto itself read-only, but ONLY when it exists — a
 * missing path is skipped so no 0-byte stub is left behind (which would both
 * pollute the host and break tools like git). Reuses the exact bind+remount
 * technique as {@link buildReadOnlyMounts} / {@link buildWriteDenials}; every
 * step is best-effort (`2>/dev/null`). Shared by the config-write and bare-git
 * guards so the ro-bind logic exists in one place (No-Duplication Rule).
 */
function roBindExistingPaths(paths: string[]): string {
  return paths
    .map((p) => {
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `if [ -e ${esc} ]; then mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; fi; `;
    })
    .join('');
}

/**
 * Relative paths (under the command's cwd) that, if writable inside the
 * sandbox, let a sandboxed command rewrite the agent's own trusted config
 * (settings-rewrite sandbox escape) or plant an auto-discovered skill / command
 * / agent that would later run with full capabilities (privilege escalation).
 * Covers both the Claude-compatible `.claude/` tree and AMI's `.ami/` tree.
 */
const CONFIG_ESCAPE_RELPATHS = [
  '.claude/settings.json',
  '.claude/settings.local.json',
  '.ami/config.json',
  '.ami/settings.json',
  '.ami/settings.local.json',
  '.claude/skills',
  '.claude/commands',
  '.claude/agents',
  '.ami/skills',
  '.ami/commands',
  '.ami/agents',
];

/**
 * PARITY (config-rewrite / privilege-escalation escape): ro-bind the agent's own
 * settings/config files and auto-loaded skills/commands/agents directories
 * (under cwd) so a sandboxed command can neither weaken the sandbox by editing
 * trusted config nor plant an auto-loaded skill/command/agent. Returns '' when
 * `denyConfigWrites` is off. Mirrors Claude Code's sandbox-adapter unconditional
 * `denyWrite` of settings files + `.claude/skills`/commands/agents.
 */
export function buildConfigWriteGuard(cwd: string, cfg: SandboxConfig): string {
  if (!cfg.denyConfigWrites) return '';
  return roBindExistingPaths(CONFIG_ESCAPE_RELPATHS.map((rel) => joinUnderCwd(cwd, rel)));
}

/**
 * The files git's `is_git_directory()` keys on: a dir containing HEAD +
 * objects/ + refs/ is treated as a (bare) repo, and a planted `config`/`hooks`
 * can run code when an UNsandboxed git later runs there.
 */
export const BARE_GIT_REPO_FILES = ['HEAD', 'objects', 'refs', 'hooks', 'config'] as const;

/**
 * PARITY (bare-repo .git-file poisoning defense): ro-bind any existing bare-repo
 * marker files under cwd so a sandboxed command cannot modify a planted/pre-
 * existing HEAD/objects/refs/hooks/config that a later unsandboxed `git` would
 * honor. Returns '' when `denyBareGitPoisoning` is off. Existence-guarded so a
 * non-existent marker is never stubbed (matching Claude Code's avoidance of
 * 0-byte stubs that break `git`). Mirrors Claude Code's sandbox-adapter guard.
 */
export function buildBareGitRepoGuard(cwd: string, cfg: SandboxConfig): string {
  if (!cfg.denyBareGitPoisoning) return '';
  return roBindExistingPaths(BARE_GIT_REPO_FILES.map((f) => joinUnderCwd(cwd, f)));
}

/**
 * Realize the advertised readOnlyPaths inside the --mount namespace: bind each
 * path onto itself and remount it read-only so a sandboxed command cannot write
 * to it. Failures (e.g. a path that does not exist on this host) are tolerated
 * so they never abort the user's command. Returns '' when no readOnlyPaths are
 * configured. Runs before cd/command.
 */
function buildReadOnlyMounts(cfg: SandboxConfig): string {
  return cfg.readOnlyPaths
    .map((p) => {
      // escapeShell single-quotes the path; re-escape those quotes so it is safe
      // to embed inside the already single-quoted `bash -c '...'` body.
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; `;
    })
    .join('');
}

/**
 * PARITY (filesystem write-deny): bind-remount each `denyWritePaths` entry
 * read-only inside the mount namespace so it cannot be written even if it would
 * otherwise be writable. Reuses the exact bind+remount technique as
 * {@link buildReadOnlyMounts}; failures are tolerated (`2>/dev/null`). Returns ''
 * when no denyWritePaths are configured. Mirrors Claude's
 * `SandboxFilesystemConfig.denyWrite`.
 */
export function buildWriteDenials(cfg: SandboxConfig): string {
  if (!cfg.denyWritePaths || cfg.denyWritePaths.length === 0) return '';
  return cfg.denyWritePaths
    .map((p) => {
      const esc = escapeShell(p).replace(/'/g, "'\\''");
      return `mount --bind ${esc} ${esc} 2>/dev/null && mount -o remount,ro,bind ${esc} 2>/dev/null; `;
    })
    .join('');
}

/**
 * PARITY (filesystem read-deny): bind /dev/null over each `denyReadPaths` entry
 * so a read inside the sandbox returns empty rather than the real contents.
 * Reuses the same /dev/null-bind technique as the credential file-hiding path;
 * each bind is best-effort (`2>/dev/null`). Returns '' when none are configured.
 * Mirrors Claude's `SandboxFilesystemConfig.denyRead`.
 */
export function buildReadDenials(cfg: SandboxConfig): string {
  if (!cfg.denyReadPaths || cfg.denyReadPaths.length === 0) return '';
  return cfg.denyReadPaths
    .map((p) => {
      const esc = p.replace(/'/g, "'\\''");
      return `mount --bind /dev/null '${esc}' 2>/dev/null; `;
    })
    .join('');
}

/**
 * Build shell commands to scrub sensitive credentials from the sandbox
 * environment. Unsets listed env vars and bind-mounts /dev/null over listed
 * files so their contents are invisible inside the namespace.
 */
export function buildCredentialScrub(cfg: SandboxConfig): string {
  let cmds = '';
  if (cfg.hiddenEnvVars && cfg.hiddenEnvVars.length > 0) {
    cmds += 'unset ' + cfg.hiddenEnvVars.join(' ') + '; ';
  }
  if (cfg.hiddenFiles && cfg.hiddenFiles.length > 0) {
    for (const f of cfg.hiddenFiles) {
      const esc = f.replace(/'/g, "'\\''");
      cmds += "mount --bind /dev/null '" + esc + "' 2>/dev/null; ";
    }
  }
  return cmds;
}

/**
 * Unix-domain sockets that grant control of a privileged host daemon and are a
 * well-known sandbox-escape vector: a sandboxed command that can talk to the
 * Docker/containerd socket can launch a privileged container and break out
 * entirely (`curl --unix-socket /var/run/docker.sock ...`). The auto-sandbox
 * only wraps network/exec triggers (curl, wget, interpreters), which never
 * legitimately need these sockets, so by default we bind /dev/null over them
 * inside the private mount namespace. The denial is confined to the sandbox's
 * own namespace and never affects the host.
 */
const DEFAULT_DENIED_SOCKETS = [
  '/var/run/docker.sock',
  '/run/docker.sock',
  '/var/run/containerd/containerd.sock',
  '/run/containerd/containerd.sock',
];

/**
 * Build shell commands that bind /dev/null over privileged daemon sockets so a
 * sandboxed command cannot connect to them. Returns '' when the operator has
 * opted out via `allowUnixSockets`. Each bind is best-effort (`2>/dev/null`) so
 * a socket that does not exist on this host is silently skipped. Reuses the same
 * /dev/null-bind technique as the credential file-hiding path.
 */
export function buildSocketDenial(cfg: SandboxConfig): string {
  if (cfg.allowUnixSockets) return '';
  // PARITY (per-path socket allow): re-allow any default-denied socket whose path
  // the operator explicitly listed in `allowedUnixSockets`, leaving the rest
  // denied — Claude's granular `allowUnixSockets: string[]` instead of the
  // all-or-nothing boolean. Normalize whitespace so a padded entry still matches.
  const allowed = new Set(
    (cfg.allowedUnixSockets ?? []).map(s => s.trim()).filter(s => s.length > 0),
  );
  return DEFAULT_DENIED_SOCKETS.filter(s => !allowed.has(s))
    .map(s => `mount --bind /dev/null '${s}' 2>/dev/null; `)
    .join('');
}

/**
 * PARITY (host denylist): build iptables DROP rules for each denied domain's
 * resolved IPs. Returns '' for an empty/undefined list. These rules are emitted
 * (and iptables-appended) BEFORE {@link buildNetworkAllowlist}'s ACCEPT rules so
 * a denied host is dropped first even if it also appears in / overlaps the
 * allowlist — "deny wins over allow". Trailing dots are stripped (FQDN
 * normalization), matching the allowlist builder. Mirrors Claude's
 * `SandboxNetworkConfig.deniedHosts`.
 */
export function buildDeniedDomainRules(domains?: string[]): string {
  if (!domains || domains.length === 0) return '';
  let rules = '';
  for (const raw of domains) {
    const d = raw.replace(/\.+$/, '');
    const esc = d.replace(/'/g, "'\\''");
    rules +=
      "for ip in $(getent ahosts '" +
      esc +
      "' 2>/dev/null | awk '{print $1}' | sort -u); do iptables -A OUTPUT -d $ip -j DROP 2>/dev/null; done; ";
  }
  return rules;
}

/**
 * Build iptables rules that allow outbound traffic only to the listed domains
 * (plus loopback and DNS). Returns '' for an empty list. Meant to run inside
 * a network namespace created by unshare --net.
 */
export function buildNetworkAllowlist(domains: string[]): string {
  if (domains.length === 0) return '';
  let rules = '';
  // Allow loopback always
  rules += 'iptables -A OUTPUT -o lo -j ACCEPT 2>/dev/null; ';
  // Allow DNS
  rules += 'iptables -A OUTPUT -p udp --dport 53 -j ACCEPT 2>/dev/null; ';
  rules += 'iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT 2>/dev/null; ';
  // Allow each domain — strip trailing dots (FQDN normalization)
  for (const raw of domains) {
    const d = raw.replace(/\.+$/, '');
    const esc = d.replace(/'/g, "'\\''");
    rules +=
      "for ip in $(getent ahosts '" +
      esc +
      "' 2>/dev/null | awk '{print $1}' | sort -u); do iptables -A OUTPUT -d $ip -j ACCEPT 2>/dev/null; done; ";
  }
  // Default deny outbound
  rules += 'iptables -A OUTPUT -j DROP 2>/dev/null; ';
  return rules;
}

function wrapWithResourceLimits(command: string, cfg: SandboxConfig): string {
  // In resource-limits-only mode (non-Linux or no unshare), we can still hide
  // env vars (no mount namespace available, so file hiding is skipped).
  let envScrub = '';
  if (cfg.hiddenEnvVars && cfg.hiddenEnvVars.length > 0) {
    envScrub = 'unset ' + cfg.hiddenEnvVars.join(' ') + '; ';
  }
  return `${buildUlimits(cfg)}${envScrub}${command}`;
}

function buildUlimits(cfg: SandboxConfig): string {
  const limits: string[] = [];
  const memKB = cfg.maxMemoryMB * 1024;
  limits.push(`ulimit -v ${memKB} 2>/dev/null`);
  limits.push(`ulimit -u ${cfg.maxProcesses} 2>/dev/null`);
  limits.push(`ulimit -f 104857600 2>/dev/null`);
  return limits.join('; ') + '; ';
}

function escapeShell(s: string): string {
  return "'" + s.replace(/'/g, "'\\''") + "'";
}

/**
 * Report the host-side isolation capability plus a `warnings` channel surfacing
 * any degraded/unenforced controls (parity with Claude's dependency-aware status).
 * `available` stays true (AMI always has at least the ulimits fallback). When the
 * namespace sandbox cannot run, its diagnostic is folded into `warnings` (the
 * ulimits fallback still runs, so from the user's view it's a degradation, not a
 * hard error). When a `cfg` with an allow/deny domain list is supplied, a missing
 * `iptables`/`getent` is reported here too — so `/sandbox` can tell the user their
 * `allowedDomains` policy is silently a no-op instead of hiding it behind `2>/dev/null`.
 */
export function getSandboxStatus(
  cfg?: Partial<SandboxConfig>,
): { available: boolean; method: string; warnings: string[] } {
  const dep = checkSandboxDependencies(cfg);
  const nsAvailable = process.platform === 'linux' && isUnshareAvailable();
  const method = nsAvailable ? 'unshare' : 'ulimits';
  // On the ulimits fallback the namespace "errors" are a user-facing degradation
  // notice (AMI still runs, just with weaker isolation), so fold them in.
  const warnings = nsAvailable ? dep.warnings : [...dep.errors, ...dep.warnings];
  return { available: true, method, warnings };
}

export interface SandboxValidation {
  valid: boolean;
  error?: string;
  method?: string;
  // PARITY (fail-closed gate): when the sandbox is NOT valid, `fatal` tells the
  // caller whether to abort (true → the `failIfUnavailable` hard gate was set)
  // or degrade to running unsandboxed with a warning (false → the default).
  // Always false/absent on the valid path. Mirrors Claude Code's
  // `SandboxSettings.failIfUnavailable` semantics.
  fatal?: boolean;
}

/**
 * Validate that the host bash sandbox can actually start. When it cannot, the
 * `fatal` flag reflects the caller's `failIfUnavailable` policy: a managed
 * deployment that passes `{ failIfUnavailable: true }` gets `fatal: true` (abort
 * at startup) while the default gets `fatal: false` (warn + run unsandboxed).
 */
export function validateSandboxEnabled(opts?: { failIfUnavailable?: boolean }): SandboxValidation {
  const fatal = !!opts?.failIfUnavailable;
  // Reuse the structured dependency check so the error ENUMERATES the specific
  // missing dependency (unshare not installed vs. present-but-userns-disabled vs.
  // non-Linux platform) instead of one generic "not available or lacks
  // permissions" string that conflates distinct causes.
  const { errors } = checkSandboxDependencies();
  if (errors.length > 0) {
    return { valid: false, fatal, error: errors.join(' ') };
  }
  return { valid: true, method: 'unshare' };
}

export function resetSandboxCache(): void {
  _unshareAvailable = null;
}
