// Split from bash-sandbox.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { homedir } from 'os';
import { isAbsolute, resolve as resolvePath } from 'path';
import { wildcardMatch } from '../permissions';
import type { SandboxPolicy } from '../types';
import { BINARY_HIJACK_VARS, LEADING_ENV_ASSIGN_RE, SANDBOX_TRIGGER_PATTERNS, buildSandboxExemptPatterns } from './bash-sandbox-sandbox-config';
import type { SandboxConfig, SandboxExemptionOptions } from './bash-sandbox-sandbox-config';

/**
 * Strip ONE leading env-var assignment from a command unless the variable is a
 * binary-hijack var (which stops the stripping). Returns the stripped command,
 * or null when nothing could be stripped.
 */
export function stripOneLeadingEnvVar(cmd: string): string | null {
  const m = cmd.match(LEADING_ENV_ASSIGN_RE);
  if (!m) return null;
  if (BINARY_HIJACK_VARS.test(m[1]!)) return null;
  return cmd.slice(m[0].length);
}

/**
 * Safe command wrappers whose leading occurrence can be transparently stripped
 * before exemption matching so `timeout 300 bazel run` still matches a `bazel:*`
 * exemption. Mirrors the reference's `stripSafeWrappers` safe-list — deliberately
 * EXCLUDING `sudo`/`env`/`bash -c`/`eval`, which run arbitrary code and must
 * never be transparently stripped (doing so would let them inherit an exemption
 * meant for the inner command).
 */
export const SAFE_WRAPPER_PATTERNS: RegExp[] = [
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
export function stripOneSafeWrapper(cmd: string): string | null {
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
 * (mirrors the reference's fixed-point candidate generation).
 */
export function buildExemptionCandidates(subcommand: string): string[] {
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
export type ExcludedRule =
  | { type: 'prefix'; prefix: string }
  | { type: 'wildcard'; pattern: string };

/**
 * True when `pattern` contains an UNescaped `*` (a real wildcard), ignoring the
 * legacy `:*` prefix suffix and `\*` escapes. Mirrors the reference's
 * `hasWildcards`.
 */
export function hasUnescapedWildcard(pattern: string): boolean {
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
 * Parse one `excludedCommands` entry into a structured rule (mirrors the reference
 * Code's `bashPermissionRule`). A `foo:*` legacy suffix and a bare token both
 * become PREFIX rules (AMI's additive-prefix convenience — a prefix rule also
 * matches the exact command); any other unescaped `*` becomes a WILDCARD rule.
 */
export function parseExcludedRule(pattern: string): ExcludedRule {
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
export function matchesExcludedRule(rule: ExcludedRule, cand: string): boolean {
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
export function commandIsExcluded(subcommand: string, rules: ExcludedRule[]): boolean {
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
export function splitTopLevelSubcommands(command: string): string[] {
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
export function stripUrlArgs(s: string): string {
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
    // PARITY (default-deny coverage): once a subcommand survived both exemption
    // checks above, the reference sandboxes it unconditionally — there is no trigger
    // gate. When `defaultSandbox` is on we do the same, so any non-exempt command
    // (`./evil`, `nc`, `ssh`, a path-invoked script, `> /etc/…`) is sandboxed
    // instead of slipping through the narrow trigger allowlist. Per-subcommand, so
    // one exempt subcommand still cannot suppress the sandbox for a sibling
    // (AMI's stricter superset over the reference's whole-command check is preserved).
    if (options?.defaultSandbox) return true;
    // Triggers match on the quote-stripped subcommand so trigger text inside
    // quotes (`echo "curl is a tool"`) does not force the sandbox.
    const stripped = sub.replace(/"[^"]*"|'[^']*'/g, '');
    if (SANDBOX_TRIGGER_PATTERNS.some(p => p.test(stripped))) return true;
  }
  return false;
}

/** Inputs that decide whether a command WILL actually be wrapped by the HOST
 *  (unshare) auto-sandbox. Mirrors the host-path gating in `bashTool` so the
 *  decision has a SINGLE canonical home (consumed by both `bash.ts` and the
 *  permission layer's `autoAllowBashIfSandboxed` host-mode fallback). */
export interface HostSandboxEligibilityOptions {
  /** Operator `sandboxExcludedCommands` carve-outs (threaded from the tool context). */
  excludedCommands?: string[];
  /** `SandboxPolicy.enabledPlatforms` — restrict host sandboxing to given platforms. */
  enabledPlatforms?: string[];
  /** `SandboxPolicy.enabled` — exactly `false` is the config-level host off-switch. */
  enabled?: boolean;
  /** Env (for the `AMI_DISABLE_SANDBOX`/default-deny gates). Defaults to `process.env`. */
  env?: NodeJS.ProcessEnv;
  /** Platform override for tests. Defaults to `process.platform`. */
  platform?: string;
  /** Injectable `shouldUseSandbox` impl so callers that already expose a test seam
   *  (e.g. `bash.ts`'s `_bashHooks.shouldUseSandbox`) keep driving one decision. */
  shouldUseSandboxImpl?: typeof shouldUseSandbox;
}

/**
 * PARITY (CC-specific sandbox fs path conventions — the reference's
 * `resolveSandboxFilesystemPath`): normalize one `sandbox.filesystem.*` path entry
 * the way the reference resolves `allowWrite`/`denyWrite`/`allowRead`/`denyRead`
 * settings values, so an operator's path works as written rather than being
 * silently mis-matched by the literal bind-mount sandbox:
 *   - `//path` → `/path`  (legacy permission-rule escape, accepted for compat),
 *   - `~` or `~/path` → expanded to the user's home directory,
 *   - a RELATIVE path (`./x`, `x`) → resolved against `cwd` (AMI's settings/project
 *     root) when a `cwd` is supplied; left as-is when it is not,
 *   - an already-absolute `/path` → returned unchanged (as-written, NOT
 *     settings-relative; matches the reference's #30067 fix).
 * Pure; absolute inputs are returned verbatim so a policy of absolute paths maps
 * identically to the pre-normalization behavior (fully additive).
 */
export function normalizeSandboxFsPath(pattern: string, cwd?: string): string {
  const p = pattern.trim();
  if (p.length === 0) return pattern;
  // `//abs` → `/abs` (legacy permission-rule escape).
  if (p.startsWith('//')) return p.slice(1);
  // `~` / `~/rest` → home-relative.
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return resolvePath(homedir(), p.slice(2));
  // Already absolute → as-written.
  if (isAbsolute(p)) return p;
  // Relative → resolve against cwd when available, else leave untouched so a
  // cwd-less caller (e.g. the existing absolute-path translation) is unaffected.
  return cwd ? resolvePath(cwd, p) : p;
}

/**
 * Map a high-level {@link SandboxPolicy} (the engine/tool-facing config, shared
 * with the container backend) onto the low-level host {@link SandboxConfig}
 * consumed by {@link wrapWithSandbox}. Only fields the HOST unshare backend can
 * enforce are mapped: the per-host network allow/deny lists, the Unix-socket
 * allowlist, the http/socks proxy ports, and the filesystem deny-read/deny-write
 * carve-outs with the allow-read AND allow-write re-opens. Filesystem path entries
 * are normalized through {@link normalizeSandboxFsPath} (CC `~`/`//`/relative
 * conventions). Each field is included only when set, so an empty/absent policy
 * yields `{}` and leaves the sandbox defaults intact (no behavior change when no
 * policy carve-outs are configured). This is the single translation point so
 * bash.ts stays a one-line call site and the host backend honors the same policy
 * the container backend already does.
 */
export function hostSandboxConfigFromPolicy(policy?: SandboxPolicy, cwd?: string): Partial<SandboxConfig> {
  const cfg: Partial<SandboxConfig> = {};
  if (!policy) return cfg;
  const net = policy.network;
  if (net) {
    // PARITY (enterprise managed-domains lockdown — the reference's
    // shouldAllowManagedSandboxDomainsOnly): when the managed policy forces
    // domains-only egress, the effective outbound allowlist is the managed list
    // ALONE — user `allowedHosts` (incl. web_fetch-allow rules folded in by
    // getEffectiveSandboxPolicy) is ignored, and an empty/absent managed list
    // blocks ALL outbound (the secure default-deny). The denylist below still
    // applies (deny wins). Otherwise the normal allowlist flows through.
    if (net.allowManagedDomainsOnly) {
      cfg.allowedDomains = net.managedAllowedHosts ? [...net.managedAllowedHosts] : [];
    } else if (net.allowedHosts) {
      cfg.allowedDomains = net.allowedHosts;
    }
    if (net.deniedHosts) cfg.deniedDomains = net.deniedHosts;
    if (net.allowUnixSockets) cfg.allowedUnixSockets = net.allowUnixSockets;
    if (net.allowAllUnixSockets !== undefined) cfg.allowUnixSockets = net.allowAllUnixSockets;
    if (net.httpProxyPort !== undefined) cfg.httpProxyPort = net.httpProxyPort;
    if (net.socksProxyPort !== undefined) cfg.socksProxyPort = net.socksProxyPort;
    if (net.allowLocalBinding !== undefined) cfg.allowLocalBinding = net.allowLocalBinding;
  }
  const fs = policy.filesystem;
  if (fs) {
    // Normalize each configured fs path through the CC-compatible convention
    // resolver so `~`/`//abs`/relative entries work as written. Absolute paths
    // pass through unchanged, so a policy built entirely from absolute paths maps
    // identically to the pre-normalization behavior.
    const norm = (list: string[]): string[] => list.map((p) => normalizeSandboxFsPath(p, cwd));
    if (fs.denyRead) cfg.denyReadPaths = norm(fs.denyRead);
    if (fs.denyWrite) cfg.denyWritePaths = norm(fs.denyWrite);
    // PARITY (enterprise managed read-paths lockdown — the reference's
    // allowManagedReadPathsOnly): under the managed lockdown the read RE-allow set
    // is the managed list ALONE — user `allowRead` is ignored so a lower tier
    // cannot re-open a managed-denied read region. denyRead above still applies.
    if (fs.allowManagedReadPathsOnly) {
      cfg.allowReadPaths = fs.managedAllowRead ? norm(fs.managedAllowRead) : [];
    } else if (fs.allowRead) {
      cfg.allowReadPaths = norm(fs.allowRead);
    }
    if (fs.allowWrite) cfg.allowWritePaths = norm(fs.allowWrite);
  }
  // Security-weakening escape hatches (nested-userns / network-isolation relax).
  if (policy.enableWeakerNestedSandbox !== undefined) cfg.enableWeakerNestedSandbox = policy.enableWeakerNestedSandbox;
  if (policy.enableWeakerNetworkIsolation !== undefined) cfg.enableWeakerNetworkIsolation = policy.enableWeakerNetworkIsolation;
  return cfg;
}

/**
 * Trim, drop empties, and de-duplicate a host list while preserving first-seen
 * order. Used to merge a static allowlist with the session's granted hosts so a
 * host that appears in both is emitted once. Pure.
 */
export function dedupeHostList(hosts: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of hosts) {
    const h = raw.trim();
    if (h.length === 0 || seen.has(h)) continue;
    seen.add(h);
    out.push(h);
  }
  return out;
}

/**
 * PARITY (allowLocalBinding): bring the loopback interface UP inside the
 * sandbox's network namespace so a sandboxed server/test can bind 127.0.0.1
 * WITHOUT enabling outbound network. Returns '' unless the sandbox has its own
 * netns (outbound network OFF) AND `allowLocalBinding` is set — when outbound
 * network is on, loopback is already up, and when the knob is unset the stricter
 * default (loopback left down) is preserved. Mirrors the reference's
 * `SandboxNetworkConfig.allowLocalBinding`. Best-effort (`2>/dev/null`).
 */
export function buildLoopbackSetup(cfg: SandboxConfig): string {
  if (!cfg.allowLocalBinding || cfg.allowNetwork) return '';
  return 'ip link set lo up 2>/dev/null; ';
}

/** Join a sandbox cwd and a relative path, normalizing a trailing slash so the
 * segment never contains a doubled separator. */
export function joinUnderCwd(cwd: string, rel: string): string {
  return cwd.replace(/\/+$/, '') + '/' + rel;
}

/**
 * Relative paths (under the command's cwd) that, if writable inside the
 * sandbox, let a sandboxed command rewrite the agent's own trusted config
 * (settings-rewrite sandbox escape) or plant an auto-discovered skill / command
 * / agent that would later run with full capabilities (privilege escalation).
 * Covers both the Claude-compatible `.claude/` tree and AMI's `.ami/` tree.
 */
export const CONFIG_ESCAPE_RELPATHS = [
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
