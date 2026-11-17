import { isEnvTruthy } from '../analytics';

/**
 * PARITY (additive): centralized capability gates for conditional tool
 * registration. Mirrors Claude Code's feature-flag pattern — `tasks.ts`
 * conditionally registers task types behind `feature('WORKFLOW_SCRIPTS')` /
 * `feature('MONITOR_TOOL')`, `agentSwarmsEnabled.ts` exposes a single
 * `isAgentSwarmsEnabled()` gate checked everywhere teammates are referenced,
 * and `useLspInitializationNotification.tsx` only activates the LSP subsystem
 * "when ENABLE_LSP_TOOL is set".
 *
 * AMI keeps these subsystems ON by default (they are shipped, loved
 * differentiators — never dropped to match a reference's default-off), while
 * adding a single source-of-truth toggle so an operator can disable a whole
 * subsystem uniformly from ONE place instead of passing `--deny` per tool name.
 * This is purely additive: the default preserves AMI's always-on behavior.
 */

/**
 * Parse a default-ON capability env flag. Unset or empty → enabled; an explicit
 * falsey token ('0'/'false'/'no'/'off') → disabled; any other value → enabled.
 * Reuses {@link isEnvTruthy} for the canonical falsey-token vocabulary so the
 * off-switch spelling matches every other AMI env flag.
 */
export function capabilityEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw.trim() === '') return true;
  return isEnvTruthy(raw);
}

/** Env var that toggles the multi-agent teammate subsystem (default ON). */
export const AGENT_TEAMS_ENV = 'AMI_AGENT_TEAMS';
/** CLI flag that force-enables the teammate subsystem (mirrors Claude's `--agent-teams`). */
export const AGENT_TEAMS_ON_FLAG = '--agent-teams';
/** CLI flag that force-disables the teammate subsystem. */
export const AGENT_TEAMS_OFF_FLAG = '--no-agent-teams';

/**
 * Single runtime gate for the multi-agent teammate subsystem (`task`,
 * `team_create`, `team_delete`, `send_message`). The one place to check whether
 * teammates are enabled, so prompt/tool-registration/UI stay consistent.
 *
 * Resolution order (first match wins): `--no-agent-teams` → off,
 * `--agent-teams` → on, then {@link AGENT_TEAMS_ENV}. Default ON.
 */
export function isTeammatesEnabled(
  env: NodeJS.ProcessEnv = process.env,
  argv: readonly string[] = process.argv,
): boolean {
  if (argv.includes(AGENT_TEAMS_OFF_FLAG)) return false;
  if (argv.includes(AGENT_TEAMS_ON_FLAG)) return true;
  return capabilityEnabled(env[AGENT_TEAMS_ENV]);
}

/** Env var that toggles the LSP tool subsystem (default ON). */
export const LSP_ENV = 'AMI_LSP';

/**
 * Gate for the LSP tools (`diagnostics`, `find_references`, `lsp`). Parity with
 * Claude's ENABLE_LSP_TOOL knob, but default ON (AMI ships LSP always-on) with
 * {@link LSP_ENV} as the disable switch.
 */
export function isLspEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return capabilityEnabled(env[LSP_ENV]);
}

/**
 * Env vars that disable background-task execution (default: ENABLED). When either
 * is truthy the bash tool (a) omits `run_in_background` from the model-facing input
 * schema, (b) drops the background guidance sentence from its description, and
 * (c) forces a `run_in_background: true` request to run in the foreground. Lets an
 * operator in a locked-down environment turn the capability fully off from one
 * place. Parity: Claude Code's `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` (read via
 * `isEnvTruthy`), whose spelling is accepted alongside the AMI-native name — same
 * dual-name convention analytics.ts uses for the DISABLE_NONESSENTIAL_TRAFFIC pair.
 */
export const DISABLE_BACKGROUND_TASKS_ENV = 'AMI_DISABLE_BACKGROUND_TASKS';
/** Claude Code-compatible spelling accepted alongside {@link DISABLE_BACKGROUND_TASKS_ENV}. */
export const DISABLE_BACKGROUND_TASKS_ENV_COMPAT = 'CLAUDE_CODE_DISABLE_BACKGROUND_TASKS';

/**
 * True when background tasks are disabled by {@link DISABLE_BACKGROUND_TASKS_ENV}
 * (or its Claude-compatible alias). This is a disable switch — default OFF means
 * background tasks stay ENABLED (AMI's always-on default), so it uses truthy
 * detection rather than {@link capabilityEnabled}'s default-ON semantics.
 */
export function isBackgroundTasksDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[DISABLE_BACKGROUND_TASKS_ENV]) || isEnvTruthy(env[DISABLE_BACKGROUND_TASKS_ENV_COMPAT]);
}

/**
 * Env var that disables the git/commit guidance in the bash tool description
 * (default: ENABLED — the git sentence is shown). When truthy, the bash tool
 * drops the "Git: `git commit` via bash is blocked … never commit unless
 * explicitly asked" sentence from its model-facing description. Parity: Claude
 * Code's `shouldIncludeGitInstructions()` / `getCommitAndPRInstructions()` gate
 * (prompt.ts), which omits the git/commit-PR section for deployments that steer
 * the model away from git entirely. This is a disable switch, so it uses truthy
 * detection rather than {@link capabilityEnabled}'s default-ON semantics.
 */
export const DISABLE_GIT_INSTRUCTIONS_ENV = 'AMI_DISABLE_GIT_INSTRUCTIONS';

/**
 * True when the bash git/commit guidance is disabled by
 * {@link DISABLE_GIT_INSTRUCTIONS_ENV}. Default OFF → git guidance stays in the
 * description.
 */
export function isGitInstructionsDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[DISABLE_GIT_INSTRUCTIONS_ENV]);
}

/**
 * Env var that globally disables the lightweight host (unshare) auto-sandbox
 * (default: ENABLED — the auto-wrap stays on). When truthy the bash tool never
 * wraps a command with the unshare backend in the non-container branch, even
 * when a SANDBOX_TRIGGER_PATTERN matches. Parity: Claude Code's
 * `SandboxManager.isSandboxingEnabled()` — a single off-switch for all
 * auto-sandboxing. Container mode (`sandbox.mode === 'container'`) is governed
 * separately and is NOT affected. Per-call `SandboxPolicy.enabled === false`
 * provides the same off-switch scoped to one engine config.
 */
export const DISABLE_HOST_SANDBOX_ENV = 'AMI_DISABLE_SANDBOX';

/**
 * True when the lightweight host sandbox is disabled by
 * {@link DISABLE_HOST_SANDBOX_ENV}. Default OFF → the host auto-sandbox stays on.
 */
export function isHostSandboxDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[DISABLE_HOST_SANDBOX_ENV]);
}

/**
 * Env var that forbids the `dangerouslyDisableSandbox` escape hatch entirely
 * (default: ALLOWED — the parameter is advertised and honored). When truthy the
 * bash tool (a) omits `dangerouslyDisableSandbox` from the model-facing input
 * schema and (b) ignores it at runtime, so a model can never run a command
 * outside the sandbox. Parity: Claude Code's
 * `SandboxManager.areUnsandboxedCommandsAllowed()` policy knob, whose prompt
 * states 'the `dangerouslyDisableSandbox` parameter is disabled by policy'. This
 * is the env-level analogue of the per-config
 * `SandboxPolicy.allowUnsandboxedCommands === false` runtime gate — either one
 * locks out the override.
 */
export const DISABLE_SANDBOX_ESCAPE_ENV = 'AMI_DISABLE_SANDBOX_ESCAPE';

/**
 * True when the `dangerouslyDisableSandbox` escape hatch is forbidden by
 * {@link DISABLE_SANDBOX_ESCAPE_ENV}. Default OFF → the override is available.
 */
export function isSandboxEscapeDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[DISABLE_SANDBOX_ESCAPE_ENV]);
}

/**
 * Env var that switches the host auto-sandbox from AMI's narrow trigger-pattern
 * allowlist to Claude Code's DEFAULT-DENY coverage model (default: OFF — the
 * trigger allowlist is used). When truthy, {@link shouldUseSandbox} sandboxes
 * EVERY command that is not explicitly exempt, instead of only the ~11 hardcoded
 * trigger patterns (curl/wget/interpreters/eval/…). This closes the structural
 * coverage hole where a compiled/local binary (`./evil`), `nc`, `ssh`, `scp`,
 * `rsync`, `dd`, `socat`, a path-invoked shell script, or a bare `> /etc/…`
 * redirection runs with NO sandbox because it matches no trigger. Parity: Claude
 * Code's `shouldUseSandbox()` (tools/BashTool/shouldUseSandbox.ts), which returns
 * `true` for all commands once sandboxing is enabled and `false` only for
 * excluded commands. Disable switch semantics (default OFF → trigger allowlist),
 * so it uses truthy detection rather than {@link capabilityEnabled}.
 */
export const SANDBOX_DEFAULT_DENY_ENV = 'AMI_SANDBOX_DEFAULT_DENY';

/**
 * True when the host auto-sandbox should cover every non-exempt command
 * (default-deny) rather than only the hardcoded trigger patterns, per
 * {@link SANDBOX_DEFAULT_DENY_ENV}. Default OFF → AMI's trigger allowlist is used.
 */
export function isSandboxDefaultDeny(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[SANDBOX_DEFAULT_DENY_ENV]);
}

/** Env var that toggles the workflow-scripts tool (default ON). */
export const WORKFLOW_SCRIPTS_ENV = 'AMI_WORKFLOW_SCRIPTS';

/**
 * Gate for the `workflow` tool. Parity with Claude's `feature('WORKFLOW_SCRIPTS')`
 * conditional task registration, so the capability can be flipped off from one
 * place without removing code. Default ON.
 */
export function isWorkflowScriptsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return capabilityEnabled(env[WORKFLOW_SCRIPTS_ENV]);
}

/** Env var that toggles the cron / scheduled-prompt subsystem (default ON). */
export const CRON_ENV = 'AMI_CRON';

/**
 * Gate for the cron / scheduling tools (`cron_create`, `cron_delete`,
 * `cron_list`, `schedule_wakeup`). Parity with Claude's `isKairosCronEnabled()`
 * feature-flag check on its ScheduleCron tools, but default ON (AMI ships
 * scheduling always-on) with {@link CRON_ENV} as the single disable switch.
 */
export function isCronEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return capabilityEnabled(env[CRON_ENV]);
}

/**
 * Env var holding a comma-separated blanket DENY list for the advertised tool pool.
 * PARITY (additive): mirrors Claude Code's `filterToolsByDenyRules` (tools.ts) — a
 * config-driven blocklist that strips tools from the pool before the model sees them,
 * instead of AMI's previously fixed-at-compile-time reserved set. Each entry is one of:
 *   - an exact tool name (`web_search`),
 *   - a `name*` prefix wildcard (`cron_*` drops the whole cron family),
 *   - an `mcp__<server>` server rule that drops every `mcp__<server>__*` tool.
 * Empty / unset → nothing denied (AMI's always-on default is preserved).
 */
export const DENY_TOOLS_ENV = 'AMI_DENY_TOOLS';

/** Parse {@link DENY_TOOLS_ENV} into a de-duplicated, trimmed rule set. */
export function parseDeniedTools(env: NodeJS.ProcessEnv = process.env): Set<string> {
  const raw = env[DENY_TOOLS_ENV];
  if (!raw) return new Set();
  return new Set(
    raw
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0),
  );
}

/**
 * True when `name` is blocked by any rule in `denied` — an exact match, a `prefix*`
 * wildcard, or an `mcp__<server>` server rule that covers every tool namespaced to
 * that server (`mcp__<server>__<tool>`). Shared matcher so the registry filter and
 * any future caller apply identical deny semantics.
 */
export function isToolDenied(name: string, denied: ReadonlySet<string>): boolean {
  if (denied.size === 0) return false;
  if (denied.has(name)) return true;
  for (const rule of denied) {
    if (rule.endsWith('*') && name.startsWith(rule.slice(0, -1))) return true;
    if (rule.startsWith('mcp__') && !rule.includes('*') && name.startsWith(`${rule}__`)) {
      return true;
    }
  }
  return false;
}
