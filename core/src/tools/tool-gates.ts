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
