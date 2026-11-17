/**
 * Sub-agent handoff safety review (parity: Claude Code's
 * `classifyHandoffIfNeeded` / `agentToolUtils.ts:L389-481`).
 *
 * When a parent delegates to a sub-agent in an AUTONOMOUS permission mode
 * (`auto-allow` / `bypassPermissions`, i.e. "yolo"), the sub-agent's tool calls
 * are auto-approved with no human in the loop. Before control is handed back, its
 * work is reviewed across the agent trust boundary: if the sub-agent took a
 * dangerous action, a hard SECURITY WARNING is PREPENDED to the result so the
 * parent verifies before acting on it. When the reviewing classifier is
 * UNAVAILABLE (errored / threw), the sub-agent output is still propagated — never
 * silently trusted nor silently dropped — but a softer "classifier was
 * unavailable, please verify" note is prepended instead (Claude's graceful
 * fallback, L461-470).
 *
 * The default classifier is a self-contained HEURISTIC that reuses core's existing
 * dangerous-command detectors (no duplicate detection logic) over the exact tool
 * calls the sub-agent made. A richer async classifier (e.g. an LLM reviewer) can be
 * injected via {@link reviewSubagentHandoff}'s `classifier` option; a throwing /
 * `unavailable` classifier takes the fallback path.
 */

import { commandHasDangerousRemoval } from '../permissions';
import { detectNuclearCommand, detectDangerousRm } from './bash-security';
import { detectDetachedGitDiscard } from './bash';

/** A single tool call a sub-agent made, captured during its run for post-hoc review. */
export interface HandoffAction {
  /** Canonical tool name (e.g. `bash`, `file_edit`). */
  tool: string;
  /** The shell command, when `tool === 'bash'`. */
  command?: string;
}

/** The verdict a handoff classifier returns for a sub-agent's run. */
export interface HandoffReviewResult {
  /** True when the sub-agent took an action that may violate security policy. */
  flagged: boolean;
  /** Human-readable reason, surfaced in the SECURITY WARNING. */
  reason?: string;
  /** Set when the classifier could not run (treated like a throw → soft fallback). */
  unavailable?: boolean;
}

/** A pluggable handoff classifier. May be sync or async; a throw → fallback path. */
export type HandoffClassifier = (
  actions: HandoffAction[],
) => HandoffReviewResult | Promise<HandoffReviewResult>;

/** Hard warning prepended when the review flags a dangerous sub-agent action. */
export const HANDOFF_SECURITY_WARNING_PREFIX =
  'SECURITY WARNING: This sub-agent performed actions that may violate security policy. '
  + 'Verify its work before acting on it.';

/** Soft note prepended when the classifier was unavailable (parity: Claude L469). */
export const HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE =
  'Note: The safety classifier was unavailable when reviewing this sub-agent\'s work. '
  + 'Please carefully verify the actions it took before relying on this result.';

/**
 * The permission modes that auto-approve a sub-agent's tool calls with no human in
 * the loop, so its handoff warrants a safety review (parity: Claude's `auto` / yolo
 * gate). `ask` / `plan` / `deny-all` / `acceptEdits` keep a human (or a prompt) in
 * the approval path, so no post-hoc review is needed.
 */
export function isAutonomousHandoffMode(mode: string | undefined): boolean {
  return mode === 'auto-allow' || mode === 'bypassPermissions';
}

/**
 * The built-in heuristic classifier: flags a sub-agent's run when any of its bash
 * commands is a catastrophic removal, a nuclear command (fork bomb / disk wipe /
 * etc.), a dangerous `rm`, or a detached git-discard (reset --hard / stash drop /
 * restore / clean -f). Reuses the SAME detectors the live permission layer uses —
 * no second copy of dangerous-command logic (AGENTS.md No-Duplication Rule).
 */
export function builtinHandoffClassifier(cwd: string): HandoffClassifier {
  return (actions: HandoffAction[]): HandoffReviewResult => {
    for (const action of actions) {
      if (action.tool !== 'bash' || !action.command) continue;
      const command = action.command;
      if (commandHasDangerousRemoval(command, cwd)) {
        return { flagged: true, reason: `destructive removal: \`${command}\`` };
      }
      const nuclear = detectNuclearCommand(command);
      if (nuclear.blocked) {
        return { flagged: true, reason: `${nuclear.reason}: \`${command}\`` };
      }
      if (detectDangerousRm(command).dangerous) {
        return { flagged: true, reason: `dangerous rm: \`${command}\`` };
      }
      if (detectDetachedGitDiscard(command)) {
        return { flagged: true, reason: `git history/worktree discard: \`${command}\`` };
      }
    }
    return { flagged: false };
  };
}

/**
 * Review a sub-agent's accumulated output before handing control back to the
 * parent. A no-op (returns `output` unchanged) unless the sub-agent ran in an
 * autonomous mode ({@link isAutonomousHandoffMode}). In an autonomous mode:
 *  - a FLAGGED verdict prepends {@link HANDOFF_SECURITY_WARNING_PREFIX} (+ reason);
 *  - an UNAVAILABLE verdict, or a classifier that THROWS, prepends
 *    {@link HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE} (output still propagated);
 *  - a clean verdict returns `output` unchanged.
 */
export async function reviewSubagentHandoff(opts: {
  output: string;
  permissionMode: string | undefined;
  actions: HandoffAction[];
  cwd: string;
  classifier?: HandoffClassifier;
}): Promise<string> {
  if (!isAutonomousHandoffMode(opts.permissionMode)) return opts.output;
  const classifier = opts.classifier ?? builtinHandoffClassifier(opts.cwd);
  let result: HandoffReviewResult;
  try {
    result = await classifier(opts.actions);
  } catch {
    return `${HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE}\n\n${opts.output}`;
  }
  if (result.unavailable) {
    return `${HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE}\n\n${opts.output}`;
  }
  if (result.flagged) {
    const reason = result.reason ? ` Reason: ${result.reason}` : '';
    return `${HANDOFF_SECURITY_WARNING_PREFIX}${reason}\n\n${opts.output}`;
  }
  return opts.output;
}
