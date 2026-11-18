/**
 * Auto-mode safety-classifier INPUT PROJECTION (data-minimization + relevance gate).
 *
 * The auto-mode safety classifier (common/src/auto-mode-safety-classifier.ts) is a
 * secondary reviewer consulted on the autonomous auto-approve path. Shipping a tool's
 * RAW input to that reviewer is both a data-exposure risk (secrets / large payloads)
 * and often needless (many fields carry no security signal). This module centralizes
 * the per-tool projection that controls EXACTLY which fields a tool exposes to the
 * classifier, mirroring Claude Code's per-tool `Tool.toAutoClassifierInput(input)`
 * (yoloClassifier.ts L384-424):
 *
 *  - A tool's own {@link ToolDefinition.toAutoClassifierInput} hook, when present, is
 *    the single source of truth for its projected view (e.g. file_edit exposes only
 *    the path, never the file content).
 *  - A hook returning the empty string `''` declares the tool NOT security-relevant:
 *    the classifier SHORT-CIRCUITS to auto-allow and never runs the model query
 *    (parity: yoloClassifier L1021-1029 — `'' => allow without hitting the model`).
 *  - The hook runs on UNVALIDATED model/history output, so it is wrapped in try/catch
 *    with a raw-input fallback rather than crashing the permission check (parity:
 *    yoloClassifier L392-410 — malformed historical tool_use input falls back to the
 *    raw object).
 *  - With no hook, a `bash` call collapses to its command string and every other tool
 *    keeps its full input object (the previous hardcoded default, now consolidated).
 *
 * This is the ONE canonical projection resolver; callers must never re-derive a tool's
 * classifier view.
 */
import type { ToolDefinition } from '../types';

/** Result of projecting a tool's raw input to what the safety classifier may see. */
export interface AutoClassifierProjection {
  /** The minimized value to describe to the classifier. */
  projected: unknown;
  /**
   * `false` when the tool declared itself NOT security-relevant (its
   * `toAutoClassifierInput` returned `''`): the classifier must SKIP the model call
   * and leave the deterministic auto-approval intact. `true` otherwise (including the
   * try/catch raw-input fallback, which fails SAFE by keeping the call reviewable).
   */
  securityRelevant: boolean;
}

/**
 * Resolve the classifier projection for a tool call. See the module doc for the
 * precedence (per-tool hook → `''` short-circuit → defensive raw fallback → default
 * bash/full-input projection).
 */
export function resolveAutoClassifierInput(
  tool: Pick<ToolDefinition, 'name' | 'toAutoClassifierInput'> | undefined,
  input: Record<string, unknown>,
): AutoClassifierProjection {
  const hook = tool?.toAutoClassifierInput;
  if (hook) {
    let projected: string;
    try {
      projected = hook(input);
    } catch {
      // The hook consumed unvalidated model/history output and threw — fall back to
      // the raw input (reviewable) instead of crashing the permission check.
      return { projected: input, securityRelevant: true };
    }
    if (projected === '') return { projected: '', securityRelevant: false };
    return { projected, securityRelevant: true };
  }
  // No per-tool hook: default projection. A bash call collapses to its command;
  // every other tool keeps its full input object.
  if (tool?.name === 'bash' && typeof input.command === 'string') {
    return { projected: input.command, securityRelevant: true };
  }
  return { projected: input, securityRelevant: true };
}
