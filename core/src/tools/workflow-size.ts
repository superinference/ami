/**
 * Dynamic workflow size — a soft, operator-tunable guideline on how many agents a
 * single workflow run should fan out to, plus the threshold above which the TUI
 * flags a run as a "Large workflow". Mirrors the reference's "Dynamic workflow
 * size" /config setting ("medium size (/config)" on the background-run card).
 *
 * This is a GUIDELINE (injected into the workflow tool description so the model
 * sizes its fan-out) + a WARN threshold (drives the progress-tree "⚠ Large
 * workflow · /flows to stop" badge). It is NEVER a hard cap — the hard ceiling is
 * the runtime's MAX_AGENTS (1000). The active size is sourced from an env knob the
 * config→env bridge seeds from ProjectConfig.dynamicWorkflowSize.
 */

export type WorkflowSize = 'small' | 'medium' | 'large' | 'off';

export const WORKFLOW_SIZES: readonly WorkflowSize[] = ['small', 'medium', 'large', 'off'] as const;

/** Env knob seeded from ProjectConfig.dynamicWorkflowSize by the config bridge. */
export const WORKFLOW_SIZE_ENV = 'AMI_DYNAMIC_WORKFLOW_SIZE';

/** Default when nothing is configured (matches the reference's "medium" default). */
export const DEFAULT_WORKFLOW_SIZE: WorkflowSize = 'medium';

export interface WorkflowSizeGuideline {
  size: WorkflowSize;
  /** Soft guideline: aim to keep a run under ~this many agents (null when 'off'). */
  softMaxAgents: number | null;
  /** Flag a run as "Large" once it exceeds this many agents (Infinity when 'off'). */
  warnThreshold: number;
  /** Card label, e.g. "medium size". */
  label: string;
}

const BANDS: Record<WorkflowSize, { softMaxAgents: number | null; warnThreshold: number }> = {
  small: { softMaxAgents: 5, warnThreshold: 8 },
  medium: { softMaxAgents: 10, warnThreshold: 16 },
  large: { softMaxAgents: 25, warnThreshold: 40 },
  off: { softMaxAgents: null, warnThreshold: Infinity },
};

/** Coerce an arbitrary value to a WorkflowSize, or undefined when not one. */
export function normalizeWorkflowSize(v: unknown): WorkflowSize | undefined {
  if (typeof v !== 'string') return undefined;
  const s = v.trim().toLowerCase();
  return (WORKFLOW_SIZES as readonly string[]).includes(s) ? (s as WorkflowSize) : undefined;
}

/** Resolve the active size from env (config→env bridge seeds it), defaulting to medium. */
export function resolveWorkflowSize(env: NodeJS.ProcessEnv = process.env): WorkflowSize {
  return normalizeWorkflowSize(env[WORKFLOW_SIZE_ENV]) ?? DEFAULT_WORKFLOW_SIZE;
}

/** The band (soft cap + warn threshold + label) for a size. */
export function getWorkflowSizeGuideline(size: WorkflowSize = resolveWorkflowSize()): WorkflowSizeGuideline {
  const band = BANDS[size];
  return { size, softMaxAgents: band.softMaxAgents, warnThreshold: band.warnThreshold, label: `${size} size` };
}

/** The one-line guideline injected into the workflow tool description. */
export function workflowSizeGuidelineText(env: NodeJS.ProcessEnv = process.env): string {
  const g = getWorkflowSizeGuideline(resolveWorkflowSize(env));
  if (g.softMaxAgents === null) {
    return 'Dynamic workflow size is OFF (/config dynamicWorkflowSize): no soft agent-count guideline — scale the fan-out to the task.';
  }
  return `Dynamic workflow size is ${g.size} (/config dynamicWorkflowSize): aim to keep one run under ~${g.softMaxAgents} agents (soft guideline, not a hard cap); a run beyond ~${g.warnThreshold} agents is flagged as a Large workflow.`;
}

/** Whether a run's agent count warrants the "Large workflow" warning for a size. */
export function isLargeWorkflow(agentCount: number, size: WorkflowSize = resolveWorkflowSize()): boolean {
  return agentCount > getWorkflowSizeGuideline(size).warnThreshold;
}
