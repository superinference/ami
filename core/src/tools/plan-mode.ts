import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { generateWordSlug, generateShortWordSlug } from '../words';
import { isEnvTruthy } from '../analytics';

let _planModeActive = false;
let _planFilePath: string | null = null;
/** Last plan content submitted on exit — used as a fallback when a rejected
 *  plan must be re-rendered but the file is gone. */
let _lastPlan: string | null = null;

/** Max retries when a slug-based plan filename collides before falling back. */
export const MAX_SLUG_RETRIES = 5;
/** Hard cap on how much plan content is echoed back into the context on exit. */
export const PLAN_MAX_RESULT_CHARS = 100_000;
/** Soft conciseness threshold: `verify` warns past this so the model trims the
 *  plan to a reviewable outline rather than a dump before exiting. */
export const PLAN_SOFT_CAP_CHARS = 12_000;

/**
 * Plan-mode interview-phase feature flag (plan mode V2 parity).
 *
 * When enabled, `enter` delivers an interview-first workflow — clarify
 * requirements with AskUserQuestion, then run parallel read-only exploration —
 * instead of the linear exploration workflow. Off by default; opt in with
 * `AMI_PLAN_MODE_INTERVIEW_PHASE` (the Claude-config var
 * `CLAUDE_CODE_PLAN_MODE_INTERVIEW_PHASE` is honored for compatibility).
 */
export function isPlanModeInterviewPhaseEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.AMI_PLAN_MODE_INTERVIEW_PHASE) ||
    isEnvTruthy(env.CLAUDE_CODE_PLAN_MODE_INTERVIEW_PHASE);
}

/** Clamp bounds + default for the configurable plan-mode exploration agent count. */
export const PLAN_AGENT_COUNT_MIN = 1;
export const PLAN_AGENT_COUNT_MAX = 10;
export const PLAN_AGENT_COUNT_DEFAULT = 1;

/**
 * Number of parallel read-only exploration passes/agents plan mode may use
 * during the interview phase (plan mode V2 parity: getPlanModeV2AgentCount).
 *
 * Overridable via `AMI_PLAN_AGENT_COUNT` (the Claude-config var
 * `CLAUDE_CODE_PLAN_V2_AGENT_COUNT` is honored for compatibility), clamped to
 * `[PLAN_AGENT_COUNT_MIN, PLAN_AGENT_COUNT_MAX]`. Unset, non-numeric, or
 * out-of-range values fall back to {@link PLAN_AGENT_COUNT_DEFAULT}.
 */
export function getPlanAgentCount(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.AMI_PLAN_AGENT_COUNT ?? env.CLAUDE_CODE_PLAN_V2_AGENT_COUNT;
  if (raw !== undefined) {
    const n = parseInt(raw, 10);
    if (!Number.isNaN(n) && n >= PLAN_AGENT_COUNT_MIN && n <= PLAN_AGENT_COUNT_MAX) {
      return n;
    }
  }
  return PLAN_AGENT_COUNT_DEFAULT;
}

export function isPlanModeActive(): boolean { return _planModeActive; }
export function setPlanModeActive(active: boolean): void { _planModeActive = active; }
export function getPlanFilePath(): string | null { return _planFilePath; }
/**
 * Recover the current plan content (fallback on rejection / read-back).
 *
 * Prefers the last plan submitted on exit (`_lastPlan`); when none has been
 * submitted in this plan session it falls back to reading the active plan file
 * from disk, so a plan that was written but not yet submitted — or that must be
 * re-rendered after a rejection — is never lost. Mirrors Claude's `getPlan()`,
 * which reads the plan file directly.
 */
export function getPlan(): string | null {
  if (_lastPlan !== null) return _lastPlan;
  if (_planFilePath) {
    try { return fs.readFileSync(_planFilePath, 'utf-8'); } catch { /* no file yet */ }
  }
  return null;
}

/** Memoized resolution of the plans directory, keyed by cwd + override, so the
 *  repeated enter/write/boundary-check callers don't re-resolve each time. */
const _plansDirCache = new Map<string, string>();

/** Default plans directory: the validated per-project state dir, rejecting any
 *  path that escapes it (defense against traversal via a poisoned cwd). */
function defaultPlansDirectory(cwd: string): string {
  const base = projectStateDir(cwd, 'plans');
  const resolved = path.resolve(base);
  const root = path.resolve(projectStateDir(cwd, ''));
  if (!resolved.startsWith(root)) {
    throw new Error('Refusing to use a plans directory outside the project state dir.');
  }
  return resolved;
}

/**
 * Resolve the plans directory for a cwd.
 *
 * - Default: the validated per-project state dir (`<state>/plans`).
 * - Configurable: when `AMI_PLANS_DIR` is set it relocates plan `.md` files,
 *   resolved RELATIVE TO the project root (`cwd`). The resolved path is
 *   validated to stay within `cwd` (path-traversal defense); on a violation it
 *   falls back to the default dir rather than writing outside the project.
 *
 * Result is memoized per (cwd, override) so render/permission-path callers do
 * not re-resolve on every call.
 */
export function getPlansDirectory(cwd: string, env: NodeJS.ProcessEnv = process.env): string {
  const override = env.AMI_PLANS_DIR;
  const key = `${cwd}\0${override ?? ''}`;
  const cached = _plansDirCache.get(key);
  if (cached !== undefined) return cached;

  let resolved: string;
  if (override) {
    const candidate = path.resolve(cwd, override);
    const root = path.resolve(cwd);
    if (candidate === root || candidate.startsWith(root + path.sep)) {
      resolved = candidate;
    } else {
      // Traversal attempt — refuse the configured dir, use the safe default.
      resolved = defaultPlansDirectory(cwd);
    }
  } else {
    resolved = defaultPlansDirectory(cwd);
  }
  _plansDirCache.set(key, resolved);
  return resolved;
}

/** Test/reset seam: clear the memoized plans-directory resolutions. */
export function clearPlansDirectoryCache(): void {
  _plansDirCache.clear();
}

/** Per-session plan slug cache (SessionId -> word slug). A session reuses one
 *  stable, human-readable slug so repeated enter/exit cycles and the plan
 *  file name line up, mirroring Claude's getPlanSlugCache. */
const _planSlugCache = new Map<string, string>();

/**
 * Get (lazily generating) the stable word slug for a session's plan, retrying
 * up to {@link MAX_SLUG_RETRIES} to avoid colliding with an existing
 * `<slug>.md` in `planDir` before caching the result for the whole session.
 */
export function getPlanSlug(sessionId: string, planDir: string): string {
  const existing = _planSlugCache.get(sessionId);
  if (existing !== undefined) return existing;
  let slug = generateWordSlug();
  for (let i = 0; i < MAX_SLUG_RETRIES; i++) {
    slug = generateWordSlug();
    if (!fs.existsSync(path.join(planDir, `${slug}.md`))) break;
  }
  _planSlugCache.set(sessionId, slug);
  return slug;
}

/** Forget a session's cached plan slug (e.g. on session end / fork). */
export function clearPlanSlug(sessionId: string): void {
  _planSlugCache.delete(sessionId);
}

/** Forget every cached plan slug (e.g. on /clear). */
export function clearAllPlanSlugs(): void {
  _planSlugCache.clear();
}

/**
 * Pick a collision-free, human-recognizable plan filename within `planDir`.
 *
 * - With a `sessionId`, the file is `<session-slug>.md`, stable for the whole
 *   session (so re-entering plan mode in one session reuses the same file).
 * - Without one (bare/test contexts), a fresh unique `plan-<word-slug>.md` is
 *   allocated, retrying to dodge an on-disk collision.
 */
export function allocatePlanFile(planDir: string, opts: { sessionId?: string } = {}): string {
  if (opts.sessionId) {
    return path.join(planDir, `${getPlanSlug(opts.sessionId, planDir)}.md`);
  }
  for (let i = 0; i < MAX_SLUG_RETRIES; i++) {
    const candidate = path.join(planDir, `plan-${generateWordSlug()}.md`);
    if (!fs.existsSync(candidate)) return candidate;
  }
  // Fallback: a short-slug + timestamp name is effectively collision-proof.
  return path.join(planDir, `plan-${generateShortWordSlug()}-${Date.now().toString(36)}.md`);
}

export const planModeTool: ToolDefinition = {
  name: 'plan_mode',
  description:
    'Use this tool proactively when you are about to start a non-trivial implementation task. Getting user ' +
    'sign-off on your approach before writing code prevents wasted rework and ensures alignment. ' +
    'Enter or exit plan mode. In plan mode, writable tools are blocked so you can safely explore the codebase, ' +
    'create tasks, and design your implementation plan before executing.\n' +
    'WHEN TO USE — prefer plan mode for implementation tasks unless they are trivial. Use it when ANY apply:\n' +
    '  1. New feature implementation (meaningful new functionality).\n' +
    '  2. Multiple valid approaches (the task can be solved several ways).\n' +
    '  3. Code modifications that change existing behavior or structure.\n' +
    '  4. Architectural decisions (choosing between patterns or technologies).\n' +
    '  5. Multi-file changes (likely more than 2-3 files).\n' +
    '  6. Unclear requirements that need exploration before scoping.\n' +
    '  7. User preferences matter (if you would AskUserQuestion about the approach, enter plan mode instead).\n' +
    'WHEN NOT TO USE — skip plan mode for simple tasks: single/few-line fixes (typos, obvious bugs), a single ' +
    'function with clear requirements, very specific detailed instructions, or pure research/read-only tasks ' +
    'that produce no code change (explore directly instead — plan mode just adds a round-trip).\n' +
    'Workflow after "enter": 1) explore with Read/grep/glob and read-only Bash; 2) resolve open questions with ' +
    'AskUserQuestion BEFORE exiting (do not ask "is this plan okay?" — the exit step handles approval); ' +
    '3) write a concise plan to the plan file; 4) call "exit" to submit it for approval. ' +
    'Keep the plan tight — a reviewable outline, not a dump. ' +
    'Exit requires user approval; the plan content is shown to the user for review, and implementation begins only after approval.\n' +
    '## Examples\n' +
    'GOOD (enter plan mode): "Add user authentication" (session vs JWT, token storage, middleware structure); ' +
    '"Optimize the database queries" (profile first, several approaches); "Implement dark mode" (theme system ' +
    'spanning many components).\n' +
    'BAD (skip plan mode): "Fix this typo"; "Rename this variable"; "Explain how auth works" (read-only research).\n' +
    'Example flow: enter → inspect auth module → AskUserQuestion about the token store → write plan → exit.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['enter', 'exit', 'verify'],
        description: 'Whether to enter, exit, or verify plan mode.',
      },
      allowedPrompts: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            tool: { type: 'string', enum: ['Bash'], description: 'Tool the prompt targets (only Bash may be pre-approved).' },
            prompt: {
              type: 'string',
              description: 'The exact Bash command the plan needs to run during implementation (e.g. "npm run build"). Added verbatim as a session permission rule on exit.',
            },
          },
          required: ['tool', 'prompt'],
          additionalProperties: false,
        },
        description: 'Bash commands the plan needs for implementation (added as session permission rules on exit). Only Bash prompts are allowed.',
      },
      plan: {
        type: 'string',
        description: 'Optional plan content to submit on exit, overriding the plan file (e.g. a user-edited plan).',
      },
    },
    required: ['action'],
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>, context?: ToolContext): Promise<ToolResult> {
    const action = input.action as string;

    if (action === 'enter') {
      // Defense-in-depth: plan mode governs the top-level session; a sub-agent
      // must not flip the shared flag.
      if (context?.agentId) {
        return { output: 'Plan mode cannot be entered from a sub-agent context.', isError: true };
      }
      if (_planModeActive) {
        return { output: 'Already in plan mode.', isError: true };
      }
      _planModeActive = true;
      // A new plan session starts fresh: drop any plan submitted in a prior
      // session so getPlan() never reports a stale plan before this one is
      // written/submitted (it falls back to the fresh plan file on disk).
      _lastPlan = null;
      const cwd = context?.cwd || process.cwd();
      let planDir: string;
      try {
        planDir = getPlansDirectory(cwd);
        fs.mkdirSync(planDir, { recursive: true });
      } catch {
        planDir = projectStateDir(cwd, 'plans');
      }
      _planFilePath = allocatePlanFile(planDir, { sessionId: context?.sessionId });
      // Plan mode V2: the interview phase leads with clarifying questions and
      // parallel exploration; otherwise use the linear exploration workflow.
      const body = isPlanModeInterviewPhaseEnabled()
        ? `Interview phase: first clarify requirements with AskUserQuestion, then explore. You may run up to ` +
          `${getPlanAgentCount()} parallel read-only exploration pass(es) (Read, read-only Bash, grep, glob) ` +
          'before writing a concise plan to the plan file and calling exit to submit it for approval.'
        : 'Writable tools are now blocked. Explore with Read, read-only Bash, grep, glob.\n' +
          'Exploration workflow:\n' +
          '  1. Read the relevant code and tests.\n' +
          '  2. Resolve open questions with AskUserQuestion before exiting.\n' +
          '  3. Write a concise plan to the plan file.\n' +
          '  4. Call exit to submit the plan for approval.';
      return {
        output: `Entered plan mode. Plan file: ${_planFilePath}\n${body}`,
        isError: false,
      };
    }

    if (action === 'exit') {
      if (!_planModeActive) {
        return { output: 'Not in plan mode. Use action "enter" first.', isError: true };
      }
      const override = typeof input.plan === 'string' ? input.plan : undefined;
      const planWasEdited = override !== undefined;
      let planContent = override ?? '';
      if (override === undefined && _planFilePath) {
        try {
          planContent = fs.readFileSync(_planFilePath, 'utf-8');
        } catch {
          planContent = '';
        }
      }
      _planModeActive = false;
      const planPath = _planFilePath;
      _planFilePath = null;

      // Empty-plan special case: nothing to review — skip the approval framing.
      if (!planContent.trim()) {
        _lastPlan = null;
        return {
          output: `Exited plan mode with an empty plan (nothing submitted for approval). Plan file: ${planPath}`,
          isError: false,
        };
      }

      _lastPlan = planContent;
      let echoed = planContent;
      if (echoed.length > PLAN_MAX_RESULT_CHARS) {
        echoed = echoed.slice(0, PLAN_MAX_RESULT_CHARS) + '\n[... plan truncated ...]';
      }
      return {
        output:
          `Exiting plan mode. Plan submitted for user approval${planWasEdited ? ' (user-edited)' : ''}.\n\n` +
          `Plan file: ${planPath}\n\n--- Plan Content ---\n${echoed}\n--- End Plan ---\n\n` +
          'Next: await user approval. Once approved, implement the plan step by step and track progress with todo_write.',
        isError: false,
      };
    }

    if (action === 'verify') {
      if (!_planModeActive) {
        return { output: 'Not in plan mode.', isError: false };
      }
      let planContent = '';
      if (_planFilePath) {
        try { planContent = fs.readFileSync(_planFilePath, 'utf-8'); } catch { /* no file yet */ }
      }
      let status: string;
      if (!planContent) {
        status = 'Plan file is empty — write your plan before exiting.';
      } else if (planContent.length > PLAN_SOFT_CAP_CHARS) {
        status = `Plan is long (${planContent.length} chars, soft cap ${PLAN_SOFT_CAP_CHARS}) — trim it to a concise, reviewable outline before exiting.`;
      } else {
        status = 'Plan has content.';
      }
      return {
        output: `Plan mode active.\nPlan file: ${_planFilePath}\nPlan length: ${planContent.length} chars\n${status}`,
        isError: false,
      };
    }

    return { output: `Unknown action "${action}". Use enter, exit, or verify.`, isError: true };
  },
};
