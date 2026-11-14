import * as fs from 'fs';
import { projectStateDir } from '../reference-journal';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';

let _planModeActive = false;
let _planFilePath: string | null = null;
/** Last plan content submitted on exit — used as a fallback when a rejected
 *  plan must be re-rendered but the file is gone. */
let _lastPlan: string | null = null;

/** Max retries when a slug-based plan filename collides before falling back. */
export const MAX_SLUG_RETRIES = 5;
/** Hard cap on how much plan content is echoed back into the context on exit. */
export const PLAN_MAX_RESULT_CHARS = 100_000;

export function isPlanModeActive(): boolean { return _planModeActive; }
export function setPlanModeActive(active: boolean): void { _planModeActive = active; }
export function getPlanFilePath(): string | null { return _planFilePath; }
/** Recover the last submitted plan content (fallback on rejection). */
export function getPlan(): string | null { return _lastPlan; }

/**
 * Resolve (and create) the plans directory for a cwd, rejecting any path that
 * escapes the per-project state dir (defense against traversal via a poisoned
 * cwd). Returns the validated absolute directory.
 */
export function getPlansDirectory(cwd: string): string {
  const base = projectStateDir(cwd, 'plans');
  const resolved = path.resolve(base);
  const root = path.resolve(projectStateDir(cwd, ''));
  if (!resolved.startsWith(root)) {
    throw new Error('Refusing to use a plans directory outside the project state dir.');
  }
  return resolved;
}

const SLUG_WORDS = ['amber', 'basalt', 'cobalt', 'cedar', 'delta', 'ember', 'flint', 'garnet', 'harbor', 'ivory'];

/** Pick a collision-free, human-recognizable plan filename within planDir. */
export function allocatePlanFile(planDir: string): string {
  for (let i = 0; i < MAX_SLUG_RETRIES; i++) {
    const word = SLUG_WORDS[Math.floor(Math.random() * SLUG_WORDS.length)];
    const candidate = path.join(planDir, `plan-${word}-${Date.now().toString(36)}.md`);
    if (!fs.existsSync(candidate)) return candidate;
  }
  // Fallback: a timestamp+random name is effectively collision-proof.
  return path.join(planDir, `plan-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.md`);
}

export const planModeTool: ToolDefinition = {
  name: 'plan_mode',
  description:
    'Enter or exit plan mode. In plan mode, writable tools are blocked so you can safely explore the codebase, ' +
    'create tasks, and design your implementation plan before executing.\n' +
    'WHEN TO USE: complex or multi-step tasks where you should research and agree on an approach before changing ' +
    'anything — large refactors, ambiguous requirements, or anything where a wrong first move is costly. ' +
    'WHEN NOT TO USE: trivial, single-step, or purely read-only requests — entering plan mode just adds a round-trip.\n' +
    'Workflow after "enter": 1) explore with Read/grep/glob and read-only Bash; 2) resolve open questions with ' +
    'AskUserQuestion BEFORE exiting (do not ask "is this plan okay?" — the exit step handles approval); ' +
    '3) write a concise plan to the plan file; 4) call "exit" to submit it for approval. ' +
    'Keep the plan tight — a reviewable outline, not a dump. ' +
    'Exit requires user approval; the plan content is shown to the user for review, and implementation begins only after approval. ' +
    'Example: enter → inspect auth module → AskUserQuestion about the token store → write plan → exit.',
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
            prompt: { type: 'string' },
          },
          required: ['tool', 'prompt'],
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
      const cwd = context?.cwd || process.cwd();
      let planDir: string;
      try {
        planDir = getPlansDirectory(cwd);
        fs.mkdirSync(planDir, { recursive: true });
      } catch {
        planDir = projectStateDir(cwd, 'plans');
      }
      _planFilePath = allocatePlanFile(planDir);
      return {
        output:
          `Entered plan mode. Plan file: ${_planFilePath}\n` +
          'Writable tools are now blocked. Explore with Read, read-only Bash, grep, glob.\n' +
          'Exploration workflow:\n' +
          '  1. Read the relevant code and tests.\n' +
          '  2. Resolve open questions with AskUserQuestion before exiting.\n' +
          '  3. Write a concise plan to the plan file.\n' +
          '  4. Call exit to submit the plan for approval.',
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
      return {
        output: `Plan mode active.\nPlan file: ${_planFilePath}\nPlan length: ${planContent.length} chars\n${planContent ? 'Plan has content.' : 'Plan file is empty — write your plan before exiting.'}`,
        isError: false,
      };
    }

    return { output: `Unknown action "${action}". Use enter, exit, or verify.`, isError: true };
  },
};
