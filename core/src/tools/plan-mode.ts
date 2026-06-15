import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';

let _planModeActive = false;
let _planFilePath: string | null = null;

export function isPlanModeActive(): boolean { return _planModeActive; }
export function setPlanModeActive(active: boolean): void { _planModeActive = active; }
export function getPlanFilePath(): string | null { return _planFilePath; }

export const planModeTool: ToolDefinition = {
  name: 'plan_mode',
  description:
    'Enter or exit plan mode. In plan mode, writable tools are blocked so you can safely explore the codebase, ' +
    'create tasks, and design your implementation plan before executing. ' +
    'Use action "enter" before complex tasks, and "exit" when ready to implement. ' +
    'Exit requires user approval — the plan content is shown to the user for review.',
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
        items: { type: 'object', properties: { tool: { type: 'string' }, prompt: { type: 'string' } } },
        description: 'Bash commands the plan needs for implementation (added as session permission rules on exit).',
      },
    },
    required: ['action'],
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>, context?: ToolContext): Promise<ToolResult> {
    const action = input.action as string;

    if (action === 'enter') {
      if (_planModeActive) {
        return { output: 'Already in plan mode.', isError: true };
      }
      _planModeActive = true;
      const cwd = context?.cwd || process.cwd();
      const planDir = path.join(cwd, '.superinference', 'plans');
      try { fs.mkdirSync(planDir, { recursive: true }); } catch { /* ok */ }
      _planFilePath = path.join(planDir, `plan-${Date.now()}.md`);
      return { output: `Entered plan mode. Plan file: ${_planFilePath}\nWritable tools are now blocked. Use Read, Bash (read-only), grep, glob to explore.\nWrite your plan to the plan file, then call exit to submit for approval.`, isError: false };
    }

    if (action === 'exit') {
      if (!_planModeActive) {
        return { output: 'Not in plan mode. Use action "enter" first.', isError: true };
      }
      let planContent = '';
      if (_planFilePath) {
        try {
          planContent = fs.readFileSync(_planFilePath, 'utf-8');
        } catch {
          planContent = '[No plan file found — write your plan before exiting]';
        }
      }
      _planModeActive = false;
      const planPath = _planFilePath;
      _planFilePath = null;
      return {
        output: `Exiting plan mode. Plan submitted for user approval.\n\nPlan file: ${planPath}\n\n--- Plan Content ---\n${planContent}\n--- End Plan ---\n\nAwaiting user approval before implementation begins.`,
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
