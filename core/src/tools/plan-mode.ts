import { ToolDefinition, ToolResult } from '../types';

export const planModeTool: ToolDefinition = {
  name: 'plan_mode',
  description:
    'Enter or exit plan mode. In plan mode, writable tools are blocked so you can safely explore the codebase, ' +
    'create tasks, and design your implementation plan before executing. ' +
    'Use action "enter" before complex tasks, and "exit" when ready to implement.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        enum: ['enter', 'exit'],
        description: 'Whether to enter or exit plan mode.',
      },
    },
    required: ['action'],
  },
  isReadOnly: true,

  async execute(): Promise<ToolResult> {
    return { output: 'plan_mode is handled by the engine directly.', isError: false };
  },
};
