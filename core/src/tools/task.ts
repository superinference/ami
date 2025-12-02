import { ToolDefinition, ToolContext, ToolResult, EngineConfig } from '../types';
import { ToolRegistry, createDefaultTools } from './index';
import { SessionManager } from '../session';

export const taskTool: ToolDefinition = {
  name: 'task',
  description:
    'Spawn a subagent to perform a specific task. The subagent runs in an isolated context with its own conversation. Use for parallel codebase exploration, research, or delegating independent subtasks. The subagent\'s final text response is returned as the result.',
  inputSchema: {
    type: 'object',
    properties: {
      prompt: {
        type: 'string',
        description: 'The task for the subagent to perform.',
      },
      mode: {
        type: 'string',
        description: 'Agent mode. "explore" gives read-only tools only. "general" gives all tools except task (no recursion).',
        enum: ['explore', 'general'],
      },
    },
    required: ['prompt'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const prompt = input.prompt as string;
    const mode = (input.mode as string) || 'explore';

    if (!prompt || prompt.trim().length === 0) {
      return { output: 'Error: prompt must not be empty.', isError: true };
    }

    const allTools = createDefaultTools(context.cwd);
    const readOnlyNames = new Set([
      'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
      'web_search', 'web_fetch',
    ]);

    let tools: ToolDefinition[];
    if (mode === 'explore') {
      tools = allTools.getAll().filter(t => readOnlyNames.has(t.name));
    } else {
      tools = allTools.getAll().filter(t => t.name !== 'task');
    }

    const subAbort = new AbortController();
    context.abortSignal.addEventListener('abort', () => subAbort.abort(), { once: true });

    const subConfig: EngineConfig = {
      provider: context._providerConfig || {
        baseUrl: '',
        apiKey: '',
        model: '',
      },
      cwd: context.cwd,
      tools,
      sessionId: SessionManager.newId(),
      permissionMode: mode === 'explore' ? 'auto-allow' : 'ask',
      permissionPromptHandler: context._permissionPromptHandler,
      abortController: subAbort,
    };

    if (!context._engineFactory) {
      return { output: 'Error: engine factory not available in this context.', isError: true };
    }
    const subEngine = context._engineFactory(subConfig);
    let result = '';

    try {
      for await (const event of subEngine.submit(prompt)) {
        if (event.type === 'text_delta') {
          result += event.text;
        }
        if (event.type === 'error') {
          result += `\nError: ${event.error}`;
        }
      }
    } catch (err) {
      return {
        output: `Subagent error: ${err instanceof Error ? err.message : String(err)}`,
        isError: true,
      };
    }

    return { output: result || '(subagent produced no output)', isError: false };
  },
};
