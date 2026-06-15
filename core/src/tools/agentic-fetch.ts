import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const agenticFetchTool: ToolDefinition = {
  name: 'agentic_fetch',
  description: 'Delegate web research to a subagent. The subagent searches the web and fetches relevant pages to answer your question, keeping the results out of your main context.',
  inputSchema: {
    type: 'object',
    properties: {
      query: { type: 'string', description: 'The research question or topic.' },
      max_pages: { type: 'number', description: 'Maximum pages to fetch (default 3).' },
    },
    required: ['query'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const query = input.query as string;
    const maxPages = (input.max_pages as number) || 3;

    if (!context._engineFactory) {
      return { output: 'Error: Engine factory not available for agentic fetch.', isError: true };
    }

    const subEngine = context._engineFactory({
      provider: context._providerConfig!,
      cwd: context.cwd,
      permissionMode: 'auto-allow',
      maxTurns: 5,
      tokenBudget: 16000,
    });

    let result = '';
    const prompt = `Search the web for: "${query}"\n\nUse web_search to find relevant pages, then web_fetch up to ${maxPages} of the most relevant results. Synthesize the key findings into a concise summary.`;

    for await (const event of subEngine.submit(prompt)) {
      if (event.type === 'text_delta') result += event.text;
    }
    subEngine.shutdown?.();

    return { output: result || `[No results found for: ${query}]` };
  },
};
