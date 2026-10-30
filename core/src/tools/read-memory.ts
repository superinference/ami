import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { MemoryManager } from '../memory';

/**
 * read_memory — expand a single stored memory by name. When the Stored Memories
 * section is rendered as an INDEX (large memory sets), this fetches the full body
 * of a named entry on demand instead of shipping every body in the prompt.
 */
export const readMemoryTool: ToolDefinition = {
  name: 'read_memory',
  description:
    "Read the full body of a stored memory by name. Use this to expand an entry listed in the '## Stored Memories (index ...)' section.",
  inputSchema: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        description: 'The memory name exactly as shown in the Stored Memories index.',
      },
    },
    required: ['name'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const name = typeof input.name === 'string' ? input.name.trim() : '';
    if (!name) {
      return { output: 'Error: name must not be empty.', isError: true };
    }

    const body = new MemoryManager(context.cwd).readMemory(name);
    if (body === null) {
      return { output: `Error: no stored memory named "${name}".`, isError: true };
    }
    return { output: `## Memory: ${name}\n\n${body}` };
  },
};
