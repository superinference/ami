import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { MemoryManager, MEMORY_TYPES, type MemoryType } from '../memory';

/**
 * write_memory — persist a durable memory for future sessions. The read side
 * (read_memory) and the engine could already surface memories, but the model had
 * no way to AUTHOR one, so "remember X" was unfulfillable. This wraps
 * MemoryManager.saveMemory with a typed, validated tool surface. (H6)
 */
export const writeMemoryTool: ToolDefinition = {
  name: 'write_memory',
  description:
    "Persist a durable memory so it survives across sessions. Use when the user asks you to remember something, or when you learn a durable fact worth keeping (a preference, a project constraint, a reference). Provide a short kebab-case `name`, the `content` to store, an optional one-line `description` (shown in the memory index), and an optional `type` (user | feedback | project | reference). Do not store secrets or anything already obvious from the code/git history.",
  inputSchema: {
    type: 'object',
    properties: {
      name: { type: 'string', description: 'Short kebab-case identifier, e.g. "deploy-command".' },
      content: { type: 'string', description: 'The memory body — the fact or instruction to store.' },
      description: { type: 'string', description: 'One-line summary used in the memory index (optional).' },
      type: {
        type: 'string',
        enum: [...MEMORY_TYPES],
        description: 'Memory category (default: project). user=about the user; feedback=how to work; project=ongoing context; reference=external pointers.',
      },
    },
    required: ['name', 'content'],
  },
  isReadOnly: false,
  isConcurrencySafe: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const name = typeof input.name === 'string' ? input.name.trim() : '';
    const content = typeof input.content === 'string' ? input.content : '';
    if (!name) {
      return { output: 'Error: name must not be empty.', isError: true };
    }
    if (!content.trim()) {
      return { output: 'Error: content must not be empty.', isError: true };
    }
    const description = typeof input.description === 'string' && input.description.trim()
      ? input.description.trim()
      : undefined;
    const rawType = typeof input.type === 'string' ? input.type : undefined;
    const type: MemoryType | undefined =
      rawType && (MEMORY_TYPES as readonly string[]).includes(rawType) ? (rawType as MemoryType) : undefined;

    new MemoryManager(context.cwd).saveMemory(name, content, description, type);
    return { output: `Saved memory "${name}"${type ? ` (type: ${type})` : ''}.` };
  },
};
