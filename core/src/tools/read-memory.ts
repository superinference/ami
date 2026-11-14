import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { MemoryManager } from '../memory';

/**
 * Build a staleness caveat for a memory body surfaced WITHOUT its own
 * system-reminder wrapper (this tool returns the body directly to the model).
 * Mirrors Claude's memoryFreshnessNote: fires only for memories older than a
 * day so the model treats a potentially-outdated fact with appropriate caution.
 */
export function memoryFreshnessNote(ageDays: number | undefined): string {
  if (ageDays === undefined || ageDays <= 1) return '';
  return `\n\n_Note: this memory was last updated ${ageDays} days ago; verify it is still accurate before relying on it._`;
}

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

    const manager = new MemoryManager(context.cwd);
    const body = manager.readMemory(name);
    if (body === null) {
      return { output: `Error: no stored memory named "${name}".`, isError: true };
    }
    // Locate the entry's age so a stale memory carries a freshness caveat even
    // though we emit the body directly (no surrounding wrapper to add one).
    const target = name.trim().toLowerCase();
    const entry = manager.loadMemories().find((m) => (m.name || '').trim().toLowerCase() === target);
    const note = memoryFreshnessNote(entry?.ageDays);
    return { output: `## Memory: ${name}\n\n${body}${note}` };
  },
};
