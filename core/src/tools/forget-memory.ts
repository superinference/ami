import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { MemoryManager } from '../memory';

/**
 * forget_memory — delete a durable memory the user asks to forget. write_memory
 * lets the model AUTHOR a memory and the save guidance tells it to "remove
 * memories that turn out to be wrong or outdated", but there was no surface to
 * actually do the removal — so "forget X" was unfulfillable. This wraps
 * MemoryManager.deleteMemory with a typed tool surface, mirroring Claude Code's
 * extractMemories "if they ask you to forget something, find and remove the
 * relevant entry" affordance. It removes the topic file in BOTH the private and
 * team scopes and prunes the MEMORY.md index pointer. (gap: extractMemories
 * prompts.ts L87 forget affordance)
 */
export const forgetMemoryTool: ToolDefinition = {
  name: 'forget_memory',
  description:
    'Delete a durable memory by name — the inverse of write_memory. Use when the user asks you to forget something, or when a saved memory turns out to be wrong or outdated and should be removed rather than updated.\n' +
    'Provide the `name` of the memory to remove (the kebab-case identifier or title it was saved under). The matching topic file is deleted from both private and team scope and its pointer is pruned from the MEMORY.md index. ' +
    'If no memory matches the name, nothing is removed and you are told so — check the memory index (/memory) for the exact name first.',
  inputSchema: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        description: 'The name/title of the memory to forget, e.g. "deploy-command".',
      },
    },
    required: ['name'],
  },
  isReadOnly: false,
  isConcurrencySafe: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const name = typeof input.name === 'string' ? input.name.trim() : '';
    if (!name) {
      return { output: 'Error: name must not be empty.', isError: true };
    }

    const mgr = new MemoryManager(context.cwd);
    const removed = mgr.deleteMemory(name);
    if (removed.length === 0) {
      return {
        output:
          `No memory named "${name}" was found — nothing removed. ` +
          `Check the exact name in the memory index with /memory.`,
        isError: true,
      };
    }

    const scopes = [...new Set(removed.map(r => r.scope))].sort();
    const plural = removed.length === 1 ? 'memory' : 'memories';

    // Count-bearing confirmation mirroring write_memory: after the delete, report
    // how many memories remain, split by scope, with singular/plural agreement and
    // zero-suppression of the team segment.
    const counts = mgr.countMemoriesByScope();
    const pluralMem = (n: number) => (n === 1 ? 'memory' : 'memories');
    const countParts = [`${counts.private} private ${pluralMem(counts.private)}`];
    if (counts.team > 0) countParts.push(`${counts.team} team ${pluralMem(counts.team)}`);

    return {
      output:
        `Forgot ${removed.length} ${plural} named "${name}" (${scopes.join(', ')}). ` +
        `You now have ${countParts.join(' and ')}. View with /memory.`,
    };
  },
};
