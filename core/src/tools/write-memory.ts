import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { MemoryManager, MEMORY_TYPES, MEMORY_SCOPES, type MemoryType, type MemoryScope } from '../memory';
import { findCredentials } from '../credential-guard';

/**
 * write_memory — persist a durable memory for future sessions. The read side
 * (read_memory) and the engine could already surface memories, but the model had
 * no way to AUTHOR one, so "remember X" was unfulfillable. This wraps
 * MemoryManager.saveMemory with a typed, validated tool surface. (H6)
 */
export const writeMemoryTool: ToolDefinition = {
  name: 'write_memory',
  description:
    "Persist a durable memory so it survives across sessions. Use when the user asks you to remember something, or when you learn a durable fact worth keeping (a preference, a project constraint, a reference).\n" +
    'Provide a short kebab-case `name`, the `content` to store, an optional one-line `description` (shown in the memory index, keep it under ~150 chars — one line, no newlines), and an optional `type` (user | feedback | project | reference). ' +
    'user=about the user; feedback=how to work; project=ongoing context; reference=external pointers.\n' +
    'Pick a `scope`: private (default — known only to you and the current user) or team (shared with and contributed to by EVERYONE working in this project directory, synced each session). Use team for durable project/reference context useful to every collaborator; keep user-specific preferences private. ' +
    'NEVER save secrets or user credentials in a team memory — team memories are visible to all collaborators.\n' +
    'Body structure: for feedback and project memories, lead with the rule/fact, then a `**Why:**` line (the originating reason) and a `**How to apply:**` line (when/where it applies). ' +
    'For project memories, convert relative dates to absolute (e.g. "Thursday" -> "2026-03-05") so the memory stays interpretable later.\n' +
    'Before writing, check the existing memory index and UPDATE an existing entry rather than creating a near-duplicate. ' +
    'Do NOT store secrets (API keys, tokens, passwords), or anything already obvious from the code/git history; these exclusions apply even when the user explicitly asks you to save them.',
  inputSchema: {
    type: 'object',
    properties: {
      name: { type: 'string', description: 'Short kebab-case identifier, e.g. "deploy-command".' },
      content: { type: 'string', description: 'The memory body — the fact or instruction to store.' },
      description: { type: 'string', description: 'One-line summary used in the memory index (optional, under ~150 chars).' },
      type: {
        type: 'string',
        enum: [...MEMORY_TYPES],
        description: 'Memory category (default: project). user=about the user; feedback=how to work; project=ongoing context; reference=external pointers.',
      },
      scope: {
        type: 'string',
        enum: [...MEMORY_SCOPES],
        description: 'Storage scope (default: private). private=only you and the current user; team=shared with everyone working in this project directory (synced each session). NEVER put secrets or user credentials in a team memory.',
      },
      skipIndex: {
        type: 'boolean',
        description:
          'When true, do a ONE-step save: write the memory file only and do NOT append a pointer to the directory\'s MEMORY.md index. Default false — the two-step save also indexes the memory so the always-loaded MEMORY.md stays in sync with what was written.',
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

    // Scope defaults to 'private'; an unrecognized value falls back to private
    // (mirrors the invalid-type fallback) so a bad model arg never mis-routes a
    // write into the shared team directory. Resolved BEFORE the secret guard so
    // the rejection message can be scope-aware (a team leak is worse — it is
    // shared with every collaborator).
    const rawScope = typeof input.scope === 'string' ? input.scope : undefined;
    const scope: MemoryScope =
      rawScope && (MEMORY_SCOPES as readonly string[]).includes(rawScope) ? (rawScope as MemoryScope) : 'private';

    // Secret-leak guard: a durable memory persists across sessions and may be
    // committed/synced, so credentials must never be written to it. Block the
    // write and name only the KIND(s) of credential detected (the rule label,
    // e.g. "Anthropic Key") so the model knows what to remove — but NEVER echo
    // the matched value itself. Mirrors Claude's team-memory secret guard, which
    // records the gitleaks ruleId and never the secret. For a TEAM-scoped write
    // the consequence is sharper (teamMemSecretGuard.ts): the file would be
    // shared with every repository collaborator, so the message says so.
    const secrets = findCredentials(content);
    if (secrets.length > 0) {
      const kinds = [...new Set(secrets.map(s => s.label))].sort();
      const sharedNote = scope === 'team'
        ? 'Team memory is shared with all repository collaborators'
        : 'Memories persist across sessions and may be shared';
      return {
        output:
          `Error: refusing to store this memory — it contains ${secrets.length} ` +
          `detected secret${secrets.length === 1 ? '' : 's'} (${kinds.join(', ')}). ` +
          `${sharedNote}; remove the sensitive value and try again.`,
        isError: true,
      };
    }

    const description = typeof input.description === 'string' && input.description.trim()
      ? input.description.trim()
      : undefined;
    const rawType = typeof input.type === 'string' ? input.type : undefined;
    const type: MemoryType | undefined =
      rawType && (MEMORY_TYPES as readonly string[]).includes(rawType) ? (rawType as MemoryType) : undefined;

    // Save mode: by default do Claude Code's TWO-step save (write the topic file
    // AND append a one-line pointer to the directory's MEMORY.md index so the
    // always-loaded index stays in sync). `skipIndex:true` selects the ONE-step
    // save (file only, no index pointer) — parity with extractMemories' skipIndex
    // branch (teamMemPrompts.ts L29-58).
    const skipIndex = input.skipIndex === true;
    const mgr = new MemoryManager(context.cwd);
    const savedPath = mgr.saveMemory(name, content, description, type, scope, !skipIndex);

    // Count-bearing confirmation (teamMemSaved.ts): after the save, report how
    // many memories now exist, split by scope, with singular/plural agreement and
    // zero-suppression of the team segment (parity with Claude's teamCount split).
    const counts = mgr.countMemoriesByScope();
    const pluralMem = (n: number) => (n === 1 ? 'memory' : 'memories');
    const countParts = [`${counts.private} private ${pluralMem(counts.private)}`];
    if (counts.team > 0) countParts.push(`${counts.team} team ${pluralMem(counts.team)}`);

    const meta = type ? `${scope}, type: ${type}` : scope;
    const indexNote = skipIndex ? '' : ' and indexed it in MEMORY.md';
    return {
      output:
        `Saved memory "${name}" (${meta}) to ${savedPath}${indexNote}. ` +
        `You now have ${countParts.join(' and ')}. View with /memory.`,
    };
  },
};
