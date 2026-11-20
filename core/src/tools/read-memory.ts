import { ToolDefinition, ToolContext, ToolResult } from '../types';
import {
  MemoryManager,
  sanitizeMemoryContent,
  MAX_ENTRYPOINT_LINES,
  MAX_ENTRYPOINT_BYTES,
} from '../memory';

/**
 * Build a staleness caveat for a memory body surfaced WITHOUT its own
 * system-reminder wrapper (this tool returns the body directly to the model).
 * Mirrors the reference's memoryFreshnessNote: fires only for memories older than a
 * day so the model treats a potentially-outdated fact with appropriate caution.
 */
export function memoryFreshnessNote(ageDays: number | undefined): string {
  if (ageDays === undefined || ageDays <= 1) return '';
  return `\n\n_Note: this memory was last updated ${ageDays} days ago; verify it is still accurate before relying on it._`;
}

/**
 * Line/byte caps for a single on-demand memory expansion. A memory body is
 * written by the model/agents and can grow large (an accidental or malicious
 * multi-hundred-KB entry), so one read_memory call must never flood the context.
 * Reuses the MEMORY.md entrypoint caps so the bound is consistent across surfaces.
 */
export const MAX_READ_MEMORY_LINES = MAX_ENTRYPOINT_LINES;
export const MAX_READ_MEMORY_BYTES = MAX_ENTRYPOINT_BYTES;

/**
 * Truncate a memory body to the line cap then the byte cap, appending a warning
 * that names which cap fired so the overflow is never silent and the model knows
 * to page with offset/limit. The byte cut is UTF-8-accurate (never splits a
 * multi-byte char). Returns the body unchanged when already within both caps.
 */
export function capMemoryBody(body: string): string {
  const lines = body.split('\n');
  let truncatedBy: 'lines' | 'bytes' | null = null;
  let out = body;
  if (lines.length > MAX_READ_MEMORY_LINES) {
    out = lines.slice(0, MAX_READ_MEMORY_LINES).join('\n');
    truncatedBy = 'lines';
  }
  if (Buffer.byteLength(out, 'utf-8') > MAX_READ_MEMORY_BYTES) {
    const buf = Buffer.from(out, 'utf-8').subarray(0, MAX_READ_MEMORY_BYTES);
    out = buf.toString('utf-8').replace(/�+$/, '');
    truncatedBy = 'bytes';
  }
  if (!truncatedBy) return body;
  const cap = truncatedBy === 'lines'
    ? `${MAX_READ_MEMORY_LINES}-line`
    : `${MAX_READ_MEMORY_BYTES}-byte`;
  return `${out}\n\n[memory body truncated to the ${cap} cap — re-read with offset/limit to see more]`;
}

/**
 * Return a line-range slice of a memory body. `offset` is a 0-based first line and
 * `limit` the maximum number of lines. Either may be omitted; with neither set the
 * body is returned unchanged. Lets the model page a large entry instead of pulling
 * the whole body into context at once.
 */
export function sliceMemoryBody(body: string, offset?: number, limit?: number): string {
  if (offset === undefined && limit === undefined) return body;
  const lines = body.split('\n');
  const start = offset ?? 0;
  const end = limit === undefined ? lines.length : start + limit;
  return lines.slice(start, end).join('\n');
}

/** Levenshtein edit distance (iterative, O(n*m)) for the did-you-mean suggestion. */
function editDistance(a: string, b: string): number {
  const prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    let diag = prev[0];
    prev[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const tmp = prev[j];
      prev[j] = Math.min(
        prev[j] + 1,
        prev[j - 1] + 1,
        diag + (a[i - 1] === b[j - 1] ? 0 : 1),
      );
      diag = tmp;
    }
  }
  return prev[b.length];
}

/**
 * Turn a not-found read into a self-correcting hint instead of a dead end: pick the
 * closest known name (case-insensitive edit distance within a small threshold) and,
 * when any names exist, list the available ones so the model can retry without
 * re-guessing blind. Mirrors the reference's FileReadTool "Did you mean …?" fallback.
 */
export function suggestMemoryNames(name: string, names: string[]): string {
  const available = names.map((n) => n.trim()).filter(Boolean);
  if (available.length === 0) return '';
  const target = name.trim().toLowerCase();
  let best: string | null = null;
  let bestDist = Infinity;
  for (const n of available) {
    const dist = editDistance(target, n.toLowerCase());
    if (dist < bestDist) {
      bestDist = dist;
      best = n;
    }
  }
  // Only suggest a near-match when it is actually close to what was asked.
  const threshold = Math.max(2, Math.floor(target.length / 3));
  const didYouMean = best && bestDist <= threshold ? ` Did you mean "${best}"?` : '';
  return `${didYouMean} Available memories: ${available.join(', ')}.`;
}

/**
 * read_memory — expand a single stored memory by name. When the Stored Memories
 * section is rendered as an INDEX (large memory sets), this fetches the full body
 * of a named entry on demand instead of shipping every body in the prompt. The
 * body is page-able (offset/limit) and bounded (line/byte cap) so one call can
 * never flood the context.
 */
export const readMemoryTool: ToolDefinition = {
  name: 'read_memory',
  description:
    "Read the body of a stored memory by name. Use this to expand an entry listed in the '## Stored Memories (index ...)' section. Pass offset/limit to page through a large entry.",
  inputSchema: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        description: 'The memory name exactly as shown in the Stored Memories index.',
      },
      offset: {
        type: 'integer',
        description: 'Optional 0-based first line to return (for paging a large memory body).',
      },
      limit: {
        type: 'integer',
        description: 'Optional maximum number of lines to return (for paging a large memory body).',
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
    const offset =
      typeof input.offset === 'number' && Number.isInteger(input.offset) && input.offset >= 0
        ? input.offset
        : undefined;
    const limit =
      typeof input.limit === 'number' && Number.isInteger(input.limit) && input.limit >= 1
        ? input.limit
        : undefined;

    const manager = new MemoryManager(context.cwd);
    const memories = manager.loadMemories();
    const target = name.toLowerCase();
    const entry = memories.find((m) => (m.name || '').trim().toLowerCase() === target);
    if (!entry) {
      const names = memories.map((m) => m.name || '');
      return {
        output: `Error: no stored memory named "${name}".${suggestMemoryNames(name, names)}`,
        isError: true,
      };
    }

    const full = sanitizeMemoryContent(entry.content);
    // Page first (if requested), then bound the result so no single call floods
    // the context; finally attach a staleness caveat for an aged memory.
    const body = capMemoryBody(sliceMemoryBody(full, offset, limit));
    const note = memoryFreshnessNote(entry.ageDays);
    return { output: `## Memory: ${name}\n\n${body}${note}` };
  },
};
