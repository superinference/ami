import type { EngineEvent } from '../types';

/**
 * Build a compact, human-readable argument summary for a sub-agent's tool call so
 * the live roster shows "bash npm test" or "file_read src/x.ts" instead of just
 * the tool name. Picks the most salient field per tool and truncates. Returns ''
 * when there's nothing useful to show (keeps the bare tool name).
 */
export function summarizeToolInput(toolName: string, input: unknown): string {
  if (!input || typeof input !== 'object') return '';
  const o = input as Record<string, unknown>;
  const pick = (...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = o[k];
      if (typeof v === 'string' && v.trim()) return v.trim();
    }
    return undefined;
  };
  // Most salient argument per tool family, with sensible fallbacks.
  const raw =
    pick('command') ??
    pick('file_path', 'path', 'notebook_path') ??
    pick('pattern', 'query', 'regex') ??
    pick('url') ??
    pick('prompt', 'subject', 'description') ??
    undefined;
  if (!raw) return '';
  const oneLine = raw.replace(/\s+/g, ' ').trim();
  return oneLine.length > 80 ? `${oneLine.slice(0, 77)}...` : oneLine;
}

/**
 * Pure mapper: translate a child sub-engine's EngineEvent into a parent-facing
 * `subagent_progress` event tagged with the child's node id + depth, or null for
 * events that should not be relayed (everything the live roster doesn't render).
 * Kept pure + separate from task.ts so the relay policy is fully unit-testable.
 */
export function mapChildToSubagentEvent(
  child: EngineEvent,
  nodeId: string,
  depth: number,
): EngineEvent | null {
  switch (child.type) {
    case 'text_delta':
      return child.text
        ? { type: 'subagent_progress', nodeId, depth, activity: 'text', detail: child.text }
        : null;
    case 'thinking_delta':
      return child.text
        ? { type: 'subagent_progress', nodeId, depth, activity: 'thinking', detail: child.text }
        : null;
    case 'tool_use_start': {
      const arg = summarizeToolInput(child.toolName, child.input);
      return { type: 'subagent_progress', nodeId, depth, activity: 'tool', detail: arg ? `${child.toolName} ${arg}` : child.toolName };
    }
    case 'tool_use_result':
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: `${child.toolName} ${child.isError ? '✗' : '✓'}`,
      };
    default:
      return null;
  }
}
