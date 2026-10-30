import type { EngineEvent } from '../types';

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
    case 'tool_use_start':
      return { type: 'subagent_progress', nodeId, depth, activity: 'tool', detail: child.toolName };
    case 'tool_use_result':
      return {
        type: 'subagent_progress', nodeId, depth, activity: 'tool',
        detail: `${child.toolName} ${child.isError ? '✗' : '✓'}`,
      };
    default:
      return null;
  }
}
