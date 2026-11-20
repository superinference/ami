// Extracted leaf: terminal task-status predicate. Type-only import of TaskStatus
// (erased at runtime) so flow-manager.ts can import this without a
// flow-manager <-> process-manager RUNTIME cycle.
import type { TaskStatus } from './process-manager';

const TERMINAL_TASK_STATUSES = new Set<TaskStatus>(['completed', 'failed', 'killed']);

export function isTerminalTaskStatus(status: string | undefined): boolean {
  return !!status && TERMINAL_TASK_STATUSES.has(status as TaskStatus);
}
