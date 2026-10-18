export interface ToolConflict {
  toolName: string;
  existingSource: string;
  conflictingSource: string;
  reason: string;
}

export const RESERVED_TOOL_PREFIXES = [
  'file_read', 'file_write', 'file_edit',
  'bash', 'grep', 'glob', 'list_dir',
  'web_fetch', 'web_search',
  'notebook_edit',
  'task_', 'agent_',
] as const;

export function detectToolConflict(
  toolName: string,
  source: string,
  existingTools: Map<string, string>,
): ToolConflict | null {
  const existingSource = existingTools.get(toolName);
  if (existingSource && existingSource !== source) {
    return {
      toolName,
      existingSource,
      conflictingSource: source,
      reason: `Tool "${toolName}" already registered by "${existingSource}"`,
    };
  }

  if (source !== 'core' && source !== 'builtin') {
    for (const prefix of RESERVED_TOOL_PREFIXES) {
      const boundary = prefix.endsWith('_') ? prefix : prefix + '_';
      if (toolName === prefix || toolName.startsWith(boundary)) {
        return {
          toolName,
          existingSource: 'core',
          conflictingSource: source,
          reason: `Tool "${toolName}" conflicts with reserved prefix "${prefix}"`,
        };
      }
    }
  }

  return null;
}

export function isReservedToolName(name: string): boolean {
  for (const prefix of RESERVED_TOOL_PREFIXES) {
    const boundary = prefix.endsWith('_') ? prefix : prefix + '_';
    if (name === prefix || name.startsWith(boundary)) return true;
  }
  return false;
}

export class ToolRegistryGuard {
  private _registry = new Map<string, string>();
  private _conflicts: ToolConflict[] = [];

  register(toolName: string, source: string): ToolConflict | null {
    const conflict = detectToolConflict(toolName, source, this._registry);
    if (conflict) {
      this._conflicts.push(conflict);
      return conflict;
    }
    this._registry.set(toolName, source);
    return null;
  }

  isRegistered(toolName: string): boolean {
    return this._registry.has(toolName);
  }

  getSource(toolName: string): string | undefined {
    return this._registry.get(toolName);
  }

  getConflicts(): readonly ToolConflict[] {
    return this._conflicts;
  }

  getRegisteredTools(): Map<string, string> {
    return new Map(this._registry);
  }

  clear(): void {
    this._registry.clear();
    this._conflicts = [];
  }
}
