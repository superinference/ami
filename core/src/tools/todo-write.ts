import { ToolDefinition, ToolResult, ToolContext } from '../types';

interface TodoItem {
  id: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled';
}

let sessionTodos: TodoItem[] = [];

export function resetTodos(): void { sessionTodos = []; }
export function getTodos(): TodoItem[] { return [...sessionTodos]; }

export const todoWriteTool: ToolDefinition = {
  name: 'todo_write',
  description: 'Create or update a structured todo list for the current task. Use to track multi-step work.',
  inputSchema: {
    type: 'object',
    properties: {
      todos: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            id: { type: 'string' },
            content: { type: 'string' },
            status: { type: 'string', enum: ['pending', 'in_progress', 'completed', 'cancelled'] },
          },
          required: ['id', 'content', 'status'],
        },
        description: 'Array of todo items. Replaces the current list.',
      },
      merge: { type: 'boolean', description: 'If true, merge with existing todos by id. If false, replace all.' },
    },
    required: ['todos'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>): Promise<ToolResult> {
    const items = (input.todos as TodoItem[]) ?? [];
    const merge = input.merge as boolean ?? false;
    const oldTodos = [...sessionTodos];
    if (merge) {
      for (const item of items) {
        const idx = sessionTodos.findIndex(t => t.id === item.id);
        if (idx >= 0) sessionTodos[idx] = { ...sessionTodos[idx], ...item };
        else sessionTodos.push(item);
      }
    } else {
      sessionTodos = items;
    }
    let summary = sessionTodos.map(t => {
      const icon = t.status === 'completed' ? '[x]' : t.status === 'in_progress' ? '[>]' : t.status === 'cancelled' ? '[-]' : '[ ]';
      return `${icon} ${t.id}: ${t.content}`;
    }).join('\n');

    const completedCount = sessionTodos.filter(t => t.status === 'completed').length;
    if (completedCount >= 3) {
      summary += '\n\n[Verification suggested: 3+ items completed. Consider reviewing the results.]';
    }

    const changed = items.map(i => i.id);
    return {
      output: `Todos updated (${sessionTodos.length} items, ${changed.length} changed):\n${summary}`,
      metadata: { oldTodos: oldTodos.map(t => t.id), newTodos: sessionTodos.map(t => t.id) },
    } as ToolResult;
  },
};
