import { ToolDefinition, ToolInputSchema } from '../types';
import { bashTool } from './bash';
import { fileReadTool } from './file-read';
import { fileWriteTool } from './file-write';
import { fileEditTool } from './file-edit';
import { grepTool } from './grep';
import { globTool } from './glob';
import { listDirTool } from './list-dir';
import { webFetchTool } from './web-fetch';
import { webSearchTool } from './web-search';
import { notebookEditTool } from './notebook-edit';
import { searchSymbolsTool } from './search-symbols';
import { multiEditTool } from './multi-edit';
import { taskTool } from './task';
import { toolSearchTool, setSearchableTools } from './tool-search';
import { askUserQuestionTool } from './ask-user';
import { gitCommitTool } from './git-commit';
import { taskTrackerTool } from './task-tracker';
import { planModeTool } from './plan-mode';
import { taskOutputTool } from './task-output';
import { taskKillTool } from './task-kill';
import { taskListTool } from './task-list';
import { skillTool } from './skill';
import { cronCreateTool } from './cron-create';
import { cronDeleteTool } from './cron-delete';
import { cronListTool } from './cron-list';
import { scheduleWakeupTool } from './schedule-wakeup';
import { enterWorktreeTool } from './enter-worktree';
import { exitWorktreeTool } from './exit-worktree';
import { sendMessageTool } from './send-message';
import { sleepTool } from './sleep';
import { workflowTool } from './workflow';
import { briefTool } from './brief';
import { teamCreateTool, teamDeleteTool } from './team';
import { todoWriteTool } from './todo-write';
import { configTool } from './config-tool';
import { syntheticOutputTool } from './synthetic-output';
import { lspDiagnosticsTool, lspReferencesTool } from './lsp-diagnostics';
import { agenticFetchTool } from './agentic-fetch';
import { scoutTool } from './scout';

export class ToolRegistry {
  private tools: Map<string, ToolDefinition> = new Map();

  register(tool: ToolDefinition): void {
    this.tools.set(tool.name, tool);
  }

  get(name: string): ToolDefinition | undefined {
    return this.tools.get(name);
  }

  getAll(): ToolDefinition[] {
    return Array.from(this.tools.values());
  }

  toOpenAIFormat(): Array<{
    type: 'function';
    function: {
      name: string;
      description: string;
      parameters: ToolInputSchema;
    };
  }> {
    return this.getAll().map((tool) => ({
      type: 'function' as const,
      function: {
        name: tool.name,
        description: tool.description,
        parameters: tool.inputSchema,
      },
    }));
  }
}

export function createDefaultTools(_cwd: string): ToolRegistry {
  const registry = new ToolRegistry();

  registry.register(bashTool);
  registry.register(fileReadTool);
  registry.register(fileWriteTool);
  registry.register(fileEditTool);
  registry.register(grepTool);
  registry.register(globTool);
  registry.register(listDirTool);
  registry.register(webFetchTool);
  registry.register(webSearchTool);
  registry.register(notebookEditTool);
  registry.register(searchSymbolsTool);
  registry.register(multiEditTool);
  registry.register(taskTool);
  registry.register(toolSearchTool);
  registry.register(askUserQuestionTool);
  registry.register(gitCommitTool);
  registry.register(taskTrackerTool);
  registry.register(planModeTool);
  registry.register(taskOutputTool);
  registry.register(taskKillTool);
  registry.register(taskListTool);
  registry.register(skillTool);
  registry.register(cronCreateTool);
  registry.register(cronDeleteTool);
  registry.register(cronListTool);
  registry.register(scheduleWakeupTool);
  registry.register(enterWorktreeTool);
  registry.register(exitWorktreeTool);
  registry.register(sendMessageTool);
  registry.register(sleepTool);
  registry.register(workflowTool);
  registry.register(briefTool);
  registry.register(teamCreateTool);
  registry.register(teamDeleteTool);
  registry.register(todoWriteTool);
  registry.register(configTool);
  registry.register(syntheticOutputTool);
  registry.register(lspDiagnosticsTool);
  registry.register(lspReferencesTool);
  registry.register(agenticFetchTool);
  registry.register(scoutTool);

  setSearchableTools(registry.getAll());

  return registry;
}

// Re-export individual tools for direct access
export { bashTool } from './bash';
export { fileReadTool } from './file-read';
export { fileWriteTool } from './file-write';
export { fileEditTool } from './file-edit';
export { grepTool } from './grep';
export { globTool } from './glob';
export { listDirTool } from './list-dir';
export { webFetchTool } from './web-fetch';
export { webSearchTool } from './web-search';
export { notebookEditTool } from './notebook-edit';
export { searchSymbolsTool, getWorkspaceIndexer } from './search-symbols';
export { multiEditTool } from './multi-edit';
export { taskTool } from './task';
export { toolSearchTool, setSearchableTools } from './tool-search';
export { askUserQuestionTool } from './ask-user';
export { gitCommitTool } from './git-commit';
export { taskTrackerTool, resetTaskState } from './task-tracker';
export { planModeTool } from './plan-mode';
export { taskOutputTool } from './task-output';
export { taskKillTool } from './task-kill';
export { taskListTool } from './task-list';
export { skillTool } from './skill';
export { cronCreateTool, getCronScheduler, resetCronScheduler } from './cron-create';
export { cronDeleteTool } from './cron-delete';
export { cronListTool } from './cron-list';
export { scheduleWakeupTool } from './schedule-wakeup';
export { enterWorktreeTool } from './enter-worktree';
export { exitWorktreeTool } from './exit-worktree';
export { sendMessageTool } from './send-message';
export { getMailbox, clearMailbox, resetAllMailboxes } from './send-message';
export { sleepTool } from './sleep';
export { workflowTool } from './workflow';
export { briefTool } from './brief';
export { WorkflowRuntime, executeWorkflow, parseWorkflowMeta } from './workflow-runtime';
export type { WorkflowMeta, AgentHandler, AgentOptions, WorkflowBudget, WorkflowContext, WorkflowResolver } from './workflow-runtime';
export { parseBashAST, extractCommands, extractCommandsFromString } from './bash-parser';
export type { CommandNode } from './bash-parser';
export { shouldUseSandbox, wrapWithSandbox, getSandboxStatus, resetSandboxCache } from './bash-sandbox';
export type { SandboxConfig } from './bash-sandbox';
export { teamCreateTool, teamDeleteTool, getCurrentTeam, resetTeam } from './team';
export { todoWriteTool, resetTodos, getTodos } from './todo-write';
export { createMcpAuthTool } from './mcp-auth';
export { createMcpTool } from './mcp-tool';
export { listMcpResourcesTool, readMcpResourceTool } from './mcp-resources';
export { configTool } from './config-tool';
export { syntheticOutputTool } from './synthetic-output';
export { lspDiagnosticsTool, lspReferencesTool } from './lsp-diagnostics';
export { agenticFetchTool } from './agentic-fetch';
export { scoutTool } from './scout';
