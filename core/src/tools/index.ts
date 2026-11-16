import { ToolDefinition, ToolInputSchema } from '../types';
import { ToolRegistryGuard, type ToolConflict } from './tool-registry-guard';
import { bashTool } from './bash';
import { fileReadTool } from './file-read';
import { fileEditTool } from './file-edit';
import { grepTool } from './grep';
import { globTool } from './glob';
import { listDirTool } from './list-dir';
import { readMemoryTool } from './read-memory';
import { writeMemoryTool } from './write-memory';
import { webFetchTool } from './web-fetch';
import { webSearchTool } from './web-search';
import { notebookEditTool } from './notebook-edit';
import { searchSymbolsTool } from './search-symbols';
import { taskTool } from './task';
import { toolSearchTool, setSearchableTools } from './tool-search';
import { askUserQuestionTool } from './ask-user';
import { gitCommitTool } from './git-commit';
import { taskTrackerTool } from './task-tracker';
import { planModeTool } from './plan-mode';
import { taskCompleteTool } from './task-complete';
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
import { lspTool } from './lsp';
import { agenticFetchTool } from './agentic-fetch';
import { scoutTool } from './scout';
import { endConversationTool } from './end-conversation';

export class ToolRegistry {
  private tools: Map<string, ToolDefinition> = new Map();
  private _guard = new ToolRegistryGuard();

  register(tool: ToolDefinition, source = 'core'): void {
    // Pass the tool's declared aliases so the guard both collision-checks them and
    // can resolve a legacy/renamed name back to the canonical tool (parity: aliases).
    this._guard.register(tool.name, source, tool.aliases);
    this.tools.set(tool.name, tool);
  }

  get(name: string): ToolDefinition | undefined {
    // Resolve by primary name, then via an alias (back-compat for renamed tools).
    const canonical = this._guard.resolveToolName(name) ?? name;
    return this.tools.get(canonical);
  }

  getAll(): ToolDefinition[] {
    return Array.from(this.tools.values());
  }

  getConflicts(): readonly ToolConflict[] {
    return this._guard.getConflicts();
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
  registry.register(fileEditTool);
  registry.register(grepTool);
  registry.register(globTool);
  registry.register(listDirTool);
  registry.register(webFetchTool);
  registry.register(webSearchTool);
  registry.register(notebookEditTool);
  registry.register(searchSymbolsTool);
  registry.register(taskTool);
  registry.register(toolSearchTool);
  registry.register(askUserQuestionTool);
  registry.register(gitCommitTool);
  registry.register(taskTrackerTool);
  registry.register(planModeTool);
  registry.register(taskCompleteTool);
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
  registry.register(lspTool);
  registry.register(agenticFetchTool);
  registry.register(scoutTool);
  registry.register(endConversationTool);
  registry.register(readMemoryTool);
  registry.register(writeMemoryTool);

  setSearchableTools(registry.getAll());

  return registry;
}

// Re-export individual tools for direct access
export { bashTool } from './bash';
export { fileReadTool } from './file-read';
export { fileEditTool } from './file-edit';
export { grepTool } from './grep';
export { globTool } from './glob';
export { listDirTool } from './list-dir';
export { readMemoryTool } from './read-memory';
export { writeMemoryTool } from './write-memory';
export { webFetchTool } from './web-fetch';
export { webSearchTool } from './web-search';
export { notebookEditTool } from './notebook-edit';
export { searchSymbolsTool, getWorkspaceIndexer } from './search-symbols';
export { taskTool } from './task';
export { toolSearchTool, setSearchableTools, resolveSearchableTool, resolveSearchableSkill } from './tool-search';
export { askUserQuestionTool } from './ask-user';
export { gitCommitTool } from './git-commit';
export { taskTrackerTool, resetTaskState } from './task-tracker';
export { planModeTool } from './plan-mode';
export {
  taskCompleteTool,
  resetCompletedTasks,
  isTaskCompleted,
  normalizeTaskStatus,
  VALID_TASK_STATUSES,
} from './task-complete';
export { taskOutputTool } from './task-output';
export { taskKillTool } from './task-kill';
export {
  taskListTool,
  isAgentTask,
  isMonitorTask,
  agentDisplayName,
  formatTokenCount,
  formatElapsed,
  formatTaskStatus,
  summarizeTasks,
} from './task-list';
export { skillTool } from './skill';
export { cronCreateTool, getCronScheduler, resetCronScheduler, MAX_CRON_PROMPT_LEN, isSelfSchedulingPrompt, findDuplicateCronJob } from './cron-create';
export { cronDeleteTool } from './cron-delete';
export { cronListTool } from './cron-list';
export { scheduleWakeupTool } from './schedule-wakeup';
export { enterWorktreeTool } from './enter-worktree';
export { exitWorktreeTool } from './exit-worktree';
export { sendMessageTool } from './send-message';
export { getMailbox, clearMailbox, resetAllMailboxes } from './send-message';
export { sleepTool } from './sleep';
export { workflowTool, MAX_WORKFLOW_SCRIPT_BYTES, sanitizeWorkflowRunId, persistWorkflowRun, loadWorkflowRun } from './workflow';
export type { WorkflowRunRecord } from './workflow';
export { briefTool } from './brief';
export { WorkflowRuntime, executeWorkflow, parseWorkflowMeta } from './workflow-runtime';
export type { WorkflowMeta, AgentHandler, AgentOptions, WorkflowBudget, WorkflowContext, WorkflowResolver } from './workflow-runtime';
export { parseBashAST, extractCommands, extractCommandsFromString, PARSE_ABORTED } from './bash-parser';
export type { CommandNode, ParseBudget } from './bash-parser';
export { shouldUseSandbox, wrapWithSandbox, getSandboxStatus, resetSandboxCache } from './bash-sandbox';
export type { SandboxConfig } from './bash-sandbox';
export { teamCreateTool, teamDeleteTool, getCurrentTeam, resetTeam } from './team';
export { todoWriteTool, resetTodos, getTodos } from './todo-write';
export { createMcpAuthTool } from './mcp-auth';
export { createMcpTool } from './mcp-tool';
export { createMcpConnectTool } from './mcp-connect';
export { listMcpResourcesTool, readMcpResourceTool } from './mcp-resources';
export { configTool } from './config-tool';
export { syntheticOutputTool } from './synthetic-output';
export { lspDiagnosticsTool, lspReferencesTool } from './lsp-diagnostics';
export { lspTool } from './lsp';
export { agenticFetchTool } from './agentic-fetch';
export { scoutTool } from './scout';
export { endConversationTool, resetEndedConversations, isConversationEnded, countPendingWork } from './end-conversation';
export { isReadOnlyBashCommand, BashResultCache } from './bash-cache';
export { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from './command-normalize';
export { sanitizeToolInput, validateRequiredFields, detectPromptInjection } from './input-sanitizer';
export { levenshteinDistance, suggestSimilarPaths, suggestPathCorrection, suggestByBasename, suggestPathUnderCwd } from './path-suggest';
export type { PathSuggestionKind, PathCorrectionOptions, PathCorrectionResult, BasenameSearchOptions } from './path-suggest';
export { detectToolConflict, isReservedToolName, ToolRegistryGuard, RESERVED_TOOL_PREFIXES } from './tool-registry-guard';
export type { ToolConflict } from './tool-registry-guard';
