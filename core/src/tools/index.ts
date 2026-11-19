import { ToolDefinition, ToolInputSchema, ToolContext } from '../types';
import { ToolRegistryGuard, type ToolConflict, type ToolRegistryGuardOptions, filterAvailableTools } from './tool-registry-guard';
import { parseDeniedTools, isToolDenied, parseToolRegistryGuardOptions } from './tool-gates';
import { bashTool } from './bash';
import { fileReadTool } from './file-read';
import { fileEditTool } from './file-edit';
import { grepTool } from './grep';
import { globTool } from './glob';
import { listDirTool } from './list-dir';
import { readMemoryTool } from './read-memory';
import { writeMemoryTool } from './write-memory';
import { forgetMemoryTool } from './forget-memory';
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
import { cronGetTool } from './cron-get';
import { cronRunTool } from './cron-run';
import { scheduleWakeupTool } from './schedule-wakeup';
import { enterWorktreeTool } from './enter-worktree';
import { exitWorktreeTool } from './exit-worktree';
import { sendMessageTool } from './send-message';
import { sleepTool } from './sleep';
import { workflowTool } from './workflow';
import { briefTool } from './brief';
import { userMessageTool } from './user-message';
import { surfaceAttachmentTool } from './surface-attachments';
import { teamCreateTool, teamDeleteTool } from './team';
import { todoWriteTool, todoReadTool } from './todo-write';
import { configTool } from './config-tool';
import { syntheticOutputTool } from './synthetic-output';
import { lspDiagnosticsTool, lspReferencesTool } from './lsp-diagnostics';
import { lspTool } from './lsp';
import { agenticFetchTool } from './agentic-fetch';
import { advisorTool } from './advisor';
import { scoutTool } from './scout';
import { endConversationTool } from './end-conversation';
import { replTool } from './repl-tool';
import { testingPermissionTool } from './testing-permission';

/** Sources whose tools form the cache-stable BUILT-IN prefix partition. Everything
 *  else (MCP, installed plugins, user tools) sorts into the suffix partition. */
const BUILTIN_TOOL_SOURCES = new Set(['core', 'builtin']);

export class ToolRegistry {
  private tools: Map<string, ToolDefinition> = new Map();
  /** Registration source per tool name, used to partition the cache-stable pool. */
  private sources: Map<string, string> = new Map();
  private _guard: ToolRegistryGuard;

  // PARITY (additive): accept operator-configurable registration-gate options (extra
  // reserved prefixes / trusted sources) so a non-core registration honors them. Empty
  // by default — an argument-less `new ToolRegistry()` behaves exactly as before.
  constructor(guardOptions: ToolRegistryGuardOptions = {}) {
    this._guard = new ToolRegistryGuard(guardOptions);
  }

  register(tool: ToolDefinition, source = 'core'): void {
    // Pass the tool's declared aliases so the guard both collision-checks them and
    // can resolve a legacy/renamed name back to the canonical tool (parity: aliases).
    // PARITY (additive): built-ins-win on a CROSS-source name collision. Mirrors
    // Claude Code's `uniqBy([...builtIns, ...mcp], 'name')` precedence in
    // tools.ts:assembleToolPool — a plugin/MCP tool must never silently overwrite
    // an incumbent built-in of the same name. The guard returns a ToolConflict for a
    // cross-source duplicate (and for an invalid name/alias); in that case we keep the
    // incumbent and skip the newcomer. A SAME-source re-registration is not a conflict,
    // so it still updates the tool (legit hot-swap of a tool by its own owner).
    const conflict = this._guard.register(tool.name, source, tool.aliases);
    if (conflict) return;
    this.tools.set(tool.name, tool);
    this.sources.set(tool.name, source);
  }

  get(name: string): ToolDefinition | undefined {
    // Resolve by primary name, then via an alias (back-compat for renamed tools).
    const canonical = this._guard.resolveToolName(name) ?? name;
    return this.tools.get(canonical);
  }

  getAll(): ToolDefinition[] {
    return Array.from(this.tools.values());
  }

  /**
   * PARITY (additive): cache-stable assembly of the full tool pool, mirroring
   * Claude Code's `tools.ts:assembleToolPool`. Built-in tools (registered under the
   * `core`/`builtin` sources) are sorted by name into a contiguous PREFIX; MCP /
   * installed-plugin / user tools are sorted by name into a SUFFIX partition.
   *
   * The provider's system-prompt cache places a breakpoint after the last built-in
   * tool; keeping built-ins as a sorted contiguous prefix means adding or reordering
   * an MCP tool can never interleave into the built-in region and invalidate the
   * downstream cache keys. The result is deterministic regardless of registration
   * order, so the advertised tool list is prompt-cache stable across sessions/users.
   */
  assembleToolPool(): ToolDefinition[] {
    const builtIns: ToolDefinition[] = [];
    const extended: ToolDefinition[] = [];
    for (const tool of this.tools.values()) {
      const source = this.sources.get(tool.name) ?? 'core';
      (BUILTIN_TOOL_SOURCES.has(source) ? builtIns : extended).push(tool);
    }
    const byName = (a: ToolDefinition, b: ToolDefinition) => a.name.localeCompare(b.name);
    builtIns.sort(byName);
    extended.sort(byName);
    return [...builtIns, ...extended];
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
    // Advertise the model-facing tool list in cache-stable order (built-ins sorted
    // prefix, MCP/installed sorted suffix) so the provider's prompt cache holds.
    return this.assembleToolPool().map((tool) => ({
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
  // PARITY (additive): thread the operator-configured registration-gate knobs
  // (AMI_RESERVED_TOOL_PREFIXES / AMI_TRUSTED_TOOL_SOURCES) into the registry's guard so
  // a reserved extra prefix blocks a shadowing non-core tool and a trusted source may
  // claim a reserved prefix. Empty env → {} → unchanged default behavior.
  const registry = new ToolRegistry(parseToolRegistryGuardOptions());

  // PARITY (additive): the complete default tool pool, listed unconditionally.
  // Conditionally-available subsystems (teammates, workflow scripts, LSP, cron)
  // each declare their own `ToolDefinition.isEnabled()` predicate rather than
  // being hand-gated here — mirroring Claude Code's declarative per-tool
  // `Tool.isEnabled()` (ScheduleCron/TeamDelete/TaskCreate). `filterAvailableTools`
  // honors those predicates uniformly, so there is ONE gating mechanism and the
  // whole pool is self-describing. All subsystems default ON; their env flags
  // (AMI_AGENT_TEAMS / AMI_WORKFLOW_SCRIPTS / AMI_LSP / AMI_CRON) are the single
  // source-of-truth off-switches.
  const allTools: ToolDefinition[] = [
    bashTool,
    fileReadTool,
    fileEditTool,
    grepTool,
    globTool,
    listDirTool,
    webFetchTool,
    webSearchTool,
    notebookEditTool,
    searchSymbolsTool,
    toolSearchTool,
    askUserQuestionTool,
    gitCommitTool,
    taskTrackerTool,
    planModeTool,
    taskCompleteTool,
    taskOutputTool,
    taskKillTool,
    taskListTool,
    skillTool,
    cronCreateTool,
    cronDeleteTool,
    cronListTool,
    cronGetTool,
    cronRunTool,
    scheduleWakeupTool,
    enterWorktreeTool,
    exitWorktreeTool,
    sleepTool,
    briefTool,
    // PARITY (additive): dedicated user-facing output CHANNEL for output-gating
    // mode. Gated by its own isEnabled() => isOutputGateEnabled() (opt-in via
    // AMI_OUTPUT_GATE), so when the mode is off it is dropped at registration and
    // the default pool is unchanged. When on, the engine advertises it only on
    // turns where output-gating mode is active (two-way sync) and hides the model's
    // plain prose so all user-facing output flows through this tool.
    userMessageTool,
    surfaceAttachmentTool,
    todoWriteTool,
    todoReadTool,
    configTool,
    syntheticOutputTool,
    agenticFetchTool,
    advisorTool,
    scoutTool,
    endConversationTool,
    readMemoryTool,
    writeMemoryTool,
    forgetMemoryTool,
    taskTool,
    sendMessageTool,
    teamCreateTool,
    teamDeleteTool,
    workflowTool,
    lspDiagnosticsTool,
    lspReferencesTool,
    lspTool,
    // PARITY (additive): REPL code-interpreter tool. Gated by its own
    // isEnabled() => isReplModeEnabled() (opt-in via AMI_REPL), so when REPL mode
    // is off it is dropped at registration and the pool is unchanged. When on, it
    // is advertised and the primitive tools it wraps are hidden from direct
    // tool-calling by the advertise-time filter (see common/src/tool-pool.ts).
    replTool,
    // PARITY (additive): env-gated synthetic permission-flow test fixture. Gated by
    // its own isEnabled() => isTestingPermissionEnabled() (opt-in via
    // AMI_TESTING_PERMISSION), so it is dropped at registration in production and the
    // default pool is unchanged. When enabled it is advertised and the centralized
    // permission guard forces an unconditional ask (see permissions.ts).
    testingPermissionTool,
  ];

  // PARITY (additive): apply the operator-configurable AMI_DENY_TOOLS blocklist so a
  // denied tool/server is stripped from the pool before registration (and therefore
  // before the model or tool-search can see it), mirroring Claude's deny-rule filter.
  const denied = parseDeniedTools();
  const denyPredicate = denied.size > 0 ? (name: string) => isToolDenied(name, denied) : undefined;
  for (const tool of filterAvailableTools<ToolDefinition, ToolContext>(
    allTools,
    undefined,
    undefined,
    denyPredicate,
  )) {
    registry.register(tool);
  }

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
export { forgetMemoryTool } from './forget-memory';
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
export { cronGetTool } from './cron-get';
export { cronRunTool } from './cron-run';
export { scheduleWakeupTool } from './schedule-wakeup';
export { enterWorktreeTool } from './enter-worktree';
export { exitWorktreeTool } from './exit-worktree';
export { sendMessageTool } from './send-message';
export { getMailbox, clearMailbox, resetAllMailboxes } from './send-message';
export { sleepTool } from './sleep';
export { workflowTool, MAX_WORKFLOW_SCRIPT_BYTES, sanitizeWorkflowRunId, persistWorkflowRun, loadWorkflowRun } from './workflow';
export type { WorkflowRunRecord } from './workflow';
export { briefTool } from './brief';
export { userMessageTool, isOutputGateEnabled, OUTPUT_GATE_ENV, OUTPUT_GATE_ENV_COMPAT, USER_MESSAGE_MAX_CHARS } from './user-message';
export { surfaceAttachmentTool, formatAttachmentDelivery, expandAttachmentPath, resolveSurfacedAttachments, SURFACE_ATTACHMENT_METADATA_KEY } from './surface-attachments';
export type { SurfacedAttachment } from './surface-attachments';
export { WorkflowRuntime, executeWorkflow, parseWorkflowMeta } from './workflow-runtime';
export type { WorkflowMeta, AgentHandler, AgentOptions, WorkflowBudget, WorkflowContext, WorkflowResolver } from './workflow-runtime';
export { parseBashAST, extractCommands, extractCommandsFromString, PARSE_ABORTED, getOutputRedirections, withoutOutputRedirections, getPipeSegments, hasActualOperatorNodes, resolveShellWord } from './bash-parser';
export type { CommandNode, ParseBudget, OutputRedirection } from './bash-parser';
export { parseSedEditCommand, applySedSubstitution } from './bash-sed-parser';
export type { SedEditInfo } from './bash-sed-parser';
export {
  parsePowerShellCommand,
  getParseTimeoutMs,
  buildParseScript,
  toUtf16LeBase64,
  PARSE_SCRIPT_BODY,
  WINDOWS_MAX_COMMAND_LENGTH,
  MAX_COMMAND_LENGTH,
  mapStatementType,
  mapElementType,
  classifyCommandName,
  stripModulePrefix,
  transformCommandAst,
  transformExpressionElement,
  transformRedirection,
  transformStatement,
  transformRawOutput,
  COMMON_ALIASES,
  resolveToCanonical,
  getAllCommandNames,
  getAllCommands,
  getAllRedirections,
  getVariablesByScope,
  hasCommandNamed,
  hasDirectoryChange,
  isSingleCommand,
  commandHasArg,
  PS_TOKENIZER_DASH_CHARS,
  isPowerShellParameter,
  commandHasArgAbbreviation,
  getPipelineSegments,
  isNullRedirectionTarget,
  getFileRedirections,
  deriveSecurityFlags,
  __setPwshRuntimeForTest,
  __resetPwshRuntimeForTest,
} from './powershell-parser';
export type {
  ParsedPowerShellCommand,
  ParsedStatement,
  ParsedCommandElement,
  ParsedRedirection,
  ParsedVariable,
  ParseError,
  CommandElementType,
  CommandElementChild,
  PipelineElementType,
  StatementType,
  RedirectionOperator,
  SecurityFlags,
  RawParsedOutput,
  RawStatement,
  RawPipelineElement,
  RawCommandElement,
  RawRedirection,
  PwshRuntime,
  PwshSpawnResult,
} from './powershell-parser';
export { shouldUseSandbox, wrapWithSandbox, getSandboxStatus, checkSandboxDependencies, resetSandboxCache, commandWillBeHostSandboxed } from './bash-sandbox';
export type { HostSandboxEligibilityOptions } from './bash-sandbox';
export type { SandboxDependencyCheck } from './bash-sandbox';
export type { SandboxConfig } from './bash-sandbox';
export { teamCreateTool, teamDeleteTool, getCurrentTeam, resetTeam, getSelfIdentity, getSelfPlanModeRequired, getSelfParentSessionId } from './team';
export type { SelfIdentity } from './team';
export { todoWriteTool, todoReadTool, resetTodos, getTodos, setTodos, formatTodoReadOutput, validateInProgressActiveForm, extractTodosFromTranscript, rehydrateTodosFromTranscript } from './todo-write';
export { createMcpAuthTool } from './mcp-auth';
export { createMcpTool, getMcpToolDisplayName } from './mcp-tool';
export { createMcpConnectTool } from './mcp-connect';
export { listMcpResourcesTool, readMcpResourceTool } from './mcp-resources';
export { configTool } from './config-tool';
export { syntheticOutputTool } from './synthetic-output';
export { lspDiagnosticsTool, lspReferencesTool } from './lsp-diagnostics';
export { lspTool } from './lsp';
export { agenticFetchTool } from './agentic-fetch';
export {
  advisorTool,
  isAdvisorDisabled,
  resolveAdvisorModel,
  buildTranscriptDigest,
  ADVISOR_MODEL_ENV,
  ADVISOR_DISABLE_ENV,
  ADVISOR_DISABLE_ENV_ALIAS,
  ADVISOR_TIMEOUT_MS,
  ADVISOR_REVIEWER_PREAMBLE,
  ADVISOR_TOOL_INSTRUCTIONS,
} from './advisor';
export { scoutTool } from './scout';
export { endConversationTool, resetEndedConversations, isConversationEnded, countPendingWork } from './end-conversation';
export { testingPermissionTool, isTestingPermissionEnabled, TESTING_PERMISSION_TOOL_NAME, TESTING_PERMISSION_ENV } from './testing-permission';
export {
  replTool,
  runReplCode,
  isReplModeEnabled,
  getReplPrimitiveTools,
  getReplOnlyToolNames,
  filterReplOnlyTools,
  REPL_TOOL_NAME,
  REPL_MODE_ENV,
  REPL_MODE_ENV_COMPAT,
  DEFAULT_REPL_TIMEOUT_MS,
  MAX_REPL_TIMEOUT_MS,
} from './repl-tool';
export { isReadOnlyBashCommand, BashResultCache } from './bash-cache';
export { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from './command-normalize';
export { sanitizeToolInput, validateRequiredFields, detectPromptInjection } from './input-sanitizer';
export { levenshteinDistance, suggestSimilarPaths, suggestPathCorrection, suggestByBasename, suggestPathUnderCwd } from './path-suggest';
export type { PathSuggestionKind, PathCorrectionOptions, PathCorrectionResult, BasenameSearchOptions } from './path-suggest';
export { detectToolConflict, isReservedToolName, ToolRegistryGuard, RESERVED_TOOL_PREFIXES } from './tool-registry-guard';
export type { ToolConflict, ToolRegistryGuardOptions } from './tool-registry-guard';
export {
  capabilityEnabled,
  featureFlagEnvName,
  isFeatureEnabled,
  subsystemEnabled,
  WORKFLOW_SCRIPTS_FEATURE,
  CRON_FEATURE,
  AGENT_TEAMS_FEATURE,
  LSP_FEATURE,
  isTeammatesEnabled,
  isLspEnabled,
  isWorkflowScriptsEnabled,
  isCronEnabled,
  isTaskTrackerEnabled,
  TASK_TRACKER_ENV,
  TASK_TRACKER_FEATURE,
  CLAUDE_ENABLE_TASKS_ENV,
  AGENT_TEAMS_ENV,
  AGENT_TEAMS_ON_FLAG,
  AGENT_TEAMS_OFF_FLAG,
  LSP_ENV,
  WORKFLOW_SCRIPTS_ENV,
  CRON_ENV,
  DENY_TOOLS_ENV,
  parseDeniedTools,
  isToolDenied,
  RESERVED_PREFIXES_ENV,
  TRUSTED_SOURCES_ENV,
  parseToolRegistryGuardOptions,
} from './tool-gates';
export {
  getToolCategory,
  isMcpTool,
  isToolReadOnlyForInput,
  isToolConcurrencySafe,
  TOOL_CATEGORY_LABELS,
  mcpServerName,
  getMcpServerBuckets,
} from './tool-categories';
export type { ToolCategory, CategorizableTool, McpServerBucket } from './tool-categories';
