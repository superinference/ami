export { SessionManager } from './session';
export type { Session, SessionListEntry } from './session';
export { streamChatCompletion, streamChatCompletionWithRetry, inferProviderFromApiKey } from './provider';
export { createDefaultTools } from './tools';
export { MemoryManager, matchesPaths, sanitizeMemoryContent } from './memory';
export type { MemoryEntry, MemoryType, InstructionSource } from './memory';
export { SkillManager } from './skills';
export { discoverPlugins } from './plugins';
export type { PluginBundle } from './plugins';
export type { SkillDefinition, AgentDefinition } from './skills';
export { CostTracker, CacheMissDiagnostics, fingerprint } from './cost-tracker';
export type { ExtendedUsageStats, CacheMissCause, CacheDiagRequest } from './cost-tracker';
export { PermissionManager, containsUnquotedExpansion, containsShellSubstitution, isBuildConfigFile, unwrapShellControlStructures } from './permissions';
export { safeReadFile, safeLstat } from './tools/tool-utils';
export { boundMcpResponse, validateMcpBufferSize, DEFAULT_MCP_MAX_RESPONSE_BYTES } from './mcp/client';
export type { PermissionMode, PermissionRule, BashClassification, PermissionPromptHandler, PermissionPromptResult, DenyCheckResult } from './permissions';
export { classifyError, friendlyErrorMessage } from './error-classifier';
export type { ClassifiedError, ErrorCategory } from './error-classifier';
export { HookManager, sanitizeHookOutput, EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT } from './hooks';
export type { HookFn, HookContext, PreToolUseContext, PostToolUseContext, PreSamplingContext, HookDecision, PreToolUseHookFn, PostToolUseHookFn, PreSamplingHookFn } from './hooks';
export { verdictRank, holdsOver } from './permissions';
export { SessionMemoryExtractor } from './session-memory';
export type { SessionFact } from './session-memory';
export { AnalyticsTracker, generateToolSummary } from './analytics';
export type { AnalyticsEvent } from './analytics';
export type { FileState } from './file-cache';
export { Profiler } from './profiler';
export { WorkspaceIndexer } from './workspace-indexer';
export { ProviderCoordinator } from './provider-coordinator';
export type { CoordinatorState } from './provider-coordinator';
export { ProviderHealthTracker } from './provider-health';
export type { ProviderHealthStats } from './provider-health';
export { BUILTIN_PERSONAS, matchMcpAutoAllow } from './personas';
export type { PersonaDefinition } from './personas';
export { log as coreLog, logToolCall, logApiCall, logApiResponse, logError as coreLogError } from './logger';
export type { FileEntry, SymbolEntry, ImportEdge } from './workspace-indexer';

export { SuperInferenceEngine, Critic, Retriever } from './superinference';
export type { SuperInferenceConfig, BeliefState, CriticDecision, StopReason } from './superinference';

export type {
  Message,
  UserMessage,
  AssistantMessage,
  ToolMessage,
  SystemMessage,
  ToolCallContent,
  ToolCallFunction,
  TextContent,
  ImageContent,
  ToolInputSchema,
  ToolResult,
  ToolContext,
  ToolDefinition,
  ProviderConfig,
  StreamChunk,
  EngineEvent,
  EngineConfig,
  EngineSubmitOptions,
  ContextFile,
  UsageStats,
} from './types';
export type { ThinkingConfig, ThinkingLevel, ProviderSubsystem, PermissionSubsystem, SessionSubsystem } from './types';
export { buildSubsystems } from './types';
export { getModelCapabilities, registerModelCapabilities, clearDynamicCapabilities, discoverFromModelList, clampEffortLevel, requiresResponsesAPI } from './model-capabilities';
export { detectProvider, listModels, validateModel } from './model-registry';
export { loadProjectConfig, loadGlobalConfig, mergeConfigs } from './config';
export type { ProjectConfig } from './config';
export type { FuzzyResult, StrategyName, MatchPosition } from './tools/fuzzy-match';
export { categorizePrompt } from './prompt-categorizer';
export type { Intent, Scope, PromptCategory } from './prompt-categorizer';
export type { RateLimitBucket, RateLimitStatus } from './rate-limiter';
export type { CredentialEntry, SelectionStrategy } from './credential-pool';
export type { ExecCommandOptions, ExecCommandResult } from './utils/shell';
export { estimateTokens, estimateToolSchemaTokens } from './utils/tokens';
export { generateTitle } from './title-generator';
export { ProcessManager } from './process-manager';
export type { BackgroundProcess } from './process-manager';

export { estimateMessageTokens, estimateConversationTokens, estimateToolCallTokens } from './token-estimator';
export type { ToolStats } from './tool-analytics';
export { ToolAnalytics } from './tool-analytics';
export { isReadOnlyBashCommand, BashResultCache } from './tools/bash-cache';
export { CODE_LOADING_VARIABLES, POLICY_SOURCE_VARIABLES, detectCodeLoadingEnvSet, detectDangerousRm, detectNuclearCommand, detectBashArgv0Assignment, detectBashpidAssignment, detectZshRegexConditional, detectDnsExfiltration, detectPersistenceCommands, detectDangerousDd, detectContainerEscape, detectBase64ShellPipe, detectPrivilegeEscalation, detectPackageManagerHijack, detectAliasRedefinition, detectSourceExecution, extractInputRedirectionPaths, detectUnsafeFileCommand, detectShellStartupWrite } from './tools/bash-security';
export type { NuclearCheckResult } from './tools/bash-security';
export { withExtraHeaders } from './provider';
export { levenshteinDistance, suggestSimilarPaths } from './tools/path-suggest';
export type { TurnData, TurnSummary } from './turn-metrics';
export { TurnMetrics } from './turn-metrics';
export { isGrepNoMatchExit, stripGrepFlags } from './tools/grep';
export { expandBraces, estimateBraceExpansions } from './brace-expansion';
export { SubagentLimiter } from './subagent-limiter';

// Features awaiting consumer wiring — exported for dead-code compliance (see AGENTS.md Dead Code Policy)
export { AdmissionLog } from './admission-log';
export type { AdmissionEntry, AdmissionAction, AdmissionOutcome, AdmissionSubject } from './admission-log';
export { isPinnedVariable, getPinCategory, getAllPinnedVariables, EnvProtectionGuard } from './env-protection';
export type { PinCategory, EnvSetAttempt, EnvCheckResult } from './env-protection';
export { isProtectedEvent, canHookEvent, PROTECTED_EVENTS, sortHooksByTier, isStricterVerdict, HookChainGuard } from './hook-protection';
export type { HookTier, HookRegistration } from './hook-protection';
export { isModAllowed, parseModId, normalizeModName, modNamesMatch, validateModId, ManagedModsEnforcer } from './managed-mods';
export type { ModInfo, ManagedModsPolicy } from './managed-mods';
export { resolveProvenance, isProvenanceTrusted, ProvenanceTracker } from './provenance';
export type { ProvenanceTier, ProvenanceRecord } from './provenance';
export { parseRateLimitHeaders, parseRetryAfterHeader, shouldBackoff, suggestedWaitMs } from './rate-limit-parser';
export type { RateLimitInfo } from './rate-limit-parser';
export { canOverride, getEffectiveTier, filterManagedKeys, SettingsGuard } from './settings-guard';
export type { SettingsTier, SettingsSource } from './settings-guard';
export { MANAGED_ONLY_KEYS, TIER_LOCKED_KEYS } from './settings-guard';
export { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from './tools/command-normalize';
export { detectToolConflict, isReservedToolName, ToolRegistryGuard } from './tools/tool-registry-guard';
export type { ToolConflict } from './tools/tool-registry-guard';
export { RESERVED_TOOL_PREFIXES } from './tools/tool-registry-guard';
export { countToolUses, extractFilesModified, extractConversationMeta, getConversationDuration } from './conversation-summary';
export type { ConversationMeta } from './conversation-summary';
export { sanitizeToolInput, validateRequiredFields, detectPromptInjection, recoverJsonStringFields } from './tools/input-sanitizer';
export { isProtectedLogTier, canModifyLog, UILogGuard } from './ui-log-guard';
export type { LogTier, LogEntry } from './ui-log-guard';
export { validateEngineConfig, sanitizeEngineConfig } from './types';
export { TelemetryBatchSender } from './analytics';
export type { TelemetryBatch } from './analytics';
export { SessionDiffTracker } from './session';
export type { FileChange } from './session';
export { MAX_WORKTREES, validateWorktreeGitFile, findStaleWorktrees } from './worktree-manager';
export { isSensitiveEnvVar, canAccessEnv, EnvAccessGuard } from './env-access-guard';
export type { EnvAccessTier, EnvOperation, EnvAccessEvent } from './env-access-guard';
export { SENSITIVE_ENV_VARS } from './env-access-guard';
export { containsCredential, redactCredentials, findCredentials, CredentialGuard, REDACT_PLACEHOLDER } from './credential-guard';
export { isBlockedInSafeMode, isPartiallyBlocked, shouldEnableSafeMode, SafeModeGuard } from './safe-mode';
export type { SafeModeConfig, BlockedResource } from './safe-mode';
export { generateNonce, signRequest, createRequestSignature, verifyRequestSignature, NonceTracker } from './request-signature';
export type { RequestSignature } from './request-signature';
export { scanCodeForSecurityPatterns, scanAddedLinesForPatterns, getPatternById, SECURITY_PATTERNS } from './code-security-patterns';
export type { SecurityPattern, SecurityFinding } from './code-security-patterns';
