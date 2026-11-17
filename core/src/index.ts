export { SessionManager, MAX_SESSION_READ_BYTES, isSessionPersistenceDisabled, isResumeAllProjectsEnabled, detectTurnInterruption, buildConversationChain, selectNewestLeaf, getSessionMessages, appendResumeSentinel, NO_RESPONSE_REQUESTED, getSessionDisplayTitle, getSessionLockInfo, getAllLockInfo, cleanupStaleLocks, SESSION_KINDS, envSessionKind, isBgSession, checkSessionLock, isSessionLockHeldLocally, loadMessagesFromTranscriptPath, readTranscriptForLoad, TRANSCRIPT_READ_CHUNK_SIZE, collectContentReplacements, forkContentReplacements, parsePrIdentifier, filterSessionsByPr, checkCrossProjectResume, deriveFirstPrompt, parseSessionIdentifier, matchWorktreeForProjectDir, resolveAgentModelOverride, toListEntryAgent, AGENT_MODEL_INHERIT, sha256Hex, recordFileContribution, agentContributionRatio, writeResumePointer, readResumePointer, clearResumePointer, RESUME_POINTER_FILE, RESUME_POINTER_TTL_MS, isSessionCleanupEnabled, isHousekeepingDue, maybeRunDailyHousekeeping, HOUSEKEEPING_PERIOD_MS, HOUSEKEEPING_MARKER_FILE, HOUSEKEEPING_LOCK_SUBDIR, LITE_READ_BUF_SIZE, INITIAL_ENRICH_COUNT, SKIP_PRECOMPACT_THRESHOLD, unescapeJsonString, extractJsonStringField, extractLastJsonStringField } from './session';
export type { SessionKind, PrFilter, CrossProjectResumeResult, ParsedSessionIdentifier } from './session';
export type { Session, SessionAgentIdentity, SessionListEntry, SessionMetaLite, TurnInterruption, GetSessionMessagesOptions, TranscriptLoadResult, LockInfo, SessionLockCheck, FileAttributionState, AttributionSnapshot, ResumePointer, ContentReplacementRecord, ContentReplacementEntry } from './session';
export { streamChatCompletion, streamChatCompletionWithRetry, inferProviderFromApiKey, resolveContextWindow, buildRepairToolCall, describeProviders } from './provider';
export type { ProviderDescriptor } from './provider';
export { createDefaultTools } from './tools';
export { skillTool, filterToolsForSkill, resolveSkillTarget, SKILL_MAX_RESULT_CHARS } from './tools/skill';
export type { SkillResolution } from './tools/skill';
export { MemoryManager, matchesPaths, sanitizeMemoryContent, MEMORY_TYPES, MEMORY_SOURCE_LEVELS, truncateEntrypointContent, MAX_ENTRYPOINT_LINES, MAX_ENTRYPOINT_BYTES, MAX_MEMORY_FILES } from './memory';
export type { MemoryEntry, MemoryType, MemorySourceLevel, InstructionSource, MemoryPathScope, MemoryLoadSummary } from './memory';
export { AUTO_DREAM_DEFAULTS, resolveAutoDreamConfig, consolidationLockPath, readLastConsolidatedAt, tryAcquireConsolidationLock, rollbackConsolidationLock, recordConsolidation, isConsolidationDue, buildConsolidationPrompt, CONSOLIDATION_TASK_TYPE, CONSOLIDATION_DEBUG_COMPONENT, isDreamTask, beginConsolidationRun, abortConsolidationRun, createConsolidationProgress, recordConsolidationTurn, recordConsolidationFileTouched, completeConsolidationProgress, failConsolidationProgress, consolidationElapsedMs, formatConsolidationProgress, consolidationProgressSnapshot } from './memory-consolidation';
export type { AutoDreamConfig, ConsolidationGate, ConsolidationDebug, ConsolidationRunParams, ConsolidationStatus, ConsolidationTurn, ConsolidationProgress } from './memory-consolidation';
export { SNAPSHOT_META_FILENAME, SYNCED_META_FILENAME, validateSnapshotMeta, validateSyncedMeta, readJsonFile, getSnapshotDirForAgent, checkAgentMemorySnapshot, writeAgentSnapshotMeta, saveAgentSyncedMeta } from './agent-memory-snapshot';
export type { SnapshotMeta, SyncedMeta, SnapshotAction, SnapshotCheck } from './agent-memory-snapshot';
export { detectSessionFileType, detectSessionPatternType, classifyToolSessionFileType, isMemoryFileAccess } from './memory-file-detection';
export type { SessionFileType, MemoryDirs } from './memory-file-detection';
export { sequential } from './utils/sequential';
export { discoverPlugins, discoverPluginsDetailed, loadCommands, validateGitSource, parseGitRef, buildGitCloneArgs, assertPathWithinBase, resolvePluginDir, validatePluginManifest, GIT_NO_PROMPT_ENV, VALID_INSTALL_SCOPES, isENOENT, sanitizePluginIdSegment, generatePluginId, COMMAND_SOURCES, COMMAND_TYPES, isPluginAvailable, parsePluginSettings, isCommandEnabled, isCommandHidden, getCommandDisplayName, redactSensitiveCommandArgs, registerCommandAliases, REDACTED_COMMAND_ARGS, normalizePluginSubdirPath, hashPluginSubdirPath, coercePluginOptionBool, coercePluginOptionValues, buildPluginOptionPrefill, validatePluginOptionValues, getUnconfiguredPluginOptions, PLUGIN_OPTIONS_FILE, readPluginOptionValues, substituteUserConfigValues, buildPluginOptionEnv, pluginErrorKey, mergePluginErrors, PLUGIN_EDITABLE_SCOPES, isPersistablePluginScope, resolvePluginEditableScopes, formatPluginPathTraversalWarning, getPluginErrorMessage, PLUGIN_COMPONENTS, resolvePluginManifestHooks, buildPluginRecommendation } from './plugins';
export type { PluginBundle, CommandDefinition, CommandMetadata, PluginCommandsMetadataContext, GitSourceValidation, PluginValidationResult, PluginValidationIssue, PluginLoadFailure, CommandSource, CommandType, PluginIdPrefix, PluginRequirements, PluginSettings, PluginAvailabilityContext, CommandScanDiagnostics, PluginUserConfigOption, PluginOptionSchema, PluginOptionValue, PluginOptionValues, PluginError, PluginComponent, ExtendedPluginScope, PersistablePluginScope, PluginScopeSource, PluginChannel, PluginRecommendation, PluginRecommendationHint, ManifestHooksOptions } from './plugins';
// Skill/agent TYPES + arg-safety live in core (bare building blocks). The
// SkillManager (management) lives in common/src/skill-manager.ts.
export type { SkillDefinition, AgentDefinition, SkillManagerLike, EffortLevel, AgentValidationResult, AgentColorName, AgentMemoryScope, AgentIsolationMode, SkillUpdate, SkillImprovementSuggestion, PreloadedSkillsResult, FrontmatterShell, McpServerReadiness, SkillKind, AgentMemoryScopeOption, AgentMcpServerSpec, RawAgentMcpServers, ClassifiedAgentMcpServers } from './skills';
export { sanitizeSkillArgs, validateSkillArgs, MAX_SKILL_ARG_NAMES, MAX_SKILL_ARG_NAME_LENGTH, MAX_SKILL_ARG_VALUE_LENGTH, MAX_SKILL_ARG_TOTAL_CHARS, AGENT_SOURCES, getAgentSourceDisplayName, coerceDescriptionToString, EFFORT_LEVELS, parseEffort, isValidEffort, parseArgumentNames, AGENT_PERMISSION_MODES, validateAgentDefinition, AGENT_COLORS, parseAgentColor, AGENT_MEMORY_SCOPES, parseMemoryScope, AGENT_ISOLATION_MODES, parseIsolationMode, parseBackgroundFlag, parseAgentSkills, parseAllowedAgentTypes, canSpawnAgentType, hasRequiredMcpServers, filterAgentsByMcpRequirements, SAFE_SKILL_PROPERTIES, skillHasOnlySafeProperties, parseSkillAliases, isSkillEnabled, requiredEnvSatisfied, agentFileNameMismatch, missingSkillArgumentMessage, resolveSkillPrompt, getSkillFile, skillProgressMessage, DEFAULT_SKILL_PROGRESS_MESSAGE, SKILL_LOADING_PROGRESS_MESSAGE, defaultProgressMessage, progressMessageForKind, estimateSkillFrontmatterTokens, resolveAgentSystemPrompt, parseSkillUpdates, isValidSkillImprovement, formatSkillUpdates, parseBooleanFlag, REDACTED_SKILL_ARGS, redactSkillArgs, isSkillHidden, skillUserFacingName, isSkillAvailableNonInteractive, buildPreloadedSkillsBlock, PRELOADED_SKILLS_HEADING, resolveSkillName, isSkillImmediate, FRONTMATTER_SHELLS, DEFAULT_SHELL, parseShellFrontmatter, shellForExecution, formatToolNameList, progressiveArgumentHint, applySkillTemplateVariables, isBuiltInAgent, agentMemoryScopeDisplay, formatAgentMemoryDimension, resolveAgentModelDisplay, formatAgentSkillsList, AGENT_NONEDITABLE_SOURCES, isAgentEditable, agentIdentityKey, recommendedAgentMemoryScope, agentMemoryScopeOptions, getFilesForLanguage, buildInlineReference, requiredMcpServerReadiness, substituteNamedArgPlaceholders, contentReferencesArgName, skillPluginRoot, agentPluginRoot, skillKind, SKILL_KINDS, scopedToolAllowRules, mcpToolNamespacePrefix, isMcpToolOfServer, deriveMcpToolAllowlist, parseMcpServerWildcard, isBashToolAllowRule, allowlistGrantsBash, classifyAgentMcpServerSpecs } from './skills';
// Skill catalog types + path-safety for URL-based skill discovery.
export type { SkillCatalogIndex, SkillCatalogEntry, SkillTrustTier, SkillSource, InstalledSkillManifest, CatalogValidationResult, SkillSourcePolicy } from './skill-catalog';
export { SKILL_TRUST_TIERS, isSafeSegment, isSafeRelativePath, validateCatalogIndex, resolveSkillUrl, isHexSha256, isValidCatalogName, isValidGitRef, isSkillSourceAllowed, validateInstalledSkillManifest, INSTALLED_MANIFEST_SCHEMA_VERSION, compareSemver, isAmiVersionCompatible, formatSkillSourceSuffix } from './skill-catalog';
export { CostTracker, CacheMissDiagnostics, fingerprint, CACHE_MISS_CAUSES, estimateUsageCost, diffToolHashes, WEB_SEARCH_COST_PER_REQUEST, MIN_CACHE_MISS_TOKENS, PARTIAL_CACHE_BREAK_RATIO, formatPrice, formatModelPricing, getModelPricingString } from './cost-tracker';
export { diffStringLists, buildDiffableContent, createUnifiedPatch, writeCacheBreakDiff } from './cost-tracker';
export type { ExtendedUsageStats, CacheMissCause, CacheDiagRequest, ModelCostUsage, ToolSchemaDelta, CacheBreakCause, DiffableTool, ModelSpeed } from './cost-tracker';
export {
  resolveConfigDir, migrateLegacyDataDir, migrateDeprecatedModelAlias, runStartupMigrations, formatStartupMigrationNotices, CURRENT_MIGRATION_VERSION, projectStateDir, projectSlug, MAX_SLUG_LENGTH, MAX_SESSION_ID_LENGTH, journalPathFor, subagentJournalPathFor,
  canonicalizePath, canonicalGitRoot,
  toReferenceToolName, toReferenceModelId, buildUsageFields,
  buildAssistantEntry, buildUserEntry, appendEntry, ndjsonSafeStringify,
  dashboardConfigPath, registerDataDir, maybeRegisterDataDir,
  AMI_TO_CLAUDE_TOOL, AMI_TO_REFERENCE_MODEL, COMPAT_VERSION,
} from './reference-journal';
export type {
  JournalUsageInput, JournalUsage, JournalContentBlock, StructuredPatchHunk,
  AssistantEntry, AssistantEntryArgs, UserEntry, UserEntryArgs, ModelAliasMigration,
  StartupMigrationResult, StartupMigrationNoticeOptions,
} from './reference-journal';
export { PermissionManager, containsUnquotedExpansion, containsShellSubstitution, isBuildConfigFile, unwrapShellControlStructures } from './permissions';
export { safeReadFile, safeLstat, FILE_ENCODINGS } from './tools/tool-utils';
export { boundMcpResponse, validateMcpBufferSize, DEFAULT_MCP_MAX_RESPONSE_BYTES, MCP_TRANSPORTS, MCP_CLIENT_STATES, MCP_TERMINAL_ERROR_CODES, MCP_ELICITATION_ACTIONS, MCP_ELICITATION_MODES, MCP_LOG_LEVELS, McpRpcError, McpAuthError, MCP_URL_ELICITATION_REQUIRED_CODE, MAX_MCP_URL_ELICITATION_RETRIES, extractUrlElicitations } from './mcp/client';
export type { McpTransport, McpUrlElicitation, McpIdeSelection, McpIdeAtMention, McpSelectionPoint, McpSelectionChangedParams } from './mcp/client';
export { createLinkedTransportPair } from './mcp/in-process-transport';
export type { InProcessTransport } from './mcp/in-process-transport';
export { MCP_SERVER_STATES, matchMcpAutoAllow, isValidCallbackPort, resolveMcpOAuthRedirect } from './mcp/manager';
export type { McpServerState } from './mcp/manager';
export { createMcpResourceNoticeBuffer, formatResourceUpdateNotice } from './mcp/resource-update-notice';
export type { McpResourceUpdateNotice, McpResourceNoticeBuffer, McpResourceEventSource } from './mcp/resource-update-notice';
export { createMcpConnectionNoticeBuffer, formatMcpConnectionChange } from './mcp/connection-notice';
export type { McpConnectionChange, McpConnectionState, McpConnectionNoticeBuffer, McpConnectionEventSource } from './mcp/connection-notice';
export { getMcpServerLogPath, logMcpError, logMcpDebug, sanitizeMcpLogSegment, mcpLogDateStamp } from './mcp/log-sink';
export { classifyMcpToolForCollapse, normalizeCollapseToolName } from './mcp/classify-collapse';
export type { McpCollapseClass } from './mcp/classify-collapse';
export type { McpLogSinkOptions, McpErrorLogRecord, McpDebugLogRecord } from './mcp/log-sink';
export type { PermissionMode, PermissionRule, BashClassification, PermissionPromptHandler, PermissionPromptResult, DenyCheckResult } from './permissions';
export { classifyError, friendlyErrorMessage, isGatewayRejection, ERROR_CATEGORIES, sanitizeMessageHTML, extractNestedErrorMessage, normalizeErrorMessage, parsePromptTooLongTokenCounts, getPromptTooLongTokenGap, parseMaxTokensContextOverflow, AbortError, makeAbortError, isAbort, API_ERROR_MESSAGE_PREFIX, startsWithApiErrorPrefix, isPromptTooLongMessage, classifySafetyCheckFailure } from './error-classifier';
export type { ClassifiedError, ErrorCategory, SafetyClassifierFailure } from './error-classifier';
export { FAULT_INJECTION_ENV, FAULT_DEFAULT_STATUS, parseFaultInjection, resetFaultInjection, faultInjectionActive, injectFault, consumeFault, makeInjectedError, maybeThrowFault } from './fault-injection';
export type { FaultKind, InjectedFault } from './fault-injection';
export { HookManager, sanitizeHookOutput, EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT, HOOK_EVENTS, HOOK_CONFIG_TYPES, resolveHookTimeoutMs, combineHookAbort, HOOK_AGENT_DISALLOWED_TOOLS, isHttpHookUrlAllowed, interpolateHookHeaderEnv, sanitizeHeaderValue, formatHttpHookBlockedMessage, ERR_HTTP_HOOK_BLOCKED_ADDRESS, unwrapHookSpecificOutput, matchesHookIf, ELICITATION_ACTIONS, getHookEventMetadata, eventSupportsMatcher, formatHookMatcherLabel, sortMatchersByPriority, HOOK_SOURCE_PRIORITY, SESSION_START_SOURCES, isSessionStartSource, SESSION_END_REASONS, isSessionEndReason, SETUP_TRIGGERS, isSetupTrigger, hookContentLabel, getHookDisplayText, hookSourceDisplayString, hookSourceDescription, hookSourceHeader, hookSourceInline, formatStopHookSummaryMessage, shouldEmitHookLifecycle, ALWAYS_EMITTED_HOOK_EVENTS, HOOK_TIMING_DISPLAY_THRESHOLD_MS, normalizeMapKeyedHooks, parseFrontmatterHooks, substitutePluginRoot, substitutePluginData, resolvePluginDataDir, withFrontmatterHookPluginRoot, MAX_PENDING_HOOK_EVENTS, validatePromptRequest, parsePromptRequestLine, isValidPromptSelection, buildPromptResponse, serializePromptResponse, createPromptLineScanner } from './hooks';
export type { HookFn, HookContext, PreToolUseContext, PostToolUseContext, PreSamplingContext, HookDecision, PreToolUseHookFn, PostToolUseHookFn, PreSamplingHookFn, HookEvent, HookConfigType, HttpHookPolicy, HookEventMetadata, HookEventMetadataOpts, HookDescriptor, HookEventMatcherGroup, HookEventDescription, HookAuditRow, HookProgressEvent, StopHookSummary, StopHookInfo, SessionStartSource, SessionEndReason, SetupTrigger, HookConfigEntry, HookConfigFile, FunctionHook, FunctionHookCallback, RuntimeHookMeta, HookLifecycleEvent, PromptRequest, PromptRequestOption, PromptResponse, PromptRequestHandler, PromptLineScanner } from './hooks';
export { shouldSkipHookDueToTrust } from './hook-protection';
export { wildcardMatch } from './permissions';
export { describeRule } from './permissions';
export {
  PROMPT_RULE_PREFIX, createPromptRuleContent, isPromptRuleContent, extractPromptDescription,
  managedRuleReadOnlyNotice,
} from './permissions';
export {
  AUTO_MODE_DEFAULT_RULES, getDefaultExternalAutoModeRules, resolveAutoModeRules,
  buildAutoModeClassifierPrompt, buildAutoModeClassifierPromptFromConfig,
  AUTO_MODE_UNAVAILABLE_REASONS, classifyAutoModeUnavailableReason, getAutoModeUnavailableNotification,
  resolveAutoModeClassifierOutcome, makeAutoModeClassifierFailure,
} from './auto-mode';
export type { AutoModeRules, AutoModeUnavailableReason, AutoModeAvailabilityState, AutoModeClassifierResolution, AutoModeClassifierOutcome } from './auto-mode';
export {
  RISK_LEVELS, riskLevelRank, classifyPermissionRisk,
} from './permissions';
export type { RiskLevel, PermissionExplanation } from './permissions';
export { detectPathExpansionHazard, pathExpansionHazardReason } from './permissions';
export type { PathExpansionHazard } from './permissions';
export {
  validatePermissionRule, filterInvalidPermissionRules, permissionRuleValueFromString,
  permissionRuleValueToString, escapeRuleContent, unescapeRuleContent, normalizeToolName,
  resolveFileOperationType, resolveConfigFolderScope,
} from './permissions';
export type { PermissionRuleValue, FileOperationType, ConfigFolderScope } from './permissions';
export { verdictRank, holdsOver } from './permissions';
// Delegated permission-prompt tool result parsing (parity: --permission-prompt-tool).
// Wired through common/tool-executor; re-exported here so the building block is part of
// the public core surface and reachable in the core import graph.
export { parsePermissionPromptToolResult, buildPermissionPromptToolInput } from './permission-prompt-tool';
export type {
  PermissionPromptToolDecision,
  ParsePermissionPromptToolResult,
  PermissionPromptToolInput,
} from './permission-prompt-tool';
export {
  SessionMemoryExtractor,
  selectIncrementalWindow,
  substituteMemoryVariables,
  loadSessionMemoryPromptTemplate,
  DEFAULT_MEMORY_PROMPT_TEMPLATE,
} from './session-memory';
export type { SessionFact, IncrementalWindow } from './session-memory';
export { AnalyticsTracker, generateToolSummary } from './analytics';
export type { AnalyticsEvent, AnalyticsTrackerOpts } from './analytics';
export {
  getPrivacyLevel, isTelemetryDisabled, isEssentialTrafficOnly, getEssentialTrafficOnlyReason, isEnvTruthy,
  getPrivacyPosture, isNonessentialFeatureAllowedOnMiss,
  sanitizeToolNameForAnalytics, sanitizeAnalyticsData, sanitizeAnalyticsValue,
  shouldSampleEvent, resolveSampleRate, trySerialize, markAnalyticsSafe, MAX_ANALYTICS_STRING_LEN,
  truncateContent, MAX_OTLP_ATTR_BYTES, shouldIncludeTelemetryAttr, buildTelemetryResourceAttrs,
  OTEL_CARDINALITY_DEFAULTS, parseOtelHeaders,
  getFileExtensionForAnalytics, getFileExtensionsFromBashCommand, sanitizeAgentName, MAX_FILE_EXTENSION_LENGTH,
  detectTerminalType, getHostPlatformForAnalytics, HOST_PLATFORM_OVERRIDES, getEnvironmentFingerprint, detectWslForAnalytics,
  DEFAULT_SHUTDOWN_TIMEOUT_MS, resolveShutdownTimeoutMs, stripLocalOnlyFields, resolveLocalOnlyFields,
  getCiContext, getOrCreateUserId, USER_ID_FILE, getUserBucket, NUM_USER_BUCKETS,
  bucketHttpStatus, deriveHttpStatusFields,
  parseExporterTypes, resolveOtlpProtocol, OTLP_PROTOCOLS, isTelemetrySuppressedForTest,
  resolveExportIntervalMs, DEFAULT_EXPORT_INTERVALS, resolveFlushTimeoutMs,
  DEFAULT_FLUSH_TIMEOUT_MS, formatFlushTimeoutGuidance,
  isToolContentLoggingEnabled, truncateToolInputValue, extractToolInputForTelemetry,
  TOOL_INPUT_STRING_TRUNCATE_AT, TOOL_INPUT_STRING_TRUNCATE_TO, TOOL_INPUT_MAX_JSON_CHARS,
  TOOL_INPUT_MAX_COLLECTION_ITEMS, TOOL_INPUT_MAX_DEPTH,
  resolveBetaTracingConfig, filterEventForEgress, TelemetryContentDedup,
  DEDUP_MIN_CONTENT_LEN, DEDUP_PREVIEW_LEN,
  OtelHeadersHelper, validateOtelHeaders, resolveOtelHeadersDebounceMs, DEFAULT_OTEL_HEADERS_DEBOUNCE_MS,
  raceWithTimeout, CLOSE_GRACE_MS,
  TOOL_SUMMARY_LINE_MAX, hashFilePath, hashFileContent, FILE_PATH_HASH_LEN, MAX_CONTENT_HASH_SIZE,
  SINK_NAMES, resolveKilledSinks, ProcessMetricsSampler, FailedBatchStore, FAILED_BATCH_FILE_PREFIX,
  ANALYTICS_SOURCE_CRON, buildFileOperationEvent, classifyGitOperation, IncrementalContextTracker,
  createTelemetryDiagLogger, resolveFileFlushIntervalMs,
} from './analytics';
export type { TelemetryDiagLevel, TelemetryDiagLogger, TelemetryDiagLoggerDeps } from './analytics';
export type { OtlpProtocol, BetaTracingConfig, EgressAllowlist, OtelHeadersHelperDeps,
  SinkName, ProcessMetrics, ProcessMetricsDeps, LocalOnlyFieldMatcher,
  FileOperationKind, FileOperationTool, FileOperationParams,
  GitOperationName, GitOperationClassification, IncrementalContextDelta } from './analytics';
export { PRIVACY_LEVELS, ESSENTIAL_TRAFFIC_VARS, ESSENTIAL_TRAFFIC_DENY_ON_MISS } from './analytics';
export { HeapDumpGuard, HEAP_DUMP_RSS_THRESHOLD_BYTES } from './heap-dump';
export type { HeapDumpDeps } from './heap-dump';
export type { PrivacyLevel, PrivacyPosture, SanitizeOpts, AnalyticsSafeMetadata } from './analytics';
export type { FileState } from './file-cache';
export { seedFileCacheFromTranscript, collectReadFileStatesFromTranscript, stripLineNumberPrefix } from './file-cache-seed';
export type { SeededFileState } from './file-cache-seed';
export { Profiler, FpsTracker, slowWarning, formatSecondsShort } from './profiler';
export type { ProfilerOpts, ProfilerCheckpoint, TimelineEntry, FormatTimelineOpts, SlowOperation, FpsMetrics, TimingStatistics, TurnTiming, SlowWarning, SlowSeverity, SlowWarningOpts, TimingLifecycleEvent, TraceEvent, PhaseDefinitions, TtftBreakdown, RequestPhase, SpanType, SpanHandle, SpanNode, AgentTrack } from './profiler';
export { WorkspaceIndexer, findProjectRoot } from './workspace-indexer';
export { isGeneratedFile, filterGeneratedFiles } from './generated-files';
export { ProviderCoordinator } from './provider-coordinator';
export type { CoordinatorState, ModelRecovery } from './provider-coordinator';
export { ProviderHealthTracker } from './provider-health';
export type { ProviderHealthStats } from './provider-health';
export { log as coreLog, logToolCall, logApiCall, logApiResponse, logError as coreLogError, getInMemoryErrors, readLogTail, summarizeValue, extractServerMessage, httpErrorContext, captureApiRequest, getLastApiRequest, recordDiagnostic, shouldSurfaceToTelemetry, withDiagnosticsTiming, debugBody, debugTruncate, logForDiagnosticsNoPII, flushDiagnosticsWriters, getErrorsPath, dateToFilename, isDebugMode, enableDebugLogging, flushPendingLogWrites } from './logger';
export type { LogTailResult } from './logger';
export { createBufferedWriter } from './buffered-writer';
export type { BufferedWriter, BufferedWriterOptions, WriteFn } from './buffered-writer';
export type { FileEntry, SymbolEntry, ImportEdge, BuildIndexResult } from './workspace-indexer';

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
  ToolGroupKind,
  ToolGroupCounts,
  ChannelMessageMeta,
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
export type { ThinkingConfig, ThinkingLevel, ProviderSubsystem, PermissionSubsystem, SessionSubsystem, OutputFormat, InputFormat, ChatMode, Theme, OutputStyle, StreamChunkType, AutoModeConfig, StreamingToolProgress, PromptSuggestionResolution } from './types';
export { buildSubsystems, OUTPUT_FORMATS, INPUT_FORMATS, CHAT_MODES, THEMES, OUTPUT_STYLES, INSIGHT_CALLOUT_HEADER, ENGINE_EVENT_TYPES, EXIT_REASONS, STREAM_CHUNK_TYPES, NOTIFICATION_CHANNELS, SERVICE_TIERS, TASK_TERMINAL_STATUS, SESSION_OPERATING_MODES, mergeSkillUsageStat, reconcileSessionMode, MAX_LSP_RECOMMENDATION_IGNORED_COUNT, isLspRecommendationDisabled, shouldEnablePromptSuggestion,
  MIN_SLOW_PROGRESS_REVEAL_SECONDS, shouldRevealSlowProgress, sanitizeToolInputSummary, resolveSentAt,
  selectModelFacingOutput, selectUserFacingOutput, shouldRenderGroupedToolUse, isSuggestionMessage,
  shouldForcePermissionCheck, validateSummarizeMetadata } from './types';
export type { SkillUsageStat, SessionOperatingMode, SessionModeReconciliation, SystemMessageKind, SummarizeMetadata } from './types';
export { getModelCapabilities, registerModelCapabilities, clearDynamicCapabilities, discoverFromModelList, clampEffortLevel, modelSupportsMaxEffort, modelSupportsAdvisor, modelAlwaysOnThinking, resolveMaxEffortLevel, requiresResponsesAPI, parseClaudeVersion, claudeReasoningCapabilities, is1mContextDisabled, supportsOneMillionContextBeta, normalizeModelStringForAPI, getSmallFastModel, SMALL_FAST_MODEL_ENV_VARS, THINKING_LEVELS, REVIEW_EFFORT_LEVELS, getModelDeprecationWarning, toApiProviderClass, hasUltrathinkKeyword, findThinkingTriggerPositions, get3PModelFallbackSuggestion, getAvailableContextUpgrade } from './model-capabilities';
export type { ApiProviderClass, ContextUpgrade } from './model-capabilities';
export { formatTokenCount } from './token-format';
export { PERMISSION_MODES, BASH_CLASSIFICATIONS, PERMISSION_UPDATE_DESTINATIONS, PERMISSION_RULE_SOURCES, coercePermissionRuleSource, isManagedPermissionSource, managedPermissionsPath, SESSION_GRANT_SCOPES, SAFETY_CHECK_KINDS } from './permissions';
export type { PermissionRuleSource } from './permissions';
export { detectProvider, listModels, validateModel, MODEL_ALIASES, MODEL_FAMILY_ALIASES, MODEL_SHORT_KEYS, isModelAlias, isModelFamilyAlias, isModelShortKey, resolveModelAlias, getModelDisplayName, getModelDescription, getModelMetadata, getAgentModelOptions, getNewerVersionHint, suggestFamilyVersionFallback, isLegacyModelRemapDisabled, LEGACY_MODEL_REMAP_OPT_OUT_ENV, resolveOverriddenModel, MODEL_ENV_TOGGLES } from './model-registry';
export type { AgentModelOption } from './model-registry';
export type { ValidateModelResult, ModelAlias } from './model-registry';
export { loadProjectConfig, loadGlobalConfig, mergeConfigs, applyConfigEnv, applyConfigLanguage, applyConfigStartupKnobs, applyConfigCaCerts, applyConfigGitignore, applyConfigAutoCompact, applyConfigPlansDirectory, applyConfigAnthropicBetas, resolveAdditionalDirectories, FAST_MODE_BETA, REDACT_THINKING_BETA, CONFIG_ENV_TIERS, loadConfigFileChecked, loadFlagSettingsConfig, FLAG_SETTINGS_ENV, backupCorruptConfig, findMostRecentBackup, collectConfigDiagnostics, validateProjectConfigValues, findUnknownConfigKeys, KNOWN_CONFIG_KEYS, getConfigValidationDocLink, formatConfigValueIssues, buildConfigDiagnosticsTree, formatConfigDiagnosticsTree, CONFIG_DOC_BASE, getConfigJSONSchema, CONFIG_SCHEMA_URL, markInternalWrite, consumeInternalWrite, INTERNAL_WRITE_WINDOW_MS, loadPersistedSkillUsage, persistSkillUsageStat, saveGlobalConfig, loadPreferenceFlags, wasPreferenceShown, markPreferenceShown, loadNoticeSeenCounts, getNoticeSeenCount, shouldShowNotice, recordNoticeSeen } from './config';
export type { ProjectConfig, ConfigEnvLayers, ApplyConfigEnvResult, ConfigDiagnostic, ConfigValueIssue, ConfigSource, ConfigJSONSchema, ConfigSchemaProperty } from './config';
export { STRATEGY_NAMES, NOOP_EDIT_ERROR } from './tools/fuzzy-match';
export type { FuzzyResult, StrategyName, MatchPosition } from './tools/fuzzy-match';
export { categorizePrompt, PROMPT_INTENTS, PROMPT_SCOPES, selectTier, maxTier, PROMPT_TIERS, TIER_RANK, matchesKeepGoingKeyword, matchesNegativeKeyword, getQuerySourceForAgent, getQuerySourceForREPL } from './prompt-categorizer';
export type { Intent, Scope, PromptCategory, PromptTier, TierSignals } from './prompt-categorizer';
export type { RateLimitBucket, RateLimitStatus, RateLimitOverage, RawWindowUtilization, RawUtilization } from './rate-limiter';
export { SELECTION_STRATEGIES } from './credential-pool';
export type { CredentialEntry, SelectionStrategy } from './credential-pool';
export type { ExecCommandOptions, ExecCommandResult } from './utils/shell';
export { collectAncestorPids, readParentPidMap, ANCESTOR_WALK_TIMEOUT_MS } from './utils/shell';
export { getProcessCommand, collectChildPids, collectAncestorCommands, PROCESS_COMMAND_TIMEOUT_MS } from './utils/process-utils';
export { estimateTokens, estimateToolSchemaTokens } from './utils/tokens';
export {
  atomicWriteFileSync,
  secureMkdirSync,
  OWNER_ONLY_FILE_MODE,
  OWNER_ONLY_DIR_MODE,
} from './utils/fs-atomic';
export type { AtomicWriteOptions } from './utils/fs-atomic';
export { getGitBranch, isPathGitignored } from './utils/git';
export {
  expandPath,
  isPathSafe,
  toRelativePath,
  isPathWithin,
  abbreviateHome,
  displayPath,
  shortestDisplayPath,
  stripTrailingSep,
  validatePathExpansionSafety,
} from './utils/path';
export { generateTitle, sanitizeTitle, truncateTitle, deriveTitle, isUsableTitle, generateBranchName, sanitizeBranchSlug, runAuxQuery, buildAuxResult, createAuxiliaryQuery } from './title-generator';
export type { AuxQueryResult, AuxQuerySuccess, AuxQueryError, AuxQuerySource, AuxQueryErrorReason, OnAuxQueryResult, AuxQueryOutcome, AuxQueryFailure, AuxiliaryQueryConfig, AuxiliaryQueryRun } from './title-generator';
export { stripDisplayTags } from './utils/display-tags';
export { detectPythonPackageManager, pythonInstallCommand, pythonBootstrapHint } from './utils/package-manager';
export type { PythonPackageManager } from './utils/package-manager';
export { getInstallPlatform, getOsRelease, isDistroFamily, detectMise, detectAsdf, detectHomebrew, detectWinget, detectPacman, detectDeb, detectRpm, detectApk, getPackageManager, upgradeInstructionFor, resetPackageManagerCaches, packageManagerHooks } from './package-manager';
export type { PackageManager, InstallPlatform, OsRelease } from './package-manager';
export { ProcessManager, TASK_STATUS, TASK_KINDS, TASK_ATTENTION_STATES, isTerminalTaskStatus, isActiveTaskStatus, isBackgroundTask } from './process-manager';
export type { BackgroundProcess, TaskKind, TaskStatus, TaskAttentionState, ProgressDelta, TaskMessage, ForegroundSync } from './process-manager';

export { estimateMessageTokens, estimateConversationTokens, estimateToolCallTokens, estimateBlockTokens, estimateContentTokens, IMAGE_DOCUMENT_TOKEN_ESTIMATE, estimateTextTokens, estimateImageTokens, utf8ByteLength } from './token-estimator';
export { estimateTokensForFileType, charsPerTokenForFileType } from './utils/tokens';
export type { ToolStats, ContextHint, OverallToolStats, ToolAnalyticsSnapshot } from './tool-analytics';
export { ToolAnalytics } from './tool-analytics';
export { Reservoir, percentile as reservoirPercentile, normalizeReservoirSize, DEFAULT_RESERVOIR_SIZE } from './utils/reservoir';
export { treeify } from './utils/treeify';
export type { TreeNode, TreeValue, TreeifyOptions, TreeColorRole } from './utils/treeify';
export { isReadOnlyBashCommand, BashResultCache } from './tools/bash-cache';
export { FILE_IMAGES_KINDS } from './tools/file-read';
export { MCP_AUTH_STATUS, MCP_AUTH_ERROR_KINDS, redactAuthUrl, classifyMcpAuthError } from './tools/mcp-auth';
export type { McpAuthStatus, McpAuthErrorKind, McpAuthServerConfig } from './tools/mcp-auth';
export { scheduleWakeupTool, clampWakeupDelay, resetLoopPacingState, LOOP_CONTINUE_SENTINEL, NOOP_STREAK_WARN_THRESHOLD, WAKEUP_MIN_DELAY_SECONDS, WAKEUP_MAX_DELAY_SECONDS } from './tools/schedule-wakeup';
export { CODE_LOADING_VARIABLES, POLICY_SOURCE_VARIABLES, detectCodeLoadingEnvSet, detectDangerousRm, detectNuclearCommand, detectBashArgv0Assignment, detectBashpidAssignment, detectZshRegexConditional, detectDnsExfiltration, detectPersistenceCommands, detectDangerousDd, detectContainerEscape, detectBase64ShellPipe, detectPrivilegeEscalation, detectPackageManagerHijack, detectAliasRedefinition, detectSourceExecution, extractInputRedirectionPaths, detectUnsafeFileCommand, detectShellStartupWrite, detectGitInternalWrite, detectPowerShellExecution, detectArchiveToolExecution, detectTildeExpansionVariant } from './tools/bash-security';
export type { NuclearCheckResult } from './tools/bash-security';
export { CLM_ALLOWED_TYPES, normalizeTypeName, isClmAllowedType, detectUnsafeTypeCast, CODE_EXEC_CMDLETS, MODULE_LOADING_CMDLETS, WMI_CIM_CMDLETS, ALIAS_HIJACK_CMDLETS, ENV_WRITE_CMDLETS, NETWORK_CMDLETS, FILEPATH_EXECUTION_CMDLETS, DANGEROUS_SCRIPT_BLOCK_CMDLETS, ARG_GATED_CMDLETS, POWERSHELL_NEVER_SUGGEST, isPowerShellNeverSuggest, detectUnsafeNewObject, detectStartProcessElevation, detectRuntimeStateHijack, detectEnvVarManipulation, detectDangerousPsCmdlets, detectStopParsingToken, detectDangerousPowerShellCommand, COMMON_SWITCHES, COMMON_VALUE_PARAMS, COMMON_PARAMETERS, SAFE_SCRIPT_BLOCK_CMDLETS, SCHEDULED_TASK_CMDLETS, detectFilePathExecution, detectForEachMemberName, detectScheduledTask, detectInvokeItem, detectScriptBlockInjection, detectDynamicCommandName, detectInvokeExpression, powershellCommandIsSafe, POWERSHELL_READONLY_CMDLETS, isReadOnlyPowerShellCommand } from './tools/powershell-security';
export type { PowerShellSecurityResult } from './tools/powershell-security';
export {
  isDockerAvailable,
  getDockerAvailability,
  describeDockerUnavailable,
  annotateStderrWithSandboxFailures,
  wrapWithDocker,
  defaultDenyWritePaths,
  bareGitRepoDenyPaths,
  detectWorktreeMainRepoPath,
  PRIVILEGED_AUTOLOAD_RELPATHS,
  DOCKER_SECURITY_OPTS,
  DEFAULT_DOCKER_IMAGE,
  DOCKER_PROBE_TIMEOUT_MS,
  DOCKER_SANDBOX_TMPDIR,
  extractBlockedHost,
  _resetDockerAvailableCache,
} from './tools/docker-sandbox';
export {
  recordSandboxHostGrant,
  isSandboxHostGranted,
  getGrantedSandboxHosts,
  processSandboxPermissionResponse,
  clearSandboxPermissionGrants,
  requestSandboxHostAccess,
  createSandboxAskCallback,
  sandboxNetworkAskMessage,
  isSandboxNetworkAskEnabled,
  SANDBOX_NETWORK_ASK_ENV,
} from './tools/sandbox-permission-registry';
export type {
  SandboxAskCallback,
  NetworkHostPattern,
} from './tools/sandbox-permission-registry';
export type {
  DockerSandboxConfig,
  DockerAvailability,
  DockerUnavailableCause,
  DockerProbeOptions,
} from './tools/docker-sandbox';
export { withExtraHeaders } from './provider';
// Consumed by common (/model ZDR listing); the ZDR-error + request-side helpers are
// used internally by core (error-classifier, provider) so are not re-exported here.
export { fetchZdrModelIds, filterZdrModels } from './openrouter-zdr';
export { levenshteinDistance, suggestSimilarPaths, suggestPathCorrection, suggestByBasename, suggestPathUnderCwd } from './tools/path-suggest';
export type { PathSuggestionKind, PathCorrectionOptions, PathCorrectionResult, BasenameSearchOptions } from './tools/path-suggest';
export type {
  TurnData,
  TurnSummary,
  ModelTurnUsage,
  TurnBudgetStatus,
  TokenBreakdown,
  TurnMetricsSnapshot,
} from './turn-metrics';
export { TurnMetrics, formatTurnBudget } from './turn-metrics';
export { isGrepNoMatchExit, stripGrepFlags, grepLineFilePath, splitStreamLines, stripTrailingCR } from './tools/grep';
export {
  resolveRipgrepConfig,
  getRipgrepStatus,
  probeRipgrep,
  resetRipgrepStatus,
  ripgrepHooks,
  USE_BUILTIN_RIPGREP_ENV,
  RIPGREP_PATH_ENV,
} from './tools/ripgrep-config';
export type { RipgrepConfig, RipgrepMode } from './tools/ripgrep-config';
export { expandBraces, estimateBraceExpansions } from './brace-expansion';
export { SubagentLimiter } from './subagent-limiter';

// Features awaiting consumer wiring — exported for dead-code compliance (see AGENTS.md Dead Code Policy)
export { AdmissionLog, ADMISSION_ACTIONS, ADMISSION_OUTCOMES, ADMISSION_SUBJECTS } from './admission-log';
export type { AdmissionEntry, AdmissionAction, AdmissionOutcome, AdmissionSubject } from './admission-log';
export { isPinnedVariable, getPinCategory, getAllPinnedVariables, EnvProtectionGuard, PIN_CATEGORIES, PIN_CATEGORY_RISK, explainPinnedBlock } from './env-protection';
export type { PinCategory, EnvSetAttempt, EnvCheckResult } from './env-protection';
export { SAFE_ENV_VARS, isSafeEnvVar, isDangerousSettingsEnvVar, PROVIDER_MANAGED_ENV_VARS, PROVIDER_MANAGED_ENV_PREFIXES, isProviderManagedEnvVar, ENV_REJECT_REASONS, ENV_TRUST_TIERS, filterSettingsEnv, isHostManagedProvider, NETWORK_TRUST_VARS, isNetworkTrustEnvVar, classifyUntrustedEnvVar, CODE_LOADING_PREFIXES } from './env-protection';
export type { EnvRejectReason, EnvTrustTier, SettingsEnvFilterResult } from './env-protection';
export { isProtectedEvent, canHookEvent, PROTECTED_EVENTS, PASSTHROUGH_EVENTS, HOOK_TIERS, sortHooksByTier, isStricterVerdict, HookChainGuard, isManagedHookSource, canRunHookUnderManagedOnly, filterManagedOnlyHooks, HOOK_EVENT_INFO, getHookEventInfo, tiersAllowedForProtectedEvent, describeProtectedEventBlock, buildHookVerdictCheck, mergeHookVerdictChecks, hookEntrySignature, captureHookSnapshot, detectHiddenHookAdditions } from './hook-protection';
export type { HookSourceKind, HookEventInfo, BlockedHookRegistration, HookVerdictCheck, HookVerdictFlow, HookVerdict, HookSignatureInput } from './hook-protection';
export type { HookTier, HookRegistration } from './hook-protection';
export { isModAllowed, isModBlockedByPolicy, getManagedPluginNames, parseModId, normalizeModName, modNamesMatch, validateModId, ManagedModsEnforcer } from './managed-mods';
export type { ModInfo, ManagedModsPolicy } from './managed-mods';
export { resolveProvenance, isProvenanceTrusted, ProvenanceTracker, summarizeProvenance, PROVENANCE_TIERS, PROVENANCE_TIER_INFO, PROVENANCE_INTEGRITY_INFO, PROVENANCE_INTEGRITY_ORDER, RISK_CAPABILITIES, describeTier, describeIntegrity, provenanceWarning, elevatedCapabilities, hasElevatedCapabilities, assessProvenanceRisk } from './provenance';
export type { ProvenanceTier, ProvenanceRecord, ProvenanceSummary, ProvenanceIntegrity, ProvenanceSeverity, ProvenanceTierInfo, ProvenanceIntegrityInfo, RiskCapabilityName, RiskCapabilities, SourceCapabilities, ProvenanceRiskAssessment } from './provenance';
export { parseRateLimitHeaders, parseRetryAfterHeader, shouldBackoff, isHardRateLimited, suggestedWaitMs, formatResetTime, formatRateLimitReset } from './rate-limit-parser';
export type { RateLimitInfo } from './rate-limit-parser';
export { canOverride, getEffectiveTier, filterManagedKeys, SettingsGuard, SETTINGS_TIERS } from './settings-guard';
export type { SettingsTier, SettingsSource } from './settings-guard';
export { MANAGED_ONLY_KEYS, TIER_LOCKED_KEYS, isBypassPermissionsModeDisabled, isAllHooksDisabled, isManagedHooksOnly } from './settings-guard';
export { MANAGED_ONLY_NESTED_KEYS, TIER_LOCKED_NESTED_KEYS, isManagedOnlyKey, isTierLockedKey } from './settings-guard';
export { DANGEROUS_SHELL_SETTINGS, extractDangerousSettings, hasDangerousSettings, hasDangerousSettingsChanged, formatDangerousSettingsList } from './settings-guard';
export type { DangerousShellSetting, DangerousSettings, DangerousSettingsInput } from './settings-guard';
export { getSettingsTierShortName, getSettingsTierDisplayName, getSettingsTierDisplayNameLowercase, getSettingsTierLabels, getManagedSettingsSources, getSettingsLayerOrder, resolveAppliedSettings } from './settings-guard';
export type { ManagedSettingsSource, EffectiveAppliedSettings } from './settings-guard';
export { areSandboxSettingsLockedByPolicy, validateManagedPolicyValues, SANDBOX_POLICY_LOCK_MESSAGE, managedStatusSuffix, shouldAllowManagedSandboxDomainsOnly } from './settings-guard';
export type { SandboxPolicyLockInput, ManagedPolicyWarning } from './settings-guard';
export { getSettingsJSONSchema } from './settings-guard';
export type { SettingsJSONSchema, SettingsSchemaProperty } from './settings-guard';
export { CUSTOMIZATION_SURFACES, parseStrictPluginOnlyCustomization, isRestrictedToPluginOnly, isSourceAdminTrusted, ADMIN_TRUSTED_SOURCES } from './plugin-only-policy';
export type { CustomizationSurface, StrictPluginOnlyPolicy } from './plugin-only-policy';
export { reviewManagedSettings, managedApprovalPath, loadManagedApproval, saveManagedApproval, MANAGED_SETTINGS_DECISIONS } from './managed-settings-guard';
export type { ManagedSettingsDecision, ManagedSettingsReview } from './managed-settings-guard';
export { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from './tools/command-normalize';
export { detectToolConflict, isReservedToolName, ToolRegistryGuard, validateToolName, validateToolDefinition, canonicalToolName, filterAvailableTools, toolMatchesName, findToolByName, TOOL_NAME_PATTERN, MAX_TOOL_NAME_LENGTH } from './tools/tool-registry-guard';
export type { ToolConflict, RegistrableToolLike, ToolRegistryGuardOptions } from './tools/tool-registry-guard';
export { RESERVED_TOOL_PREFIXES } from './tools/tool-registry-guard';
export { GLOB_RIPGREP_ENV, isRipgrepGlobEnabled } from './tools/glob';
export { countToolUses, extractFilesModified, extractConversationMeta, getConversationDuration, isHumanTurn, estimateCompactionMessageTokens, dropOrphanedToolResults, selectRecentWithinBudget, buildTruncationMarker, getLanguageFromPath, categorizeToolError, responseTimeHistogram, formatDuration, formatConversationSummary } from './conversation-summary';
export type { ConversationMeta, BudgetSelection } from './conversation-summary';
export { computeLineDiff } from './line-diff';
export { sanitizeToolInput, validateRequiredFields, detectPromptInjection, recoverJsonStringFields, expandTabs, DEFAULT_TAB_INTERVAL } from './tools/input-sanitizer';
export {
  isProtectedLogTier,
  canModifyLog,
  UILogGuard,
  LOG_TIERS,
  sanitizeLogMessage,
  DEFAULT_MAX_LOG_LENGTH,
  SECRET_REDACTION,
} from './ui-log-guard';
export type { LogTier, LogEntry, SanitizeLogOptions } from './ui-log-guard';
export { validateEngineConfig, sanitizeEngineConfig } from './types';
export { TelemetryBatchSender, sendOtlpBatch, postOtlpBatch, postOtlpPayload, isRetryableStatus, toOtlpLogsPayload, otlpLogsUrl,
  otlpSignalUrl, resolveOtlpLogsEndpoint, resolveOtlpSignalEndpoint, getGitEmail,
  toOtlpMetricsPayload, toOtlpTracePayload, resolveMetricsTemporalityPreference, OTLP_TEMPORALITY } from './analytics';
export type { OtlpSendResult, TelemetryBatchSenderOpts, OtlpLogsEndpoint,
  TelemetryReconfigureOpts, MetricSnapshot, MetricsTemporality } from './analytics';
export type { TelemetryBatch } from './analytics';
export { SessionDiffTracker } from './session';
export type { FileChange } from './session';
export { MAX_WORKTREES, validateWorktreeGitFile, findStaleWorktrees } from './worktree-manager';
export { isSensitiveEnvVar, canAccessEnv, EnvAccessGuard, ENV_ACCESS_TIERS, ENV_OPERATIONS } from './env-access-guard';
export type { EnvAccessTier, EnvOperation, EnvAccessEvent } from './env-access-guard';
export { SENSITIVE_ENV_VARS } from './env-access-guard';
export { containsCredential, redactCredentials, findCredentials, getCredentialLabel, CredentialGuard, REDACT_PLACEHOLDER } from './credential-guard';
export type { CredentialMatch } from './credential-guard';
export { isBlockedInSafeMode, isPartiallyBlocked, shouldEnableSafeMode, SafeModeGuard, BLOCKED_RESOURCES, resourceTier, describeBlocked, RESOURCE_DESCRIPTIONS, SAFE_MODE_GUIDANCE } from './safe-mode';
export type { SafeModeConfig, BlockedResource, ResourceTier, BlockedResourceInfo } from './safe-mode';
export { generateNonce, signRequest, createRequestSignature, verifyRequestSignature, NonceTracker, SUPPORTED_SIGNATURE_ALGORITHMS } from './request-signature';
export type { RequestSignature, SignatureAlgorithm } from './request-signature';
export { scanCodeForSecurityPatterns, scanAddedLinesForPatterns, getPatternById, SECURITY_PATTERNS } from './code-security-patterns';
export type { SecurityPattern, SecurityFinding, SecuritySeverity } from './code-security-patterns';
export { AMI_VERSION, AMI_CLIENT_NAME, AMI_CLIENT_INFO } from './version';
export {
  escapeXmlAttr,
  escapeXmlText,
  safeXmlKey,
  stripInvalidXmlChars,
  unescapeXml,
} from './xml-escape';
export {
  CHANNEL_TAG,
  SAFE_CHANNEL_META_KEY,
  wrapChannelMessage,
  parseChannelMessage,
  displayChannelServerName,
  isChannelMessage,
} from './channel-message';
export {
  classifyToolGroupKind,
  aggregateToolGroupCounts,
  emptyToolGroupCounts,
  hasNonMemoryOps,
  isGitOpBashCommand,
} from './tool-group-counts';
export type { ToolGroupClassifyOptions } from './tool-group-counts';
export {
  TOOL_RESULT_STATES,
  classifyToolResultState,
  isUnsuccessfulToolResult,
  TOOL_CALL_DISPLAY_STATES,
  deriveToolCallDisplayState,
  systemMessageDisplay,
  REDACTED_THINKING_PLACEHOLDER,
  isRedactedThinkingBlock,
  thinkingDisplayText,
} from './message-display';
export type {
  ToolResultState,
  ToolCallDisplayState,
  SystemMessageLevel,
  SystemMessageDisplay,
  SystemMessageDisplayOptions,
} from './message-display';
export {
  SESSION_STATES,
  isSessionState,
  SessionStateMachine,
  STATUS_CATEGORIES,
  isStatusCategory,
  validatePostTurnStatus,
} from './session-state';
export type {
  SessionState,
  RequiresActionDetails,
  SessionExternalMetadata,
  StatusCategory,
  PostTurnStatusSummary,
} from './session-state';
export {
  LSPClient,
  getLSPClient,
  DEFAULT_LSP_SERVERS,
  getAllLspServers,
  registerLspServers,
  resetLspServers,
  resolveLanguageForExtension,
  languageIdForExtension,
  assertNoAutoFetch,
  validateLspServerConfig,
  loadLspServersFromFile,
  resolvePluginLspEnvironment,
  DiagnosticTracker,
  mapLspSeverity,
  convertPublishDiagnostics,
  areDiagnosticsEqual,
  dedupeDiagnostics,
  normalizeDiagnosticPath,
  diagnosticPathsEqual,
  uriToPath,
  diagnosticSeveritySymbol,
  formatNewDiagnosticsSummary,
  formatDiagnosticsDetail,
  capDiagnosticVolume,
  isUncPath,
  lspFileSizeBytes,
  DIAGNOSTIC_SEVERITY_NAMES,
  MAX_LSP_FILE_SIZE_BYTES,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
  LspHealthMonitor,
  collectLspHealthErrors,
  formatLspHealthError,
  LSP_POLL_INTERVAL_MS,
  LSP_HEALTH_HINT,
  createDiagnosticKey,
  LSPDiagnosticRegistry,
  getLSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
  MAX_DELIVERED_FILES,
} from './lsp';
export type {
  LspServerConfig,
  LspPluginEnvironment,
  NotificationHandler,
  RequestHandler,
  LspServerState,
  LspServerStatus,
  LspInitializationStatus,
  HandlerRegistrationResult,
  LspHealthError,
  LspHealthMonitorOptions,
  HealthScheduler,
  Diagnostic,
  DiagnosticFile,
  DiagnosticSource,
  DiagnosticSeverityName,
  DiagnosticRange,
  DiagnosticPosition,
  PublishDiagnosticsParams,
  PendingLSPDiagnostic,
  RegisterPendingInput,
  DeliveredLSPDiagnostics,
} from './lsp';

// --- batch #10 additive parity exports ---
export { mapChildToSubagentEvent, summarizeToolInput, createSubagentRelay, buildSubagentIdleEvent, createAgentInvocationLedger, resolveEnableSummarization } from './tools/subagent-relay';
export type { SubagentRelay, SubagentState, RelayMessage, RelaySendResult, RelayRejectReason, RelayCompletion, RelayIdle, InvocationKind, InvocationEdge, AgentInvocationLedger } from './tools/subagent-relay';
export { parseEnvInt, parseEnvFloat, parseEnvBool, resolveEnvVar, parseDotenv, loadDotenv, validateBoundedEnvInt, collectEnvLimitDiagnostics, AMI_ENV_INT_LIMITS } from './utils/env';
export type { DotenvParseOptions, LoadDotenvOptions, EnvIntBounds, EnvIntStatus, BoundedEnvIntResult, EnvLimitSpec, EnvLimitDiagnostic } from './utils/env';
export { normalizePermissionMode, PERMISSION_MODE_ALIASES, PERMISSION_MODE_CYCLE, cyclePermissionMode } from './permissions';

// --- batch #11 additive parity exports ---
export { cronListTool, isActiveJob, sortJobsForDisplay, CRON_LIST_STATUSES } from './tools/cron-list';
export type { CronListStatus } from './tools/cron-list';

// --- scalable fuzzy file-path index (nucleo/fzf-v2 port) ---
export { FileIndex, fileIndexBuildConfig, yieldToEventLoop as fileIndexYieldToEventLoop } from './file-index';
export type { SearchResult as FileSearchResult } from './file-index';
export {
  SCORE_MATCH,
  BONUS_BOUNDARY,
  BONUS_CAMEL,
  BONUS_CONSECUTIVE,
  BONUS_FIRST_CHAR,
  PENALTY_GAP_START,
  PENALTY_GAP_EXTENSION,
  FUZZY_MAX_QUERY_LEN,
  isBoundary,
  isLower,
  isUpper,
  boundaryBonus,
} from './fuzzy-score';
