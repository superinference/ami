export { SessionManager, MAX_SESSION_READ_BYTES, isSessionPersistenceDisabled, detectTurnInterruption } from './session';
export type { Session, SessionListEntry, TurnInterruption } from './session';
export { streamChatCompletion, streamChatCompletionWithRetry, inferProviderFromApiKey, resolveContextWindow, buildRepairToolCall, describeProviders } from './provider';
export type { ProviderDescriptor } from './provider';
export { createDefaultTools } from './tools';
export { skillTool, filterToolsForSkill, resolveSkillTarget, SKILL_MAX_RESULT_CHARS } from './tools/skill';
export type { SkillResolution } from './tools/skill';
export { MemoryManager, matchesPaths, sanitizeMemoryContent, MEMORY_TYPES, truncateEntrypointContent, MAX_ENTRYPOINT_LINES, MAX_ENTRYPOINT_BYTES } from './memory';
export type { MemoryEntry, MemoryType, InstructionSource } from './memory';
export { discoverPlugins, discoverPluginsDetailed, loadCommands, validateGitSource, parseGitRef, buildGitCloneArgs, assertPathWithinBase, resolvePluginDir, validatePluginManifest, GIT_NO_PROMPT_ENV, VALID_INSTALL_SCOPES } from './plugins';
export type { PluginBundle, CommandDefinition, GitSourceValidation, PluginValidationResult, PluginValidationIssue, PluginLoadFailure } from './plugins';
// Skill/agent TYPES + arg-safety live in core (bare building blocks). The
// SkillManager (management) lives in common/src/skill-manager.ts.
export type { SkillDefinition, AgentDefinition, SkillManagerLike, EffortLevel, AgentValidationResult, AgentColorName, AgentMemoryScope, AgentIsolationMode } from './skills';
export { sanitizeSkillArgs, validateSkillArgs, MAX_SKILL_ARG_NAMES, MAX_SKILL_ARG_NAME_LENGTH, MAX_SKILL_ARG_VALUE_LENGTH, MAX_SKILL_ARG_TOTAL_CHARS, AGENT_SOURCES, coerceDescriptionToString, EFFORT_LEVELS, parseEffort, isValidEffort, parseArgumentNames, AGENT_PERMISSION_MODES, validateAgentDefinition, AGENT_COLORS, parseAgentColor, AGENT_MEMORY_SCOPES, parseMemoryScope, AGENT_ISOLATION_MODES, parseIsolationMode, parseBackgroundFlag, parseAgentSkills, parseAllowedAgentTypes, canSpawnAgentType, hasRequiredMcpServers, filterAgentsByMcpRequirements, SAFE_SKILL_PROPERTIES, skillHasOnlySafeProperties, parseSkillAliases, isSkillEnabled, missingSkillArgumentMessage } from './skills';
// Skill catalog types + path-safety for URL-based skill discovery.
export type { SkillCatalogIndex, SkillCatalogEntry, SkillTrustTier, SkillSource, InstalledSkillManifest, CatalogValidationResult, SkillSourcePolicy } from './skill-catalog';
export { SKILL_TRUST_TIERS, isSafeSegment, isSafeRelativePath, validateCatalogIndex, resolveSkillUrl, isHexSha256, isValidCatalogName, isValidGitRef, isSkillSourceAllowed, validateInstalledSkillManifest, INSTALLED_MANIFEST_SCHEMA_VERSION, compareSemver, isAmiVersionCompatible } from './skill-catalog';
export { CostTracker, CacheMissDiagnostics, fingerprint, CACHE_MISS_CAUSES, estimateUsageCost, diffToolHashes, WEB_SEARCH_COST_PER_REQUEST, MIN_CACHE_MISS_TOKENS, PARTIAL_CACHE_BREAK_RATIO, formatPrice, formatModelPricing, getModelPricingString } from './cost-tracker';
export { diffStringLists } from './cost-tracker';
export type { ExtendedUsageStats, CacheMissCause, CacheDiagRequest, ModelCostUsage, ToolSchemaDelta, CacheBreakCause } from './cost-tracker';
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
  StartupMigrationResult,
} from './reference-journal';
export { PermissionManager, containsUnquotedExpansion, containsShellSubstitution, isBuildConfigFile, unwrapShellControlStructures } from './permissions';
export { safeReadFile, safeLstat, FILE_ENCODINGS } from './tools/tool-utils';
export { boundMcpResponse, validateMcpBufferSize, DEFAULT_MCP_MAX_RESPONSE_BYTES, MCP_TRANSPORTS, MCP_CLIENT_STATES, MCP_TERMINAL_ERROR_CODES, MCP_ELICITATION_ACTIONS, MCP_ELICITATION_MODES, MCP_LOG_LEVELS } from './mcp/client';
export type { McpTransport } from './mcp/client';
export { MCP_SERVER_STATES, matchMcpAutoAllow } from './mcp/manager';
export type { McpServerState } from './mcp/manager';
export type { PermissionMode, PermissionRule, BashClassification, PermissionPromptHandler, PermissionPromptResult, DenyCheckResult } from './permissions';
export { classifyError, friendlyErrorMessage, isGatewayRejection, ERROR_CATEGORIES, sanitizeMessageHTML, extractNestedErrorMessage, normalizeErrorMessage, parsePromptTooLongTokenCounts, getPromptTooLongTokenGap, parseMaxTokensContextOverflow } from './error-classifier';
export type { ClassifiedError, ErrorCategory } from './error-classifier';
export { HookManager, sanitizeHookOutput, EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT, HOOK_EVENTS, HOOK_CONFIG_TYPES, resolveHookTimeoutMs, combineHookAbort, HOOK_AGENT_DISALLOWED_TOOLS, isHttpHookUrlAllowed, interpolateHookHeaderEnv, sanitizeHeaderValue, formatHttpHookBlockedMessage, ERR_HTTP_HOOK_BLOCKED_ADDRESS, unwrapHookSpecificOutput, matchesHookIf, ELICITATION_ACTIONS, getHookEventMetadata, eventSupportsMatcher, formatHookMatcherLabel, sortMatchersByPriority, HOOK_SOURCE_PRIORITY, SESSION_START_SOURCES, isSessionStartSource, SESSION_END_REASONS, isSessionEndReason, SETUP_TRIGGERS, isSetupTrigger } from './hooks';
export type { HookFn, HookContext, PreToolUseContext, PostToolUseContext, PreSamplingContext, HookDecision, PreToolUseHookFn, PostToolUseHookFn, PreSamplingHookFn, HookEvent, HookConfigType, HttpHookPolicy, HookEventMetadata, HookEventMetadataOpts, HookDescriptor, HookEventMatcherGroup, HookEventDescription, SessionStartSource, SessionEndReason, SetupTrigger } from './hooks';
export { shouldSkipHookDueToTrust } from './hook-protection';
export { wildcardMatch } from './permissions';
export {
  validatePermissionRule, filterInvalidPermissionRules, permissionRuleValueFromString,
  permissionRuleValueToString, escapeRuleContent, unescapeRuleContent, normalizeToolName,
} from './permissions';
export type { PermissionRuleValue } from './permissions';
export { verdictRank, holdsOver } from './permissions';
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
  sanitizeToolNameForAnalytics, sanitizeAnalyticsData, sanitizeAnalyticsValue,
  shouldSampleEvent, resolveSampleRate, trySerialize, markAnalyticsSafe, MAX_ANALYTICS_STRING_LEN,
  truncateContent, MAX_OTLP_ATTR_BYTES, shouldIncludeTelemetryAttr, buildTelemetryResourceAttrs,
  OTEL_CARDINALITY_DEFAULTS, parseOtelHeaders,
  getFileExtensionForAnalytics, getFileExtensionsFromBashCommand, sanitizeAgentName, MAX_FILE_EXTENSION_LENGTH,
} from './analytics';
export { PRIVACY_LEVELS, ESSENTIAL_TRAFFIC_VARS } from './analytics';
export type { PrivacyLevel, SanitizeOpts, AnalyticsSafeMetadata } from './analytics';
export type { FileState } from './file-cache';
export { Profiler, FpsTracker, slowWarning } from './profiler';
export type { ProfilerOpts, ProfilerCheckpoint, TimelineEntry, SlowOperation, FpsMetrics, TimingStatistics, TurnTiming, SlowWarning, SlowSeverity, SlowWarningOpts } from './profiler';
export { WorkspaceIndexer, findProjectRoot } from './workspace-indexer';
export { isGeneratedFile, filterGeneratedFiles } from './generated-files';
export { ProviderCoordinator } from './provider-coordinator';
export type { CoordinatorState } from './provider-coordinator';
export { ProviderHealthTracker } from './provider-health';
export type { ProviderHealthStats } from './provider-health';
export { log as coreLog, logToolCall, logApiCall, logApiResponse, logError as coreLogError } from './logger';
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
export type { ThinkingConfig, ThinkingLevel, ProviderSubsystem, PermissionSubsystem, SessionSubsystem, OutputFormat, InputFormat, ChatMode, Theme, OutputStyle, StreamChunkType } from './types';
export { buildSubsystems, OUTPUT_FORMATS, INPUT_FORMATS, CHAT_MODES, THEMES, OUTPUT_STYLES, INSIGHT_CALLOUT_HEADER, ENGINE_EVENT_TYPES, EXIT_REASONS, STREAM_CHUNK_TYPES, NOTIFICATION_CHANNELS, SERVICE_TIERS, TASK_TERMINAL_STATUS } from './types';
export { getModelCapabilities, registerModelCapabilities, clearDynamicCapabilities, discoverFromModelList, clampEffortLevel, modelSupportsMaxEffort, resolveMaxEffortLevel, requiresResponsesAPI, parseClaudeVersion, claudeReasoningCapabilities, is1mContextDisabled, supportsOneMillionContextBeta, THINKING_LEVELS, REVIEW_EFFORT_LEVELS, getModelDeprecationWarning, toApiProviderClass } from './model-capabilities';
export type { ApiProviderClass } from './model-capabilities';
export { formatTokenCount } from './token-format';
export { PERMISSION_MODES, BASH_CLASSIFICATIONS, PERMISSION_UPDATE_DESTINATIONS, PERMISSION_RULE_SOURCES, coercePermissionRuleSource, isManagedPermissionSource, managedPermissionsPath, SESSION_GRANT_SCOPES, SAFETY_CHECK_KINDS } from './permissions';
export type { PermissionRuleSource } from './permissions';
export { detectProvider, listModels, validateModel, MODEL_ALIASES, MODEL_FAMILY_ALIASES, isModelAlias, isModelFamilyAlias, resolveModelAlias, getModelDisplayName, getModelDescription, getModelMetadata } from './model-registry';
export type { ValidateModelResult, ModelAlias } from './model-registry';
export { loadProjectConfig, loadGlobalConfig, mergeConfigs, applyConfigEnv, CONFIG_ENV_TIERS, loadConfigFileChecked, backupCorruptConfig, findMostRecentBackup, collectConfigDiagnostics, validateProjectConfigValues, findUnknownConfigKeys, KNOWN_CONFIG_KEYS, markInternalWrite, consumeInternalWrite, INTERNAL_WRITE_WINDOW_MS } from './config';
export type { ProjectConfig, ConfigEnvLayers, ApplyConfigEnvResult, ConfigDiagnostic, ConfigValueIssue } from './config';
export { STRATEGY_NAMES } from './tools/fuzzy-match';
export type { FuzzyResult, StrategyName, MatchPosition } from './tools/fuzzy-match';
export { categorizePrompt, PROMPT_INTENTS, PROMPT_SCOPES, selectTier, maxTier, PROMPT_TIERS, TIER_RANK, matchesKeepGoingKeyword, matchesNegativeKeyword, getQuerySourceForAgent, getQuerySourceForREPL } from './prompt-categorizer';
export type { Intent, Scope, PromptCategory, PromptTier, TierSignals } from './prompt-categorizer';
export type { RateLimitBucket, RateLimitStatus } from './rate-limiter';
export { SELECTION_STRATEGIES } from './credential-pool';
export type { CredentialEntry, SelectionStrategy } from './credential-pool';
export type { ExecCommandOptions, ExecCommandResult } from './utils/shell';
export { estimateTokens, estimateToolSchemaTokens } from './utils/tokens';
export {
  atomicWriteFileSync,
  secureMkdirSync,
  OWNER_ONLY_FILE_MODE,
  OWNER_ONLY_DIR_MODE,
} from './utils/fs-atomic';
export type { AtomicWriteOptions } from './utils/fs-atomic';
export { getGitBranch } from './utils/git';
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
export { generateTitle, sanitizeTitle, truncateTitle } from './title-generator';
export { ProcessManager, TASK_STATUS } from './process-manager';
export type { BackgroundProcess } from './process-manager';

export { estimateMessageTokens, estimateConversationTokens, estimateToolCallTokens, estimateBlockTokens, estimateContentTokens, IMAGE_DOCUMENT_TOKEN_ESTIMATE, estimateTextTokens, estimateImageTokens, utf8ByteLength } from './token-estimator';
export { estimateTokensForFileType, charsPerTokenForFileType } from './utils/tokens';
export type { ToolStats, ContextHint, OverallToolStats, ToolAnalyticsSnapshot } from './tool-analytics';
export { ToolAnalytics } from './tool-analytics';
export { Reservoir, percentile as reservoirPercentile, normalizeReservoirSize, DEFAULT_RESERVOIR_SIZE } from './utils/reservoir';
export { isReadOnlyBashCommand, BashResultCache } from './tools/bash-cache';
export { FILE_IMAGES_KINDS } from './tools/file-read';
export { MCP_AUTH_STATUS, MCP_AUTH_ERROR_KINDS, redactAuthUrl, classifyMcpAuthError } from './tools/mcp-auth';
export type { McpAuthStatus, McpAuthErrorKind, McpAuthServerConfig } from './tools/mcp-auth';
export { scheduleWakeupTool, clampWakeupDelay, resetLoopPacingState, LOOP_CONTINUE_SENTINEL, NOOP_STREAK_WARN_THRESHOLD, WAKEUP_MIN_DELAY_SECONDS, WAKEUP_MAX_DELAY_SECONDS } from './tools/schedule-wakeup';
export { CODE_LOADING_VARIABLES, POLICY_SOURCE_VARIABLES, detectCodeLoadingEnvSet, detectDangerousRm, detectNuclearCommand, detectBashArgv0Assignment, detectBashpidAssignment, detectZshRegexConditional, detectDnsExfiltration, detectPersistenceCommands, detectDangerousDd, detectContainerEscape, detectBase64ShellPipe, detectPrivilegeEscalation, detectPackageManagerHijack, detectAliasRedefinition, detectSourceExecution, extractInputRedirectionPaths, detectUnsafeFileCommand, detectShellStartupWrite, detectGitInternalWrite, detectPowerShellExecution, detectArchiveToolExecution } from './tools/bash-security';
export type { NuclearCheckResult } from './tools/bash-security';
export { CLM_ALLOWED_TYPES, normalizeTypeName, isClmAllowedType, detectUnsafeTypeCast, CODE_EXEC_CMDLETS, MODULE_LOADING_CMDLETS, WMI_CIM_CMDLETS, ALIAS_HIJACK_CMDLETS, ENV_WRITE_CMDLETS, detectUnsafeNewObject, detectStartProcessElevation, detectRuntimeStateHijack, detectEnvVarManipulation, detectDangerousPsCmdlets, detectStopParsingToken, detectDangerousPowerShellCommand } from './tools/powershell-security';
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
  _resetDockerAvailableCache,
} from './tools/docker-sandbox';
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
export { TurnMetrics } from './turn-metrics';
export { isGrepNoMatchExit, stripGrepFlags } from './tools/grep';
export { expandBraces, estimateBraceExpansions } from './brace-expansion';
export { SubagentLimiter } from './subagent-limiter';

// Features awaiting consumer wiring — exported for dead-code compliance (see AGENTS.md Dead Code Policy)
export { AdmissionLog, ADMISSION_ACTIONS, ADMISSION_OUTCOMES, ADMISSION_SUBJECTS } from './admission-log';
export type { AdmissionEntry, AdmissionAction, AdmissionOutcome, AdmissionSubject } from './admission-log';
export { isPinnedVariable, getPinCategory, getAllPinnedVariables, EnvProtectionGuard, PIN_CATEGORIES, PIN_CATEGORY_RISK, explainPinnedBlock } from './env-protection';
export type { PinCategory, EnvSetAttempt, EnvCheckResult } from './env-protection';
export { SAFE_ENV_VARS, isSafeEnvVar, isDangerousSettingsEnvVar, PROVIDER_MANAGED_ENV_VARS, isProviderManagedEnvVar, ENV_REJECT_REASONS, ENV_TRUST_TIERS, filterSettingsEnv, isHostManagedProvider, NETWORK_TRUST_VARS, isNetworkTrustEnvVar, classifyUntrustedEnvVar, CODE_LOADING_PREFIXES } from './env-protection';
export type { EnvRejectReason, EnvTrustTier, SettingsEnvFilterResult } from './env-protection';
export { isProtectedEvent, canHookEvent, PROTECTED_EVENTS, PASSTHROUGH_EVENTS, HOOK_TIERS, sortHooksByTier, isStricterVerdict, HookChainGuard, isManagedHookSource, canRunHookUnderManagedOnly, filterManagedOnlyHooks } from './hook-protection';
export type { HookSourceKind } from './hook-protection';
export type { HookTier, HookRegistration } from './hook-protection';
export { isModAllowed, isModBlockedByPolicy, getManagedPluginNames, parseModId, normalizeModName, modNamesMatch, validateModId, ManagedModsEnforcer } from './managed-mods';
export type { ModInfo, ManagedModsPolicy } from './managed-mods';
export { resolveProvenance, isProvenanceTrusted, ProvenanceTracker, summarizeProvenance, PROVENANCE_TIERS, PROVENANCE_TIER_INFO, PROVENANCE_INTEGRITY_INFO, RISK_CAPABILITIES, describeTier, describeIntegrity, provenanceWarning, elevatedCapabilities, hasElevatedCapabilities, assessProvenanceRisk } from './provenance';
export type { ProvenanceTier, ProvenanceRecord, ProvenanceSummary, ProvenanceIntegrity, ProvenanceSeverity, ProvenanceTierInfo, ProvenanceIntegrityInfo, RiskCapabilityName, RiskCapabilities, SourceCapabilities, ProvenanceRiskAssessment } from './provenance';
export { parseRateLimitHeaders, parseRetryAfterHeader, shouldBackoff, suggestedWaitMs, formatResetTime, formatRateLimitReset } from './rate-limit-parser';
export type { RateLimitInfo } from './rate-limit-parser';
export { canOverride, getEffectiveTier, filterManagedKeys, SettingsGuard, SETTINGS_TIERS } from './settings-guard';
export type { SettingsTier, SettingsSource } from './settings-guard';
export { MANAGED_ONLY_KEYS, TIER_LOCKED_KEYS, isBypassPermissionsModeDisabled, isAllHooksDisabled, isManagedHooksOnly } from './settings-guard';
export { DANGEROUS_SHELL_SETTINGS, extractDangerousSettings, hasDangerousSettings, hasDangerousSettingsChanged, formatDangerousSettingsList } from './settings-guard';
export type { DangerousShellSetting, DangerousSettings, DangerousSettingsInput } from './settings-guard';
export { getSettingsTierShortName, getSettingsTierDisplayName, getSettingsTierDisplayNameLowercase, getSettingsTierLabels, getManagedSettingsSources } from './settings-guard';
export type { ManagedSettingsSource } from './settings-guard';
export { areSandboxSettingsLockedByPolicy, validateManagedPolicyValues } from './settings-guard';
export type { SandboxPolicyLockInput, ManagedPolicyWarning } from './settings-guard';
export { CUSTOMIZATION_SURFACES, parseStrictPluginOnlyCustomization, isRestrictedToPluginOnly, isSourceAdminTrusted, ADMIN_TRUSTED_SOURCES } from './plugin-only-policy';
export type { CustomizationSurface, StrictPluginOnlyPolicy } from './plugin-only-policy';
export { reviewManagedSettings, managedApprovalPath, loadManagedApproval, saveManagedApproval, MANAGED_SETTINGS_DECISIONS } from './managed-settings-guard';
export type { ManagedSettingsDecision, ManagedSettingsReview } from './managed-settings-guard';
export { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from './tools/command-normalize';
export { detectToolConflict, isReservedToolName, ToolRegistryGuard, validateToolName, validateToolDefinition, filterAvailableTools, toolMatchesName, findToolByName, TOOL_NAME_PATTERN, MAX_TOOL_NAME_LENGTH } from './tools/tool-registry-guard';
export type { ToolConflict, RegistrableToolLike } from './tools/tool-registry-guard';
export { RESERVED_TOOL_PREFIXES } from './tools/tool-registry-guard';
export { GLOB_RIPGREP_ENV, isRipgrepGlobEnabled } from './tools/glob';
export { countToolUses, extractFilesModified, extractConversationMeta, getConversationDuration, isHumanTurn, estimateCompactionMessageTokens, dropOrphanedToolResults, selectRecentWithinBudget, buildTruncationMarker } from './conversation-summary';
export type { ConversationMeta, BudgetSelection } from './conversation-summary';
export { sanitizeToolInput, validateRequiredFields, detectPromptInjection, recoverJsonStringFields } from './tools/input-sanitizer';
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
export { TelemetryBatchSender, sendOtlpBatch, postOtlpBatch, isRetryableStatus, toOtlpLogsPayload, otlpLogsUrl } from './analytics';
export type { OtlpSendResult, TelemetryBatchSenderOpts } from './analytics';
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
} from './lsp';
export type {
  LspServerConfig,
  NotificationHandler,
  RequestHandler,
  Diagnostic,
  DiagnosticFile,
  DiagnosticSource,
  DiagnosticSeverityName,
  DiagnosticRange,
  DiagnosticPosition,
  PublishDiagnosticsParams,
} from './lsp';

// --- batch #10 additive parity exports ---
export { mapChildToSubagentEvent, summarizeToolInput, createSubagentRelay } from './tools/subagent-relay';
export type { SubagentRelay, SubagentState, RelayMessage, RelaySendResult, RelayRejectReason, RelayCompletion } from './tools/subagent-relay';
export { parseEnvInt, parseEnvFloat, parseEnvBool, resolveEnvVar, parseDotenv, loadDotenv } from './utils/env';
export type { DotenvParseOptions, LoadDotenvOptions } from './utils/env';
export { normalizePermissionMode, PERMISSION_MODE_ALIASES, PERMISSION_MODE_CYCLE, cyclePermissionMode } from './permissions';

// --- batch #11 additive parity exports ---
export { cronListTool, isActiveJob, sortJobsForDisplay, CRON_LIST_STATUSES } from './tools/cron-list';
export type { CronListStatus } from './tools/cron-list';
