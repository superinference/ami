import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import * as core from '../src/index';

// ---------------------------------------------------------------------------
// Verify all key exports exist — must match src/index.ts barrel exactly
// ---------------------------------------------------------------------------
describe('index exports', () => {
  // Classes / constructors
  it('exports SessionManager', () => {
    assert.equal(typeof core.SessionManager, 'function');
  });

  it('exports MemoryManager', () => {
    assert.equal(typeof core.MemoryManager, 'function');
  });

  it('exports SkillManager', () => {
    assert.equal(typeof core.SkillManager, 'function');
  });

  it('exports CostTracker', () => {
    assert.equal(typeof core.CostTracker, 'function');
  });

  it('exports PermissionManager', () => {
    assert.equal(typeof core.PermissionManager, 'function');
  });

  it('exports HookManager', () => {
    assert.equal(typeof core.HookManager, 'function');
  });

  it('exports SessionMemoryExtractor', () => {
    assert.equal(typeof core.SessionMemoryExtractor, 'function');
  });

  it('exports AnalyticsTracker', () => {
    assert.equal(typeof core.AnalyticsTracker, 'function');
  });

  it('exports Profiler', () => {
    assert.equal(typeof core.Profiler, 'function');
  });

  it('exports WorkspaceIndexer', () => {
    assert.equal(typeof core.WorkspaceIndexer, 'function');
  });

  it('exports BUILTIN_PERSONAS', () => {
    assert.ok(Array.isArray(core.BUILTIN_PERSONAS));
    assert.ok(core.BUILTIN_PERSONAS.length >= 4);
  });

  it('exports matchMcpAutoAllow', () => {
    assert.equal(typeof core.matchMcpAutoAllow, 'function');
  });

  // SuperInference exports
  it('exports SuperInferenceEngine', () => {
    assert.equal(typeof core.SuperInferenceEngine, 'function');
  });

  it('exports Critic', () => {
    assert.equal(typeof core.Critic, 'function');
  });

  it('exports Retriever', () => {
    assert.equal(typeof core.Retriever, 'function');
  });

  // Functions
  it('exports streamChatCompletion', () => {
    assert.equal(typeof core.streamChatCompletion, 'function');
  });

  it('exports streamChatCompletionWithRetry', () => {
    assert.equal(typeof core.streamChatCompletionWithRetry, 'function');
  });

  it('exports inferProviderFromApiKey', () => {
    assert.equal(typeof core.inferProviderFromApiKey, 'function');
  });

  it('exports createDefaultTools', () => {
    assert.equal(typeof core.createDefaultTools, 'function');
  });

  it('exports classifyError', () => {
    assert.equal(typeof core.classifyError, 'function');
  });

  it('exports generateToolSummary', () => {
    assert.equal(typeof core.generateToolSummary, 'function');
  });

  // Logger exports
  it('exports coreLog', () => {
    assert.equal(typeof core.coreLog, 'function');
  });

  it('exports logToolCall', () => {
    assert.equal(typeof core.logToolCall, 'function');
  });

  it('exports logApiCall', () => {
    assert.equal(typeof core.logApiCall, 'function');
  });

  it('exports logApiResponse', () => {
    assert.equal(typeof core.logApiResponse, 'function');
  });

  it('exports coreLogError', () => {
    assert.equal(typeof core.coreLogError, 'function');
  });

  // Model capabilities exports
  it('exports getModelCapabilities', () => {
    assert.equal(typeof core.getModelCapabilities, 'function');
  });

  // Model registry exports
  it('exports detectProvider', () => {
    assert.equal(typeof core.detectProvider, 'function');
  });

  it('exports listModels', () => {
    assert.equal(typeof core.listModels, 'function');
  });

  it('exports validateModel', () => {
    assert.equal(typeof core.validateModel, 'function');
  });

  // Config exports
  it('exports loadProjectConfig', () => {
    assert.equal(typeof core.loadProjectConfig, 'function');
  });

  it('exports loadGlobalConfig', () => {
    assert.equal(typeof core.loadGlobalConfig, 'function');
  });

  it('exports mergeConfigs', () => {
    assert.equal(typeof core.mergeConfigs, 'function');
  });

  // Token utility exports
  it('exports estimateTokens', () => {
    assert.equal(typeof core.estimateTokens, 'function');
  });

  it('exports estimateToolSchemaTokens', () => {
    assert.equal(typeof core.estimateToolSchemaTokens, 'function');
  });

  // Additional re-exports
  it('exports ProviderCoordinator', () => {
    assert.equal(typeof core.ProviderCoordinator, 'function');
  });

  it('exports discoverPlugins', () => {
    assert.equal(typeof core.discoverPlugins, 'function');
  });

  it('exports categorizePrompt', () => {
    assert.equal(typeof core.categorizePrompt, 'function');
  });

  it('exports generateTitle', () => {
    assert.equal(typeof core.generateTitle, 'function');
  });

  it('exports ProcessManager', () => {
    assert.equal(typeof core.ProcessManager, 'function');
  });

  it('exports TurnMetrics', () => {
    assert.equal(typeof core.TurnMetrics, 'function');
  });

  it('exports ToolAnalytics', () => {
    assert.equal(typeof core.ToolAnalytics, 'function');
  });

  it('exports estimateMessageTokens', () => {
    assert.equal(typeof core.estimateMessageTokens, 'function');
  });

  it('exports estimateConversationTokens', () => {
    assert.equal(typeof core.estimateConversationTokens, 'function');
  });

  it('exports TelemetryBatchSender', () => {
    assert.equal(typeof core.TelemetryBatchSender, 'function');
  });

  it('exports SessionDiffTracker', () => {
    assert.equal(typeof core.SessionDiffTracker, 'function');
  });

  it('exports fingerprint', () => {
    assert.equal(typeof core.fingerprint, 'function');
  });

  it('exports CacheMissDiagnostics', () => {
    assert.equal(typeof core.CacheMissDiagnostics, 'function');
  });

  it('exports safeReadFile', () => {
    assert.equal(typeof core.safeReadFile, 'function');
  });

  it('exports safeLstat', () => {
    assert.equal(typeof core.safeLstat, 'function');
  });

  it('exports boundMcpResponse', () => {
    assert.equal(typeof core.boundMcpResponse, 'function');
  });

  it('exports friendlyErrorMessage', () => {
    assert.equal(typeof core.friendlyErrorMessage, 'function');
  });

  it('exports sanitizeHookOutput', () => {
    assert.equal(typeof core.sanitizeHookOutput, 'function');
  });

  it('exports EventConcurrencyLimiter', () => {
    assert.equal(typeof core.EventConcurrencyLimiter, 'function');
  });

  it('exports verdictRank', () => {
    assert.equal(typeof core.verdictRank, 'function');
  });

  it('exports holdsOver', () => {
    assert.equal(typeof core.holdsOver, 'function');
  });

  it('exports containsUnquotedExpansion', () => {
    assert.equal(typeof core.containsUnquotedExpansion, 'function');
  });

  it('exports isBuildConfigFile', () => {
    assert.equal(typeof core.isBuildConfigFile, 'function');
  });

  it('exports matchesPaths', () => {
    assert.equal(typeof core.matchesPaths, 'function');
  });

  it('exports registerModelCapabilities', () => {
    assert.equal(typeof core.registerModelCapabilities, 'function');
  });

  it('exports clearDynamicCapabilities', () => {
    assert.equal(typeof core.clearDynamicCapabilities, 'function');
  });

  it('exports discoverFromModelList', () => {
    assert.equal(typeof core.discoverFromModelList, 'function');
  });

  it('exports buildSubsystems', () => {
    assert.equal(typeof core.buildSubsystems, 'function');
  });

  it('exports validateEngineConfig', () => {
    assert.equal(typeof core.validateEngineConfig, 'function');
  });

  it('exports sanitizeEngineConfig', () => {
    assert.equal(typeof core.sanitizeEngineConfig, 'function');
  });

  it('exports MAX_WORKTREES', () => {
    assert.equal(typeof core.MAX_WORKTREES, 'number');
  });

  it('exports validateWorktreeGitFile', () => {
    assert.equal(typeof core.validateWorktreeGitFile, 'function');
  });

  it('exports findStaleWorktrees', () => {
    assert.equal(typeof core.findStaleWorktrees, 'function');
  });

  it('exports isGrepNoMatchExit', () => {
    assert.equal(typeof core.isGrepNoMatchExit, 'function');
  });

  it('exports stripGrepFlags', () => {
    assert.equal(typeof core.stripGrepFlags, 'function');
  });

  it('exports withExtraHeaders', () => {
    assert.equal(typeof core.withExtraHeaders, 'function');
  });

  it('exports levenshteinDistance', () => {
    assert.equal(typeof core.levenshteinDistance, 'function');
  });

  it('exports suggestSimilarPaths', () => {
    assert.equal(typeof core.suggestSimilarPaths, 'function');
  });

  it('exports isReadOnlyBashCommand', () => {
    assert.equal(typeof core.isReadOnlyBashCommand, 'function');
  });

  it('exports BashResultCache', () => {
    assert.equal(typeof core.BashResultCache, 'function');
  });

  it('exports normalizeCommand', () => {
    assert.equal(typeof core.normalizeCommand, 'function');
  });

  it('exports detectToolConflict', () => {
    assert.equal(typeof core.detectToolConflict, 'function');
  });

  it('exports isReservedToolName', () => {
    assert.equal(typeof core.isReservedToolName, 'function');
  });

  it('exports ToolRegistryGuard', () => {
    assert.equal(typeof core.ToolRegistryGuard, 'function');
  });

  it('exports RESERVED_TOOL_PREFIXES', () => {
    assert.ok(Array.isArray(core.RESERVED_TOOL_PREFIXES));
  });

  it('exports sanitizeToolInput', () => {
    assert.equal(typeof core.sanitizeToolInput, 'function');
  });

  it('exports validateRequiredFields', () => {
    assert.equal(typeof core.validateRequiredFields, 'function');
  });

  it('exports detectPromptInjection', () => {
    assert.equal(typeof core.detectPromptInjection, 'function');
  });

  it('exports countToolUses', () => {
    assert.equal(typeof core.countToolUses, 'function');
  });

  it('exports extractFilesModified', () => {
    assert.equal(typeof core.extractFilesModified, 'function');
  });

  it('exports getConversationDuration', () => {
    assert.equal(typeof core.getConversationDuration, 'function');
  });

  it('exports extractConversationMeta', () => {
    assert.equal(typeof core.extractConversationMeta, 'function');
  });

  // Feature-awaiting-wiring exports
  it('exports AdmissionLog', () => {
    assert.equal(typeof core.AdmissionLog, 'function');
  });

  it('exports isPinnedVariable', () => {
    assert.equal(typeof core.isPinnedVariable, 'function');
  });

  it('exports EnvProtectionGuard', () => {
    assert.equal(typeof core.EnvProtectionGuard, 'function');
  });

  it('exports isProtectedEvent', () => {
    assert.equal(typeof core.isProtectedEvent, 'function');
  });

  it('exports HookChainGuard', () => {
    assert.equal(typeof core.HookChainGuard, 'function');
  });

  it('exports ManagedModsEnforcer', () => {
    assert.equal(typeof core.ManagedModsEnforcer, 'function');
  });

  it('exports ProvenanceTracker', () => {
    assert.equal(typeof core.ProvenanceTracker, 'function');
  });

  it('exports parseRateLimitHeaders', () => {
    assert.equal(typeof core.parseRateLimitHeaders, 'function');
  });

  it('exports SettingsGuard', () => {
    assert.equal(typeof core.SettingsGuard, 'function');
  });

  it('exports scanCodeForSecurityPatterns', () => {
    assert.equal(typeof core.scanCodeForSecurityPatterns, 'function');
  });

  it('exports containsCredential', () => {
    assert.equal(typeof core.containsCredential, 'function');
  });

  it('exports CredentialGuard', () => {
    assert.equal(typeof core.CredentialGuard, 'function');
  });

  it('exports SafeModeGuard', () => {
    assert.equal(typeof core.SafeModeGuard, 'function');
  });

  it('exports NonceTracker', () => {
    assert.equal(typeof core.NonceTracker, 'function');
  });

  it('exports isSensitiveEnvVar', () => {
    assert.equal(typeof core.isSensitiveEnvVar, 'function');
  });

  it('exports EnvAccessGuard', () => {
    assert.equal(typeof core.EnvAccessGuard, 'function');
  });

  it('exports UILogGuard', () => {
    assert.equal(typeof core.UILogGuard, 'function');
  });

  it('exports CODE_LOADING_VARIABLES', () => {
    assert.ok(Array.isArray(core.CODE_LOADING_VARIABLES));
  });

  it('exports detectCodeLoadingEnvSet', () => {
    assert.equal(typeof core.detectCodeLoadingEnvSet, 'function');
  });

  it('exports detectDangerousRm', () => {
    assert.equal(typeof core.detectDangerousRm, 'function');
  });

  it('exports detectNuclearCommand', () => {
    assert.equal(typeof core.detectNuclearCommand, 'function');
  });
});
