import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import * as core from '../src/index';

// ---------------------------------------------------------------------------
// Verify all key exports exist
// ---------------------------------------------------------------------------
describe('index exports', () => {
  // Classes / constructors
  it('exports SessionManager', () => {
    assert.equal(typeof core.SessionManager, 'function');
  });

  it('exports ToolRegistry', () => {
    assert.equal(typeof core.ToolRegistry, 'function');
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

  it('exports FileCache', () => {
    assert.equal(typeof core.FileCache, 'function');
  });

  it('exports Profiler', () => {
    assert.equal(typeof core.Profiler, 'function');
  });

  it('exports WorkspaceIndexer', () => {
    assert.equal(typeof core.WorkspaceIndexer, 'function');
  });

  it('exports ToolCallGuardrailController', () => {
    assert.equal(typeof core.ToolCallGuardrailController, 'function');
  });

  it('exports PersonaManager', () => {
    assert.equal(typeof core.PersonaManager, 'function');
  });

  // SuperInference exports
  it('exports SuperInferenceEngine', () => {
    assert.equal(typeof core.SuperInferenceEngine, 'function');
  });

  it('exports BeliefTracker', () => {
    assert.equal(typeof core.BeliefTracker, 'function');
  });

  it('exports Critic', () => {
    assert.equal(typeof core.Critic, 'function');
  });

  it('exports MemoryGate', () => {
    assert.equal(typeof core.MemoryGate, 'function');
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

  it('exports toolDefinitionsToOpenAI', () => {
    assert.equal(typeof core.toolDefinitionsToOpenAI, 'function');
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

  it('exports getFileCache', () => {
    assert.equal(typeof core.getFileCache, 'function');
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

  it('exports getLogPath', () => {
    assert.equal(typeof core.getLogPath, 'function');
  });

  // Model capabilities exports
  it('exports isReasoningModel', () => {
    assert.equal(typeof core.isReasoningModel, 'function');
  });

  it('exports getModelCapabilities', () => {
    assert.equal(typeof core.getModelCapabilities, 'function');
  });

  it('exports getContextWindow', () => {
    assert.equal(typeof core.getContextWindow, 'function');
  });

  it('exports resolveThinkingBudget', () => {
    assert.equal(typeof core.resolveThinkingBudget, 'function');
  });

  it('exports resolveTemperature', () => {
    assert.equal(typeof core.resolveTemperature, 'function');
  });

  it('exports getProviderSamplingDefaults', () => {
    assert.equal(typeof core.getProviderSamplingDefaults, 'function');
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

  it('exports formatModelList', () => {
    assert.equal(typeof core.formatModelList, 'function');
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

  it('exports stripJsoncComments', () => {
    assert.equal(typeof core.stripJsoncComments, 'function');
  });

  // Provider transform exports
  it('exports applyCacheControl', () => {
    assert.equal(typeof core.applyCacheControl, 'function');
  });

  it('exports sanitizeToolCallIds', () => {
    assert.equal(typeof core.sanitizeToolCallIds, 'function');
  });

  // Fuzzy match exports
  it('exports fuzzyFindAndReplace', () => {
    assert.equal(typeof core.fuzzyFindAndReplace, 'function');
  });

  it('exports findClosestLines', () => {
    assert.equal(typeof core.findClosestLines, 'function');
  });

  it('exports reindentReplacement', () => {
    assert.equal(typeof core.reindentReplacement, 'function');
  });

  // Title generator export
  it('exports generateTitle', () => {
    assert.equal(typeof core.generateTitle, 'function');
  });

  // Prompt categorizer export
  it('exports categorizePrompt', () => {
    assert.equal(typeof core.categorizePrompt, 'function');
  });

  // Token utility exports
  it('exports estimateTokens', () => {
    assert.equal(typeof core.estimateTokens, 'function');
  });

  it('exports estimateToolSchemaTokens', () => {
    assert.equal(typeof core.estimateToolSchemaTokens, 'function');
  });

  it('exports truncateToTokenLimit', () => {
    assert.equal(typeof core.truncateToTokenLimit, 'function');
  });
});
