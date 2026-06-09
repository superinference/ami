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
});
