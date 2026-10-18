import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { CacheMissDiagnostics } from '../src/cost-tracker';
import type { CacheMissCause } from '../src/cost-tracker';

describe('CacheMissDiagnostics', () => {
  let diag: CacheMissDiagnostics;

  beforeEach(() => {
    diag = new CacheMissDiagnostics();
  });

  // -----------------------------------------------------------------------
  // Core cause detection
  // -----------------------------------------------------------------------

  it('reports first_request on the very first miss', () => {
    diag.recordRequest({ toolDefHash: 'abc', systemPromptHash: 'def', thinkingMode: 'off', model: 'gpt-4o' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'first_request');
  });

  it('reports tool_definitions_changed when tool defs hash changes', () => {
    diag.recordRequest({ toolDefHash: 'v1', systemPromptHash: 's1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0); // cache hit

    diag.recordRequest({ toolDefHash: 'v2', systemPromptHash: 's1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000); // cache miss
    assert.equal(diag.getLastMissCause(), 'tool_definitions_changed');
  });

  it('reports system_prompt_changed when system prompt hash changes', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p2', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'system_prompt_changed');
  });

  it('reports thinking_mode_changed when thinking mode changes', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'high', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'thinking_mode_changed');
  });

  it('reports model_switched when model changes', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'gpt-4o' });
    diag.recordResponse(500, 0);

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'claude-sonnet-4' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'model_switched');
  });

  it('reports idle_past_ttl when idle time exceeds TTL', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    // Simulate passage of time beyond TTL
    diag._setLastRequestTime(Date.now() - 6 * 60 * 1000); // 6 minutes ago (TTL default = 5m)

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'idle_past_ttl');
  });

  it('reports unknown when nothing changed but cache missed', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'unknown');
  });

  // -----------------------------------------------------------------------
  // Cache hits — should NOT record a miss cause
  // -----------------------------------------------------------------------

  it('does not record a cause on cache hit', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0); // hit

    assert.equal(diag.getLastMissCause(), null);
    assert.equal(diag.getCacheMissCauses().size, 0);
  });

  it('does not count cache hit after a miss', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000); // miss (first_request)

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0); // hit

    const causes = diag.getCacheMissCauses();
    assert.equal(causes.get('first_request'), 1);
    assert.equal(causes.size, 1); // only the first miss
  });

  // -----------------------------------------------------------------------
  // Accumulation
  // -----------------------------------------------------------------------

  it('accumulates multiple misses by cause', () => {
    // First miss: first_request
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);

    // Second miss: tool_definitions_changed
    diag.recordRequest({ toolDefHash: 't2', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);

    // Third miss: tool_definitions_changed again
    diag.recordRequest({ toolDefHash: 't3', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);

    const causes = diag.getCacheMissCauses();
    assert.equal(causes.get('first_request'), 1);
    assert.equal(causes.get('tool_definitions_changed'), 2);
    assert.equal(causes.size, 2);
  });

  it('getTotalMisses returns sum of all causes', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000); // first_request

    diag.recordRequest({ toolDefHash: 't2', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000); // tool_definitions_changed

    diag.recordRequest({ toolDefHash: 't2', systemPromptHash: 'p2', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000); // system_prompt_changed

    assert.equal(diag.getTotalMisses(), 3);
  });

  // -----------------------------------------------------------------------
  // Priority: when multiple things change at once, pick the most likely
  // -----------------------------------------------------------------------

  it('prioritises model_switched over other changes', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    // Change everything at once
    diag.recordRequest({ toolDefHash: 't2', systemPromptHash: 'p2', thinkingMode: 'high', model: 'm2' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'model_switched');
  });

  it('prioritises tool_definitions_changed over system_prompt_changed', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    diag.recordRequest({ toolDefHash: 't2', systemPromptHash: 'p2', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'tool_definitions_changed');
  });

  // -----------------------------------------------------------------------
  // Configurable TTL
  // -----------------------------------------------------------------------

  it('respects custom cacheTtlMs', () => {
    const custom = new CacheMissDiagnostics(60_000); // 1 minute TTL

    custom.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    custom.recordResponse(500, 0);

    // 2 minutes idle — past the 1m TTL
    custom._setLastRequestTime(Date.now() - 2 * 60 * 1000);

    custom.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    custom.recordResponse(0, 1000);
    assert.equal(custom.getLastMissCause(), 'idle_past_ttl');
  });

  it('does not report idle_past_ttl within TTL window', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(500, 0);

    // Only 2 minutes idle — within the default 5m TTL
    diag._setLastRequestTime(Date.now() - 2 * 60 * 1000);

    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'unknown'); // not idle
  });

  // -----------------------------------------------------------------------
  // Reset
  // -----------------------------------------------------------------------

  it('reset() clears all diagnostic state', () => {
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);

    diag.reset();

    assert.equal(diag.getLastMissCause(), null);
    assert.equal(diag.getCacheMissCauses().size, 0);
    assert.equal(diag.getTotalMisses(), 0);

    // After reset, next miss should be first_request again
    diag.recordRequest({ toolDefHash: 't1', systemPromptHash: 'p1', thinkingMode: 'off', model: 'm1' });
    diag.recordResponse(0, 1000);
    assert.equal(diag.getLastMissCause(), 'first_request');
  });

  // -----------------------------------------------------------------------
  // fingerprint helper
  // -----------------------------------------------------------------------

  it('fingerprint produces stable hashes for same input', () => {
    const { fingerprint } = require('../src/cost-tracker');
    const a = fingerprint('hello world this is a test string');
    const b = fingerprint('hello world this is a test string');
    assert.equal(a, b);
  });

  it('fingerprint produces different hashes for different input', () => {
    const { fingerprint } = require('../src/cost-tracker');
    const a = fingerprint('hello world');
    const b = fingerprint('goodbye world');
    assert.notEqual(a, b);
  });

  it('fingerprint handles empty string', () => {
    const { fingerprint } = require('../src/cost-tracker');
    const a = fingerprint('');
    assert.equal(typeof a, 'string');
    assert.ok(a.length > 0);
  });
});
