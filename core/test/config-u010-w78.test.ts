/**
 * U010 (w78) — Engine-behavior config knobs that `mergeConfigs` previously DROPPED.
 *
 * A cluster of config-file keys (architectEnabled, embeddedSearchTools,
 * teammateExecutionMode, unattendedRetry, fileCheckpointingEnabled, features,
 * max529RetriesBeforeFallback, openrouterZdr, strictMcpConfig, mcpRequireApproval,
 * maxUsageLimitWaitMs, sessionMemoryInterval, alwaysLoadMcpServers,
 * continueOnInvalidConfig, outputStyle, architectModel, includeGitInstructions) was
 * already READ off the MERGED config by the engine factory, yet NONE was copied by
 * {@link mergeConfigs} — so a value set in `.ami/config.json` was silently discarded
 * and the documented "project-config <key>" knob never took effect.
 *
 * RED on revert: before the fix `mergeConfigs` does not copy these keys, so every
 * "flows through the merge" assertion below fails (the merged value is `undefined`);
 * and the keys were absent from the validation tables, so `findUnknownConfigKeys`
 * flagged them as typos and `validateProjectConfigValues` never type-checked them.
 *
 * Run from core/core: node --import tsx --test test/config-u010-w78.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mergeConfigs,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  validateProjectConfigValues,
  type ProjectConfig,
} from '../src/config';

describe('U010 w78 — merged engine-behavior config knobs', () => {
  it('mergeConfigs carries every new scalar/number knob from the project tier', () => {
    const project: ProjectConfig = {
      architectEnabled: false,
      architectModel: 'arch-x',
      embeddedSearchTools: true,
      teammateExecutionMode: 'in-process',
      unattendedRetry: true,
      fileCheckpointingEnabled: false,
      max529RetriesBeforeFallback: 3,
      openrouterZdr: true,
      strictMcpConfig: true,
      mcpRequireApproval: true,
      maxUsageLimitWaitMs: 4000,
      sessionMemoryInterval: 7,
      continueOnInvalidConfig: true,
      outputStyle: 'concise',
      includeGitInstructions: false,
    };
    const merged = mergeConfigs({}, {}, project, null);
    // RED pre-fix: each of these is undefined because mergeConfigs never copied it.
    assert.equal(merged.architectEnabled, false);
    assert.equal(merged.architectModel, 'arch-x');
    assert.equal(merged.embeddedSearchTools, true);
    assert.equal(merged.teammateExecutionMode, 'in-process');
    assert.equal(merged.unattendedRetry, true);
    assert.equal(merged.fileCheckpointingEnabled, false);
    assert.equal(merged.max529RetriesBeforeFallback, 3);
    assert.equal(merged.openrouterZdr, true);
    assert.equal(merged.strictMcpConfig, true);
    assert.equal(merged.mcpRequireApproval, true);
    assert.equal(merged.maxUsageLimitWaitMs, 4000);
    assert.equal(merged.sessionMemoryInterval, 7);
    assert.equal(merged.continueOnInvalidConfig, true);
    assert.equal(merged.outputStyle, 'concise');
    assert.equal(merged.includeGitInstructions, false);
  });

  it('higher-priority layers win for scalars (local > project > global)', () => {
    const global: ProjectConfig = { outputStyle: 'g', architectEnabled: true };
    const project: ProjectConfig = { outputStyle: 'p' };
    const local: ProjectConfig = { outputStyle: 'l' };
    const merged = mergeConfigs({}, {}, project, global, local);
    assert.equal(merged.outputStyle, 'l');
    // A key set only at the global tier still survives when no higher tier sets it.
    assert.equal(merged.architectEnabled, true);
  });

  it('alwaysLoadMcpServers ACCUMULATES across layers (augment, not clobber)', () => {
    const global: ProjectConfig = { alwaysLoadMcpServers: ['a'] };
    const project: ProjectConfig = { alwaysLoadMcpServers: ['b', 'a'] };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual([...merged.alwaysLoadMcpServers!].sort(), ['a', 'b']);
  });

  it('features map merges KEY-WISE (a higher layer overrides one feature)', () => {
    const global: ProjectConfig = { features: { task_tracker: true, bg_sessions: false } };
    const project: ProjectConfig = { features: { bg_sessions: true } };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.features, { task_tracker: true, bg_sessions: true });
  });

  it('managed/global waterfall order is preserved for the new knobs', () => {
    const managed: ProjectConfig = { strictMcpConfig: true };
    const project: ProjectConfig = { strictMcpConfig: false };
    // Non-managed-only scalar: last-layer-wins → project overrides managed here
    // (these are user-level knobs, not MANAGED_ONLY_KEYS).
    const merged = mergeConfigs({}, {}, project, null, null, managed);
    assert.equal(merged.strictMcpConfig, false);
  });

  it('all new knobs are recognized keys (no spurious unknown-key typo warning)', () => {
    const cfg: ProjectConfig = {
      architectEnabled: false, architectModel: 'a', embeddedSearchTools: true,
      teammateExecutionMode: 'auto', unattendedRetry: true, fileCheckpointingEnabled: true,
      features: {}, max529RetriesBeforeFallback: 1, openrouterZdr: true, strictMcpConfig: true,
      mcpRequireApproval: true, maxUsageLimitWaitMs: 1, sessionMemoryInterval: 1,
      alwaysLoadMcpServers: ['x'], continueOnInvalidConfig: true, outputStyle: 'o',
      includeGitInstructions: true,
    };
    assert.deepEqual(findUnknownConfigKeys(cfg), []);
    for (const k of Object.keys(cfg)) {
      assert.ok(KNOWN_CONFIG_KEYS.has(k), `expected recognized key: ${k}`);
    }
  });

  it('value validation surfaces a wrong base type for a new knob', () => {
    const issues = validateProjectConfigValues({
      // @ts-expect-error intentional wrong type (string where boolean expected)
      architectEnabled: 'yes',
      // @ts-expect-error intentional wrong type (string where number expected)
      max529RetriesBeforeFallback: 'three',
    });
    const fields = issues.map(i => i.field);
    assert.ok(fields.includes('architectEnabled'), 'architectEnabled type issue expected');
    assert.ok(fields.includes('max529RetriesBeforeFallback'), 'max529RetriesBeforeFallback type issue expected');
  });

  it('a correctly-typed config for the new knobs yields no validation issues', () => {
    const issues = validateProjectConfigValues({
      architectEnabled: true, max529RetriesBeforeFallback: 2, alwaysLoadMcpServers: ['s'],
      features: { x: true },
    });
    assert.deepEqual(issues, []);
  });
});
