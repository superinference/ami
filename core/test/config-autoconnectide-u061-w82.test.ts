/**
 * U061 (w82) — `autoConnectIde` config knob threading through mergeConfigs.
 *
 * The engine's IDE auto-connect waterfall (shouldAutoConnectIde, mcp/manager.ts)
 * reads a persisted `autoConnectIde` setting as its `configEnabled` input, and the
 * engine factory threads it into EngineConfig.autoConnectIde. But a file-tier value
 * only reaches the factory if mergeConfigs copies the key. This locks that in:
 *
 *   - mergeConfigs carries `autoConnectIde` from the project tier (RED on revert:
 *     before the fix mergeConfigs dropped it, so the merged value was undefined);
 *   - higher layers override lower ones (last-layer-wins);
 *   - the key is recognized (no spurious unknown-key typo warning) and type-checked.
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

describe('U061 w82 — autoConnectIde config-knob merge threading', () => {
  it('mergeConfigs carries autoConnectIde from the project tier', () => {
    const merged = mergeConfigs({}, {}, { autoConnectIde: true } as ProjectConfig, null);
    assert.equal(merged.autoConnectIde, true);
    const mergedOff = mergeConfigs({}, {}, { autoConnectIde: false } as ProjectConfig, null);
    assert.equal(mergedOff.autoConnectIde, false);
  });

  it('leaves autoConnectIde undefined when no tier sets it', () => {
    assert.equal(mergeConfigs({}, {}, {} as ProjectConfig, null).autoConnectIde, undefined);
  });

  it('higher layers override lower ones (last-layer-wins)', () => {
    const project: ProjectConfig = { autoConnectIde: false } as ProjectConfig;
    const global: ProjectConfig = { autoConnectIde: true } as ProjectConfig;
    // Order: managed, global, project, local, env, cliArgs — project beats global.
    const merged = mergeConfigs({}, {}, project, global);
    assert.equal(merged.autoConnectIde, false);
  });

  it('is a recognized key (no spurious unknown-key typo warning)', () => {
    assert.deepEqual(findUnknownConfigKeys({ autoConnectIde: true } as ProjectConfig), []);
    assert.ok(KNOWN_CONFIG_KEYS.has('autoConnectIde'));
  });

  it('validateProjectConfigValues rejects a wrong-typed autoConnectIde', () => {
    const issues = validateProjectConfigValues({ autoConnectIde: 'yes' as unknown as boolean });
    assert.ok(issues.some((i) => /autoConnectIde/.test(typeof i === 'string' ? i : JSON.stringify(i))));
  });

  it('validateProjectConfigValues accepts a correctly-typed autoConnectIde', () => {
    assert.deepEqual(validateProjectConfigValues({ autoConnectIde: true }), []);
  });
});
