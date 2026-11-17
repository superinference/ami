/**
 * U010 (w80) — Engine-behavior config knobs (round 2) that `mergeConfigs` DROPPED.
 *
 * A second cluster of config-file keys (autoMemoryEnabled, autoDreamEnabled,
 * sessionMemoryEnabled, enableTitleGeneration, worktreeToolsEnabled,
 * terminalProgressBarEnabled, showTurnDuration) was already READ off the MERGED project
 * config by a `common/src/engine-factory.ts` resolver (resolveAutoMemory /
 * resolveTitleGenerationEnabled / resolveWorktreeToolsEnabled) or intended to drive a
 * startup env bridge, yet NONE was copied by {@link mergeConfigs} — so a value set in
 * `.ami/config.json` was silently discarded. This is the same class of bug the w78
 * cluster fixed, with newly-found keys.
 *
 * RED on revert: before the fix `mergeConfigs` does not copy these keys, so every
 * "flows through the merge" assertion is `undefined`; the keys were absent from the
 * validation tables, so `findUnknownConfigKeys` flagged them as typos; and the two new
 * env bridges (`applyConfigTerminalProgress` / `applyConfigTurnDuration`) did not exist.
 *
 * Run from core/core: node --import tsx --test test/config-u010-w80.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mergeConfigs,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  validateProjectConfigValues,
  applyConfigTerminalProgress,
  applyConfigTurnDuration,
  applyConfigStartupKnobs,
  NO_PROGRESS_ENV,
  SHOW_TURN_DURATION_ENV,
  type ProjectConfig,
} from '../src/config';

const NEW_KEYS = [
  'autoMemoryEnabled',
  'autoDreamEnabled',
  'sessionMemoryEnabled',
  'enableTitleGeneration',
  'worktreeToolsEnabled',
  'terminalProgressBarEnabled',
  'showTurnDuration',
] as const;

describe('U010 w80 — merged engine-behavior config knobs (round 2)', () => {
  it('mergeConfigs carries every new boolean knob from the project tier', () => {
    const project: ProjectConfig = {
      autoMemoryEnabled: false,
      autoDreamEnabled: true,
      sessionMemoryEnabled: false,
      enableTitleGeneration: false,
      worktreeToolsEnabled: false,
      terminalProgressBarEnabled: false,
      showTurnDuration: false,
    };
    const merged = mergeConfigs({}, {}, project, null);
    // RED pre-fix: each is undefined because mergeConfigs never copied it.
    assert.equal(merged.autoMemoryEnabled, false);
    assert.equal(merged.autoDreamEnabled, true);
    assert.equal(merged.sessionMemoryEnabled, false);
    assert.equal(merged.enableTitleGeneration, false);
    assert.equal(merged.worktreeToolsEnabled, false);
    assert.equal(merged.terminalProgressBarEnabled, false);
    assert.equal(merged.showTurnDuration, false);
  });

  it('higher layers override lower ones (last-layer-wins) for the new knobs', () => {
    const global: ProjectConfig = { autoMemoryEnabled: true, showTurnDuration: true };
    const project: ProjectConfig = { autoMemoryEnabled: false };
    const merged = mergeConfigs({}, {}, project, global);
    // project overrides global; a key only in global still survives.
    assert.equal(merged.autoMemoryEnabled, false);
    assert.equal(merged.showTurnDuration, true);
  });

  it('the new keys are recognized (no spurious unknown-key typo warning)', () => {
    const cfg: ProjectConfig = {
      autoMemoryEnabled: true,
      autoDreamEnabled: true,
      sessionMemoryEnabled: true,
      enableTitleGeneration: true,
      worktreeToolsEnabled: true,
      terminalProgressBarEnabled: true,
      showTurnDuration: true,
    };
    assert.deepEqual(findUnknownConfigKeys(cfg), []);
    for (const k of NEW_KEYS) assert.ok(KNOWN_CONFIG_KEYS.has(k), `${k} should be known`);
  });

  it('validateProjectConfigValues rejects a wrong-typed new knob', () => {
    const bad = { terminalProgressBarEnabled: 'yes' } as unknown as ProjectConfig;
    const issues = validateProjectConfigValues(bad);
    assert.ok(
      issues.some(i => i.field === 'terminalProgressBarEnabled'),
      'a string terminalProgressBarEnabled must surface an invalid-value issue',
    );
  });

  it('validateProjectConfigValues accepts correctly-typed new knobs', () => {
    const good: ProjectConfig = {
      autoMemoryEnabled: false,
      enableTitleGeneration: true,
      worktreeToolsEnabled: false,
      showTurnDuration: true,
    };
    assert.deepEqual(validateProjectConfigValues(good), []);
  });
});

describe('U010 w80 — applyConfigTerminalProgress bridge', () => {
  it('seeds AMI_NO_PROGRESS when the knob is explicitly false', () => {
    const target: NodeJS.ProcessEnv = {};
    applyConfigTerminalProgress({ terminalProgressBarEnabled: false }, target);
    assert.equal(target[NO_PROGRESS_ENV], '1');
  });

  it('is a no-op when the knob is true or unset (AMI default-on)', () => {
    const on: NodeJS.ProcessEnv = {};
    applyConfigTerminalProgress({ terminalProgressBarEnabled: true }, on);
    assert.equal(on[NO_PROGRESS_ENV], undefined);
    const unset: NodeJS.ProcessEnv = {};
    applyConfigTerminalProgress({}, unset);
    assert.equal(unset[NO_PROGRESS_ENV], undefined);
    applyConfigTerminalProgress(null, unset);
    assert.equal(unset[NO_PROGRESS_ENV], undefined);
  });

  it('an explicit env value WINS over the config knob', () => {
    const target: NodeJS.ProcessEnv = { [NO_PROGRESS_ENV]: '' };
    applyConfigTerminalProgress({ terminalProgressBarEnabled: false }, target);
    assert.equal(target[NO_PROGRESS_ENV], ''); // unchanged — env wins
  });
});

describe('U010 w80 — applyConfigTurnDuration bridge', () => {
  it("seeds AMI_SHOW_TURN_DURATION='0' when the knob is explicitly false", () => {
    const target: NodeJS.ProcessEnv = {};
    applyConfigTurnDuration({ showTurnDuration: false }, target);
    assert.equal(target[SHOW_TURN_DURATION_ENV], '0');
  });

  it('is a no-op when the knob is true or unset', () => {
    const on: NodeJS.ProcessEnv = {};
    applyConfigTurnDuration({ showTurnDuration: true }, on);
    assert.equal(on[SHOW_TURN_DURATION_ENV], undefined);
    const unset: NodeJS.ProcessEnv = {};
    applyConfigTurnDuration({}, unset);
    assert.equal(unset[SHOW_TURN_DURATION_ENV], undefined);
  });

  it('an explicit env value WINS over the config knob', () => {
    const target: NodeJS.ProcessEnv = { [SHOW_TURN_DURATION_ENV]: 'on' };
    applyConfigTurnDuration({ showTurnDuration: false }, target);
    assert.equal(target[SHOW_TURN_DURATION_ENV], 'on');
  });
});

describe('U010 w80 — applyConfigStartupKnobs wires both new bridges', () => {
  it('applies the terminal-progress and turn-duration bridges in one call', () => {
    const target: NodeJS.ProcessEnv = {};
    applyConfigStartupKnobs(
      { terminalProgressBarEnabled: false, showTurnDuration: false },
      target,
    );
    assert.equal(target[NO_PROGRESS_ENV], '1');
    assert.equal(target[SHOW_TURN_DURATION_ENV], '0');
  });
});
