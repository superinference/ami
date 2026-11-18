import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  applyConfigFullscreen,
  applyConfigStartupKnobs,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  NO_ALT_SCREEN_ENV,
  FORCE_ALT_SCREEN_ENV,
  type ProjectConfig,
} from '../src/config';

// A fresh, isolated env object so no test mutates the real process.env.
function env(init: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { ...init } as NodeJS.ProcessEnv;
}

// ---------------------------------------------------------------------------
// applyConfigFullscreen  (fullscreen -> AMI_NO_ALT_SCREEN / AMI_FORCE_ALT_SCREEN)
// ---------------------------------------------------------------------------
describe('applyConfigFullscreen', () => {
  it('arms AMI_NO_ALT_SCREEN (scrollback layout) when fullscreen === false', () => {
    const t = env();
    applyConfigFullscreen({ fullscreen: false }, t);
    assert.equal(t[NO_ALT_SCREEN_ENV], '1');
    assert.equal(t[FORCE_ALT_SCREEN_ENV], undefined);
  });

  it('arms AMI_FORCE_ALT_SCREEN (fullscreen layout) when fullscreen === true', () => {
    const t = env();
    applyConfigFullscreen({ fullscreen: true }, t);
    assert.equal(t[FORCE_ALT_SCREEN_ENV], '1');
    assert.equal(t[NO_ALT_SCREEN_ENV], undefined);
  });

  it('is a no-op when fullscreen is unset (terminal default decides)', () => {
    const t = env();
    applyConfigFullscreen({}, t);
    assert.equal(t[NO_ALT_SCREEN_ENV], undefined);
    assert.equal(t[FORCE_ALT_SCREEN_ENV], undefined);
    applyConfigFullscreen(null, t);
    assert.deepEqual(t, {});
    applyConfigFullscreen(undefined, t);
    assert.deepEqual(t, {});
  });

  it('an explicit AMI_NO_ALT_SCREEN env value wins over the config knob', () => {
    const t = env({ [NO_ALT_SCREEN_ENV]: '' });
    applyConfigFullscreen({ fullscreen: true }, t);
    // config must not clobber the already-present env override.
    assert.equal(t[NO_ALT_SCREEN_ENV], '');
    assert.equal(t[FORCE_ALT_SCREEN_ENV], undefined);
  });

  it('an explicit AMI_FORCE_ALT_SCREEN env value wins over the config knob', () => {
    const t = env({ [FORCE_ALT_SCREEN_ENV]: '1' });
    applyConfigFullscreen({ fullscreen: false }, t);
    assert.equal(t[FORCE_ALT_SCREEN_ENV], '1');
    assert.equal(t[NO_ALT_SCREEN_ENV], undefined);
  });
});

// ---------------------------------------------------------------------------
// applyConfigStartupKnobs threads the fullscreen bridge
// ---------------------------------------------------------------------------
describe('applyConfigStartupKnobs — fullscreen', () => {
  it('seeds AMI_NO_ALT_SCREEN for fullscreen:false through the aggregator', () => {
    const t = env();
    applyConfigStartupKnobs({ fullscreen: false }, t);
    assert.equal(t[NO_ALT_SCREEN_ENV], '1');
  });

  it('seeds AMI_FORCE_ALT_SCREEN for fullscreen:true through the aggregator', () => {
    const t = env();
    applyConfigStartupKnobs({ fullscreen: true }, t);
    assert.equal(t[FORCE_ALT_SCREEN_ENV], '1');
  });
});

// ---------------------------------------------------------------------------
// fullscreen through the config waterfall + validation
// ---------------------------------------------------------------------------
describe('fullscreen knob through the config waterfall', () => {
  it('a higher layer overrides a lower layer fullscreen value', () => {
    const merged = mergeConfigs(
      {},
      {},
      { fullscreen: true } as ProjectConfig, // project
      { fullscreen: false } as ProjectConfig, // global (lower precedence than local)
      { fullscreen: true } as ProjectConfig, // local (higher precedence)
      null,
    );
    assert.equal(merged.fullscreen, true);
  });

  it('is a recognized config key (no spurious unknown-key warning)', () => {
    assert.deepEqual(findUnknownConfigKeys({ fullscreen: false } as ProjectConfig), []);
  });

  it('accepts a boolean and flags a non-boolean value', () => {
    assert.deepEqual(validateProjectConfigValues({ fullscreen: false } as ProjectConfig), []);
    const issues = validateProjectConfigValues({ fullscreen: 'yes' as unknown as boolean } as ProjectConfig);
    assert.ok(issues.some((i) => i.field === 'fullscreen'));
  });
});
