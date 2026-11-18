import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  mergeConfigs,
  findUnknownConfigKeys,
  validateProjectConfigValues,
  getConfigJSONSchema,
  resetConfigCache,
  loadProjectConfig,
  loadGlobalConfig,
  KNOWN_CONFIG_KEYS,
  getProjectConfigPath,
  getLocalConfigPath,
  saveLocalConfig,
  ensureLocalConfigGitignored,
  LOCAL_CONFIG_GITIGNORE_RULE,
  readLastSessionMetrics,
  saveLastSessionMetrics,
  PROJECT_ONBOARDING_SEEN_CAP,
  getProjectOnboardingSeenCount,
  shouldShowProjectOnboarding,
  incrementProjectOnboardingSeenCount,
  maybeMarkProjectOnboardingComplete,
  resolveDeepLinkTerminal,
  saveDeepLinkTerminal,
  BACKGROUND_SLOW_OP_DELAY_MS,
  BACKGROUND_INTERACTION_RECENCY_MS,
  resolveBackgroundSlowOpDelayMs,
  resolveBackgroundInteractionRecencyMs,
  shouldDeferBackgroundWork,
  captureConfigSnapshot,
  hasConfigSnapshot,
  getConfigSnapshot,
  repointConfigSnapshot,
  clearConfigSnapshot,
  getSnapshotConfigValue,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// U010 (w114) — durable session/project STATE fields + preference knobs added to
// ProjectConfig: lastSessionMetrics (StatsProvider exit flush), projectOnboarding
// fatigue control (seen-count cap + completion flag), deepLinkTerminal preference,
// interaction-recency background-deferral knobs, and a session-startup config
// snapshot primitive. Parity targets: context/stats.tsx, projectOnboardingState.ts,
// utils/deepLink/terminalLauncher.ts, utils/backgroundHousekeeping.ts,
// utils/swarm/backends/teammateModeSnapshot.ts, utils/settings/settings.ts.
// ---------------------------------------------------------------------------

let cwdDir: string;
let homeDir: string;
const savedHome = process.env.HOME;
const savedManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
const savedDemo = process.env.IS_DEMO;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function seedProjectConfig(dir: string, cfg: ProjectConfig, name = 'config.json'): void {
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', name), JSON.stringify(cfg), 'utf-8');
  resetConfigCache();
}

beforeEach(() => {
  cwdDir = mkTmp('si-u010-w114-cwd-');
  homeDir = mkTmp('si-u010-w114-home-');
  process.env.HOME = homeDir;
  // Isolate the managed tier from the host so the loaders are deterministic.
  process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(homeDir, 'no-managed.json');
  delete process.env.IS_DEMO;
  resetConfigCache();
  clearConfigSnapshot();
});

afterEach(() => {
  for (const d of [cwdDir, homeDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  const restore = (k: string, v: string | undefined) => {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  };
  restore('HOME', savedHome);
  restore('SUPERINFERENCE_MANAGED_CONFIG', savedManaged);
  restore('IS_DEMO', savedDemo);
  resetConfigCache();
  clearConfigSnapshot();
});

describe('U010/w114 — config schema recognition + JSON schema', () => {
  it('recognizes every new key (no unknown-key diagnostic)', () => {
    for (const k of [
      'lastSessionMetrics', 'projectOnboardingSeenCount', 'hasCompletedProjectOnboarding',
      'deepLinkTerminal', 'backgroundSlowOpDelayMs', 'backgroundInteractionRecencyMs',
    ]) {
      assert.ok(KNOWN_CONFIG_KEYS.has(k), `${k} should be a known key`);
    }
    assert.deepEqual(findUnknownConfigKeys({
      lastSessionMetrics: { a: 1 },
      projectOnboardingSeenCount: 2,
      hasCompletedProjectOnboarding: true,
      deepLinkTerminal: 'iTerm',
      backgroundSlowOpDelayMs: 1000,
      backgroundInteractionRecencyMs: 500,
    }), []);
  });

  it('emits the new keys in the JSON schema', () => {
    const schema = getConfigJSONSchema();
    assert.ok(schema.properties.lastSessionMetrics);
    assert.equal(schema.properties.projectOnboardingSeenCount!.type, 'number');
    assert.equal(schema.properties.hasCompletedProjectOnboarding!.type, 'boolean');
    assert.equal(schema.properties.deepLinkTerminal!.type, 'string');
    assert.equal(schema.properties.backgroundSlowOpDelayMs!.type, 'number');
  });
});

describe('U010/w114 — value validation', () => {
  it('accepts valid values and flags bad scalar types', () => {
    assert.equal(validateProjectConfigValues({
      projectOnboardingSeenCount: 3, hasCompletedProjectOnboarding: false,
      deepLinkTerminal: 'Alacritty', backgroundSlowOpDelayMs: 600000,
    }).length, 0);
    const bad = validateProjectConfigValues({
      projectOnboardingSeenCount: 'x' as unknown as number,
      deepLinkTerminal: 5 as unknown as string,
    });
    const fields = bad.map(i => i.field).sort();
    assert.deepEqual(fields, ['deepLinkTerminal', 'projectOnboardingSeenCount']);
  });

  it('validates lastSessionMetrics nested counters as non-negative numbers', () => {
    assert.equal(validateProjectConfigValues({ lastSessionMetrics: { turns: 4, cost: 0 } }).length, 0);
    const bad = validateProjectConfigValues({
      lastSessionMetrics: { turns: -1, bad: 'nope' as unknown as number },
    });
    assert.equal(bad.length, 2);
    assert.ok(bad.every(i => i.field.startsWith('lastSessionMetrics.')));
  });
});

describe('U010/w114 — mergeConfigs precedence', () => {
  it('merges deepLinkTerminal + background knobs last-wins; leaves state fields unmerged', () => {
    const merged = mergeConfigs(
      {}, {},
      { deepLinkTerminal: 'iTerm', backgroundSlowOpDelayMs: 1, projectOnboardingSeenCount: 2 }, // project
      { deepLinkTerminal: 'Terminal', backgroundSlowOpDelayMs: 9 },                              // global (lower)
      { backgroundInteractionRecencyMs: 42 },                                                    // local (higher)
    );
    assert.equal(merged.deepLinkTerminal, 'iTerm');       // project > global
    assert.equal(merged.backgroundSlowOpDelayMs, 1);      // project > global
    assert.equal(merged.backgroundInteractionRecencyMs, 42);
    // Durable per-project STATE is read directly from the project file, never merged.
    assert.equal(merged.projectOnboardingSeenCount, undefined);
  });
});

describe('U010/w114 — lastSessionMetrics round-trip (parity: StatsProvider exit flush)', () => {
  it('persists a non-empty snapshot and reads it back', () => {
    assert.deepEqual(readLastSessionMetrics(cwdDir), {});
    assert.equal(saveLastSessionMetrics(cwdDir, { turns: 7, toolCalls: 12 }), true);
    assert.deepEqual(readLastSessionMetrics(cwdDir), { turns: 7, toolCalls: 12 });
    // The field actually landed in the project config file.
    assert.deepEqual(loadProjectConfig(cwdDir)?.lastSessionMetrics, { turns: 7, toolCalls: 12 });
  });

  it('is a no-op for an empty / all-invalid snapshot (matches Object.keys > 0 guard)', () => {
    assert.equal(saveLastSessionMetrics(cwdDir, {}), false);
    assert.equal(saveLastSessionMetrics(cwdDir, { bad: NaN, neg: -3 }), false);
    assert.equal(fs.existsSync(getProjectConfigPath(cwdDir)), false);
  });

  it('drops corrupt counters on read', () => {
    seedProjectConfig(cwdDir, { lastSessionMetrics: { ok: 5, bad: -1 } as Record<string, number> });
    assert.deepEqual(readLastSessionMetrics(cwdDir), { ok: 5 });
  });
});

describe('U010/w114 — onboarding fatigue control (parity: projectOnboardingState.ts)', () => {
  it('shows by default and increments up to the cap, then stops', () => {
    assert.equal(shouldShowProjectOnboarding(cwdDir), true);
    assert.equal(getProjectOnboardingSeenCount(cwdDir), 0);
    for (let i = 1; i <= PROJECT_ONBOARDING_SEEN_CAP; i++) {
      assert.equal(incrementProjectOnboardingSeenCount(cwdDir), true);
      assert.equal(getProjectOnboardingSeenCount(cwdDir), i);
    }
    // At the cap: no further increment, and no longer shown.
    assert.equal(incrementProjectOnboardingSeenCount(cwdDir), false);
    assert.equal(getProjectOnboardingSeenCount(cwdDir), PROJECT_ONBOARDING_SEEN_CAP);
    assert.equal(shouldShowProjectOnboarding(cwdDir), false);
  });

  it('completion flag permanently suppresses the nudge', () => {
    assert.equal(maybeMarkProjectOnboardingComplete(cwdDir), true);
    assert.equal(maybeMarkProjectOnboardingComplete(cwdDir), false); // already set — no write
    assert.equal(shouldShowProjectOnboarding(cwdDir), false);
    assert.equal(loadProjectConfig(cwdDir)?.hasCompletedProjectOnboarding, true);
  });

  it('is suppressed in a demo environment', () => {
    process.env.IS_DEMO = '1';
    assert.equal(shouldShowProjectOnboarding(cwdDir), false);
  });
});

describe('U010/w114 — deepLinkTerminal preference (parity: terminalLauncher.ts)', () => {
  it('persists to global config and resolves from global or a passed config', () => {
    assert.equal(resolveDeepLinkTerminal(), undefined);
    assert.equal(saveDeepLinkTerminal('iTerm'), true);
    assert.equal(saveDeepLinkTerminal('iTerm'), false); // unchanged — no write
    assert.equal(resolveDeepLinkTerminal(), 'iTerm');
    assert.equal(loadGlobalConfig()?.deepLinkTerminal, 'iTerm');
    // A passed config takes precedence over the global read.
    assert.equal(resolveDeepLinkTerminal({ deepLinkTerminal: 'Alacritty' }), 'Alacritty');
    assert.equal(resolveDeepLinkTerminal({ deepLinkTerminal: '  ' }), undefined);
    assert.equal(saveDeepLinkTerminal('   '), false);
  });
});

describe('U010/w114 — background deferral knobs (parity: backgroundHousekeeping.ts)', () => {
  it('exposes the reference defaults', () => {
    assert.equal(BACKGROUND_SLOW_OP_DELAY_MS, 10 * 60 * 1000);
    assert.equal(BACKGROUND_INTERACTION_RECENCY_MS, 60 * 1000);
  });

  it('resolves config overrides, else defaults', () => {
    assert.equal(resolveBackgroundSlowOpDelayMs(), BACKGROUND_SLOW_OP_DELAY_MS);
    assert.equal(resolveBackgroundSlowOpDelayMs({ backgroundSlowOpDelayMs: 1234 }), 1234);
    assert.equal(resolveBackgroundSlowOpDelayMs({ backgroundSlowOpDelayMs: -1 }), BACKGROUND_SLOW_OP_DELAY_MS);
    assert.equal(resolveBackgroundInteractionRecencyMs({ backgroundInteractionRecencyMs: 7 }), 7);
    assert.equal(resolveBackgroundInteractionRecencyMs(), BACKGROUND_INTERACTION_RECENCY_MS);
  });

  it('defers only when interactive AND last interaction is within the recency window', () => {
    const now = 1_000_000;
    // Interacted 10s ago, 60s window → defer.
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: now - 10_000, now }), true);
    // Interacted 2 min ago → do not defer.
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: now - 120_000, now }), false);
    // Non-interactive session → never defer.
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: now, now, interactive: false }), false);
    // Absent / invalid timestamp → never defer.
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: null, now }), false);
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: NaN, now }), false);
    // Custom recency window is honored.
    assert.equal(shouldDeferBackgroundWork({ lastInteractionMs: now - 90_000, now, recencyMs: 120_000 }), true);
  });
});

describe('U010/w114 — local config write auto-gitignores (parity: settings.ts)', () => {
  it('appends the local-config rule to .gitignore on a local write, idempotently', () => {
    const res = saveLocalConfig(cwdDir, c => ({ ...c, model: 'x' }));
    assert.equal(res.written, true);
    assert.ok(getLocalConfigPath(cwdDir).endsWith(path.join('.ami', 'config.local.json')));
    assert.ok(fs.existsSync(getLocalConfigPath(cwdDir)));
    const gi = path.join(cwdDir, '.gitignore');
    assert.ok(fs.readFileSync(gi, 'utf-8').includes(LOCAL_CONFIG_GITIGNORE_RULE));
    // A second write does not duplicate the rule.
    saveLocalConfig(cwdDir, c => ({ ...c, model: 'y' }));
    const occurrences = fs.readFileSync(gi, 'utf-8').split('\n').filter(l => l.trim() === LOCAL_CONFIG_GITIGNORE_RULE).length;
    assert.equal(occurrences, 1);
  });

  it('ensureLocalConfigGitignored matches an existing rule (with leading slash) and preserves content', () => {
    const gi = path.join(cwdDir, '.gitignore');
    fs.writeFileSync(gi, 'node_modules\n/' + LOCAL_CONFIG_GITIGNORE_RULE + '\n', 'utf-8');
    assert.equal(ensureLocalConfigGitignored(cwdDir), false); // already covered (leading slash form)
    // Absent gitignore → created.
    const other = mkTmp('si-u010-w114-gi-');
    try {
      assert.equal(ensureLocalConfigGitignored(other), true);
      assert.equal(fs.readFileSync(path.join(other, '.gitignore'), 'utf-8').trim(), LOCAL_CONFIG_GITIGNORE_RULE);
    } finally {
      fs.rmSync(other, { recursive: true, force: true });
    }
  });
});

describe('U010/w114 — session-startup config snapshot (parity: teammateModeSnapshot.ts)', () => {
  it('captures once, freezes against later edits, and clones on read', () => {
    assert.equal(hasConfigSnapshot(), false);
    assert.equal(getConfigSnapshot(), null);
    const snap = captureConfigSnapshot({ model: 'alpha', deepLinkTerminal: 'iTerm' });
    assert.equal(snap.model, 'alpha');
    assert.equal(hasConfigSnapshot(), true);
    // A second capture is a no-op (frozen at startup).
    captureConfigSnapshot({ model: 'beta' });
    assert.equal(getConfigSnapshot()?.model, 'alpha');
    // Returned object is a defensive clone — mutating it does not corrupt the snapshot.
    const read = getConfigSnapshot()!;
    read.model = 'mutated';
    assert.equal(getConfigSnapshot()?.model, 'alpha');
  });

  it('repoint deliberately replaces the snapshot with a directly-passed value', () => {
    captureConfigSnapshot({ model: 'alpha' });
    const next = repointConfigSnapshot({ model: 'gamma' });
    assert.equal(next.model, 'gamma');
    assert.equal(getConfigSnapshot()?.model, 'gamma');
  });

  it('getSnapshotConfigValue reads a frozen knob and lazy-captures when uncaptured', () => {
    // Uncaptured, no live fallback → undefined.
    assert.equal(getSnapshotConfigValue('model'), undefined);
    // Uncaptured WITH a live fallback → lazy-capture then read.
    assert.equal(getSnapshotConfigValue('model', { model: 'live' }), 'live');
    assert.equal(hasConfigSnapshot(), true);
    // Now frozen: a different live value does not change the frozen read.
    assert.equal(getSnapshotConfigValue('model', { model: 'changed' }), 'live');
  });
});
