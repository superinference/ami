import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  mergeConfigs,
  findUnknownConfigKeys,
  validateProjectConfigValues,
  resolveSpeculationEnabled,
  shouldEnableThinkingByDefault,
  getConfigJSONSchema,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// U010 (w98) — two additive config off-/opt-switches:
//   * ProjectConfig.speculationEnabled — the reference parity `speculationEnabled`
//     global-config knob for isSpeculationEnabled(); default ON, explicit `false`
//     disables next-prompt speculation. Resolved by resolveSpeculationEnabled.
//   * ProjectConfig.alwaysThinkingEnabled — the reference parity `alwaysThinkingEnabled`
//     consulted by shouldEnableThinkingByDefault(); AMI auto-adopts a reasoning
//     model's default thinking level unless this is `false` (opt-out) or
//     MAX_THINKING_TOKENS gates it.
// ---------------------------------------------------------------------------

let cwdDir: string;
let homeDir: string;
const savedHome = process.env.HOME;
const savedManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
const savedMtt = process.env.MAX_THINKING_TOKENS;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeConfig(dir: string, cfg: ProjectConfig, name = 'config.json'): void {
  const amiDir = path.join(dir, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, name), JSON.stringify(cfg), 'utf-8');
}

beforeEach(() => {
  cwdDir = mkTmp('si-u010-w98-cwd-');
  homeDir = mkTmp('si-u010-w98-home-');
  // Isolate the global-config read + managed tier from the real user/host tree so
  // the resolvers are deterministic, and clear the MAX_THINKING_TOKENS gate.
  process.env.HOME = homeDir;
  process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(homeDir, 'no-managed.json');
  delete process.env.MAX_THINKING_TOKENS;
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
  restore('MAX_THINKING_TOKENS', savedMtt);
});

describe('U010 w98 speculationEnabled — schema + merge', () => {
  it('is a recognized config key (no unknown-key diagnostic)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('speculationEnabled'));
    assert.deepEqual(findUnknownConfigKeys({ speculationEnabled: false }), []);
  });

  it('appears in the published JSON schema as a boolean knob', () => {
    const schema = getConfigJSONSchema();
    assert.ok(schema.properties['speculationEnabled']);
    assert.equal(schema.properties['speculationEnabled']!.type, 'boolean');
  });

  it('validates as a boolean — a non-boolean value is flagged', () => {
    assert.equal(validateProjectConfigValues({ speculationEnabled: true }).length, 0);
    const bad = validateProjectConfigValues({ speculationEnabled: 'yes' as unknown as boolean });
    assert.equal(bad.length, 1);
    assert.equal(bad[0]!.field, 'speculationEnabled');
    assert.match(bad[0]!.message, /boolean/);
  });

  it('mergeConfigs copies speculationEnabled with last-wins precedence', () => {
    // managed is the LOWEST tier: a project `false` overrides a managed `true`.
    assert.equal(
      mergeConfigs({}, {}, { speculationEnabled: false }, {}, {}, { speculationEnabled: true }).speculationEnabled,
      false,
    );
    assert.equal(
      mergeConfigs({}, {}, { speculationEnabled: true }, {}, { speculationEnabled: false }).speculationEnabled,
      false,
    );
  });
});

describe('U010 w98 resolveSpeculationEnabled', () => {
  it('returns undefined (default-on signal) when no config sets it', () => {
    assert.equal(resolveSpeculationEnabled(cwdDir), undefined);
  });

  it('returns false when the project config explicitly disables it', () => {
    writeConfig(cwdDir, { speculationEnabled: false });
    assert.equal(resolveSpeculationEnabled(cwdDir), false);
  });

  it('returns true when the project config explicitly enables it', () => {
    writeConfig(cwdDir, { speculationEnabled: true });
    assert.equal(resolveSpeculationEnabled(cwdDir), true);
  });
});

describe('U010 w98 alwaysThinkingEnabled — schema + merge', () => {
  it('is a recognized config key (no unknown-key diagnostic)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('alwaysThinkingEnabled'));
    assert.deepEqual(findUnknownConfigKeys({ alwaysThinkingEnabled: false }), []);
  });

  it('validates as a boolean — a non-boolean value is flagged', () => {
    assert.equal(validateProjectConfigValues({ alwaysThinkingEnabled: false }).length, 0);
    const bad = validateProjectConfigValues({ alwaysThinkingEnabled: 1 as unknown as boolean });
    assert.equal(bad.length, 1);
    assert.equal(bad[0]!.field, 'alwaysThinkingEnabled');
    assert.match(bad[0]!.message, /boolean/);
  });

  it('mergeConfigs copies alwaysThinkingEnabled with last-wins precedence', () => {
    assert.equal(
      mergeConfigs({}, {}, { alwaysThinkingEnabled: false }, {}, {}, { alwaysThinkingEnabled: true }).alwaysThinkingEnabled,
      false,
    );
  });
});

describe('U010 w98 shouldEnableThinkingByDefault', () => {
  it('defaults to true when no config sets it (preserves auto-on behavior)', () => {
    assert.equal(shouldEnableThinkingByDefault(cwdDir), true);
  });

  it('returns false when alwaysThinkingEnabled is explicitly false (opt-out)', () => {
    writeConfig(cwdDir, { alwaysThinkingEnabled: false });
    assert.equal(shouldEnableThinkingByDefault(cwdDir), false);
  });

  it('returns true when alwaysThinkingEnabled is explicitly true', () => {
    writeConfig(cwdDir, { alwaysThinkingEnabled: true });
    assert.equal(shouldEnableThinkingByDefault(cwdDir), true);
  });

  it('MAX_THINKING_TOKENS>0 forces default-on even when alwaysThinkingEnabled is false', () => {
    writeConfig(cwdDir, { alwaysThinkingEnabled: false });
    assert.equal(shouldEnableThinkingByDefault(cwdDir, { MAX_THINKING_TOKENS: '2048' }), true);
  });

  it('MAX_THINKING_TOKENS=0 forces off even when alwaysThinkingEnabled is true', () => {
    writeConfig(cwdDir, { alwaysThinkingEnabled: true });
    assert.equal(shouldEnableThinkingByDefault(cwdDir, { MAX_THINKING_TOKENS: '0' }), false);
  });

  it('a non-numeric MAX_THINKING_TOKENS is ignored (falls through to the setting)', () => {
    writeConfig(cwdDir, { alwaysThinkingEnabled: false });
    assert.equal(shouldEnableThinkingByDefault(cwdDir, { MAX_THINKING_TOKENS: 'abc' }), false);
  });
});
