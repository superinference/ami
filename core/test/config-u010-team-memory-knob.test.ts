import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  mergeConfigs,
  findUnknownConfigKeys,
  validateProjectConfigValues,
  resolveTeamMemoryLoadingEnabled,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';
import { MemoryManager } from '../src/memory';

// ---------------------------------------------------------------------------
// U010 — ProjectConfig.teamMemoryEnabled (parity: Claude Code's TEAMMEM enable
// gate). An additive off-switch for LOADING the shared `.ami/memory/team/` store
// into the system prompt. Default ON (load when present); an explicit `false` in
// any config tier (project/global/local/managed) turns the shared-store read OFF.
// ---------------------------------------------------------------------------

let cwdDir: string;
let homeDir: string;
let memDir: string;
const savedHome = process.env.HOME;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeConfig(dir: string, cfg: ProjectConfig, name = 'config.json'): void {
  const amiDir = path.join(dir, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, name), JSON.stringify(cfg), 'utf-8');
}

/** Seed a team-memory file in the override memory store's `team/` subtree. */
function writeTeamMemory(name: string, body: string): void {
  const teamDir = path.join(memDir, 'team');
  fs.mkdirSync(teamDir, { recursive: true });
  fs.writeFileSync(path.join(teamDir, name), body, 'utf-8');
}

beforeEach(() => {
  cwdDir = mkTmp('si-u010-team-cwd-');
  homeDir = mkTmp('si-u010-team-home-');
  memDir = mkTmp('si-u010-team-store-');
  // Isolate the global-config read (resolveTeamMemoryLoadingEnabled loads
  // ~/.ami/config.json), the auto-memory store, and the managed tier from the
  // real user/host tree so the resolver is deterministic.
  process.env.HOME = homeDir;
  process.env.AMI_MEMORY_DIR = memDir;
  process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(homeDir, 'no-managed.json');
});

afterEach(() => {
  for (const d of [cwdDir, homeDir, memDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  const restore = (k: string, v: string | undefined) => {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  };
  restore('HOME', savedHome);
  restore('AMI_MEMORY_DIR', savedMemEnv);
  restore('SUPERINFERENCE_MANAGED_CONFIG', savedManaged);
});

describe('U010 teamMemoryEnabled — config schema + merge', () => {
  it('is a recognized config key (no unknown-key diagnostic)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('teamMemoryEnabled'));
    assert.deepEqual(findUnknownConfigKeys({ teamMemoryEnabled: false }), []);
  });

  it('validates as a boolean — a non-boolean value is flagged', () => {
    assert.equal(validateProjectConfigValues({ teamMemoryEnabled: true }).length, 0);
    const bad = validateProjectConfigValues({ teamMemoryEnabled: 'no' as unknown as boolean });
    assert.equal(bad.length, 1);
    assert.equal(bad[0]!.field, 'teamMemoryEnabled');
    assert.match(bad[0]!.message, /boolean/);
  });

  it('mergeConfigs copies teamMemoryEnabled with last-wins precedence (local > project > global > managed)', () => {
    // managed is the LOWEST file tier: a project `false` overrides a managed `true`.
    assert.equal(
      mergeConfigs({}, {}, { teamMemoryEnabled: false }, {}, {}, { teamMemoryEnabled: true }).teamMemoryEnabled,
      false,
    );
    // local overrides project.
    assert.equal(
      mergeConfigs({}, {}, { teamMemoryEnabled: true }, {}, { teamMemoryEnabled: false }).teamMemoryEnabled,
      false,
    );
  });
});

describe('U010 teamMemoryEnabled — resolveTeamMemoryLoadingEnabled', () => {
  it('defaults to true when no config sets it (preserves always-load behavior)', () => {
    assert.equal(resolveTeamMemoryLoadingEnabled(cwdDir), true);
  });

  it('returns false when the project config explicitly disables it', () => {
    writeConfig(cwdDir, { teamMemoryEnabled: false });
    assert.equal(resolveTeamMemoryLoadingEnabled(cwdDir), false);
  });

  it('returns true when the project config explicitly enables it', () => {
    writeConfig(cwdDir, { teamMemoryEnabled: true });
    assert.equal(resolveTeamMemoryLoadingEnabled(cwdDir), true);
  });
});

describe('U010 teamMemoryEnabled — end-to-end loadTeamMemories gate', () => {
  it('loads team memories by default (knob unset)', () => {
    writeTeamMemory('onboarding.md', '# Team onboarding notes');
    const mgr = new MemoryManager(cwdDir);
    const loaded = mgr.loadTeamMemories();
    assert.equal(loaded.length, 1);
    assert.match(loaded[0]!, /Team onboarding notes/);
  });

  it('returns [] when teamMemoryEnabled:false even though team files are present (RED without the gate)', () => {
    writeTeamMemory('onboarding.md', '# Team onboarding notes');
    writeConfig(cwdDir, { teamMemoryEnabled: false });
    const mgr = new MemoryManager(cwdDir);
    assert.deepEqual(mgr.loadTeamMemories(), []);
  });
});
