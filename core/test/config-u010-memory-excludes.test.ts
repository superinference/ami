import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  mergeConfigs,
  findUnknownConfigKeys,
  validateProjectConfigValues,
  resolveConfiguredMemoryExcludes,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';
import { MemoryManager, resolveMemoryExcludes } from '../src/memory';

// ---------------------------------------------------------------------------
// U010 — ProjectConfig.memoryExcludes (parity: Claude Code's claudeMdExcludes).
// A persisted config knob that excludes specific instruction (CLAUDE.md /
// AGENTS.md / *.local.md) files from the auto-loaded memory context, unioned
// with the one-shot AMI_MEMORY_EXCLUDES env list and honored during loading.
// ---------------------------------------------------------------------------

let cwdDir: string;
let homeDir: string;
let memDir: string;
const savedHome = process.env.HOME;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedExcludes = process.env.AMI_MEMORY_EXCLUDES;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeConfig(dir: string, cfg: ProjectConfig, name = 'config.json'): void {
  const amiDir = path.join(dir, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, name), JSON.stringify(cfg), 'utf-8');
}

beforeEach(() => {
  cwdDir = mkTmp('si-u010-cwd-');
  homeDir = mkTmp('si-u010-home-');
  memDir = mkTmp('si-u010-store-');
  // Isolate the global-config read (resolveConfiguredMemoryExcludes loads
  // ~/.ami/config.json) and the auto-memory store from the real user tree.
  process.env.HOME = homeDir;
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_MEMORY_EXCLUDES;
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
  restore('AMI_MEMORY_EXCLUDES', savedExcludes);
});

describe('U010 memoryExcludes — config schema + merge', () => {
  it('is a recognized config key (no unknown-key diagnostic)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('memoryExcludes'));
    assert.deepEqual(findUnknownConfigKeys({ memoryExcludes: ['**/AGENTS.md'] }), []);
  });

  it('validates as a string[] — a non-array value is flagged', () => {
    const ok = validateProjectConfigValues({ memoryExcludes: ['a', 'b'] });
    assert.equal(ok.length, 0);
    const bad = validateProjectConfigValues({ memoryExcludes: 'nope' as unknown as string[] });
    assert.equal(bad.length, 1);
    assert.equal(bad[0]!.field, 'memoryExcludes');
    assert.match(bad[0]!.message, /string\[\]/);
  });

  it('mergeConfigs ACCUMULATES memoryExcludes across layers (union, dedupe)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { memoryExcludes: ['**/AGENTS.md', 'shared.md'] }, // project
      { memoryExcludes: ['global.md'] },                  // global
      { memoryExcludes: ['**/CLAUDE.local.md', 'shared.md'] }, // local
      null,
    );
    assert.deepEqual(merged.memoryExcludes, [
      'global.md', '**/AGENTS.md', 'shared.md', '**/CLAUDE.local.md',
    ]);
  });

  it('mergeConfigs leaves memoryExcludes absent when no layer sets it', () => {
    const merged = mergeConfigs({}, {}, {}, {}, {}, null);
    assert.equal(merged.memoryExcludes, undefined);
  });
});

describe('U010 resolveConfiguredMemoryExcludes — waterfall read', () => {
  it('returns [] when no config file configures it', () => {
    assert.deepEqual(resolveConfiguredMemoryExcludes(cwdDir), []);
  });

  it('merges project + local + global config files from disk', () => {
    writeConfig(cwdDir, { memoryExcludes: ['**/AGENTS.md'] });
    writeConfig(cwdDir, { memoryExcludes: ['**/CLAUDE.local.md'] }, 'config.local.json');
    writeConfig(homeDir, { memoryExcludes: ['user-global.md'] });
    const got = resolveConfiguredMemoryExcludes(cwdDir);
    assert.deepEqual([...got].sort(), ['**/AGENTS.md', '**/CLAUDE.local.md', 'user-global.md'].sort());
  });
});

describe('U010 resolveMemoryExcludes — env ∪ config union', () => {
  it('unions env-var patterns with config-supplied excludes, deduped', () => {
    assert.deepEqual(
      resolveMemoryExcludes({ AMI_MEMORY_EXCLUDES: 'a, b' }, ['b', 'c']),
      ['a', 'b', 'c'],
    );
  });

  it('config excludes alone apply when the env var is unset', () => {
    assert.deepEqual(resolveMemoryExcludes({}, ['x.md']), ['x.md']);
  });

  it('stays env-only when no config excludes are passed (back-compat)', () => {
    assert.deepEqual(resolveMemoryExcludes({ AMI_MEMORY_EXCLUDES: 'a:b' }), ['a', 'b']);
    assert.deepEqual(resolveMemoryExcludes({}), []);
  });
});

describe('U010 MemoryManager honors config memoryExcludes end-to-end', () => {
  it('excludes a matched instruction file configured via .ami/config.json', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), 'secret project rules', 'utf-8');
    // Control: with no exclude configured, the file loads.
    assert.ok(new MemoryManager(cwdDir).loadProjectInstructions().includes('secret project rules'));
    // Configure a persisted exclude and use a FRESH manager (config is read once
    // per instance). The file must now be skipped — this is the discriminating
    // assertion: on revert (call sites reading env-only) it would still load.
    writeConfig(cwdDir, { memoryExcludes: ['**/AGENTS.md'] });
    assert.ok(!new MemoryManager(cwdDir).loadProjectInstructions().includes('secret project rules'));
  });

  it('collectInstructionSources honors the config exclude list', async () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), 'proj rules body', 'utf-8');
    writeConfig(cwdDir, { memoryExcludes: ['**/AGENTS.md'] });
    const sources = await new MemoryManager(cwdDir).collectInstructionSources();
    assert.ok(!sources.some(s => s.content.includes('proj rules body')));
  });

  it('still honors the AMI_MEMORY_EXCLUDES env var alongside config', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), 'env-excluded body', 'utf-8');
    process.env.AMI_MEMORY_EXCLUDES = '**/AGENTS.md';
    assert.ok(!new MemoryManager(cwdDir).loadProjectInstructions().includes('env-excluded body'));
  });
});
